package org.corfudb.runtime.view;

import static java.util.Arrays.stream;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.LayoutPrepareResponse;
import org.corfudb.protocols.wireprotocol.LayoutQueryResponse;
import org.corfudb.protocols.wireprotocol.Phase2Data;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.util.CFUtils;

/**
 * Created by mwei on 12/10/15.
 */
@Slf4j
public class LayoutView extends AbstractView {

    public LayoutView(@Nonnull final CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     * Retrieves current layout.
     **/
    public Layout getLayout() {
        return layoutHelper(RuntimeLayout::getLayout);
    }

    public RuntimeLayout getRuntimeLayout() {
        return layoutHelper(l -> l);
    }

    public RuntimeLayout getRuntimeLayout(@Nonnull Layout layout) {
        return new RuntimeLayout(layout, runtime);
    }

    /**
     * Retrieves the number of nodes needed to obtain a quorum.
     * We define a quorum for the layout view as n/2+1
     *
     * @return The number of nodes required for a quorum.
     */
    public int getQuorumNumber() {
        return (getLayout().getLayoutServers().size() / 2) + 1;
    }

    /**
     * Drives the consensus protocol for persisting the new Layout.
     * TODO currently the code can drive only one Layout change.
     * If it has to drive a previously incomplete round
     * TODO it will drop it's own set of changes. Need to revisit this.
     * A change of layout proposal consists of a rank and the desired layout.
     *
     * @param layout The layout to propose.
     * @param rank The rank for the proposed layout.
     *
     * @throws QuorumUnreachableException Thrown if responses not received from a majority of
     *                                    layout servers.
     * @throws OutrankedException outranked exception, i.e., higher rank.
     * @throws WrongEpochException wrong epoch number.
     */
    @SuppressWarnings("unchecked")
    public void updateLayout(Layout layout, long rank)
            throws QuorumUnreachableException, OutrankedException, WrongEpochException {
        // Note this step is done because we have added the layout to the Epoch.
        long epoch = layout.getEpoch();
        Layout currentLayout = getLayout();
        if (currentLayout.getEpoch() != epoch - 1) {
            log.error("Runtime layout has epoch {} but expected {} to move to epoch {}",
                    currentLayout.getEpoch(), epoch - 1, epoch);
            throw new WrongEpochException(epoch - 1);
        }
        if (currentLayout.getClusterId() != null
            && !currentLayout.getClusterId().equals(layout.getClusterId())) {
            log.error("updateLayout: Requested layout has cluster Id {} but expected {}",
                    layout.getClusterId(), currentLayout.getClusterId());
            throw new WrongClusterException(currentLayout.getClusterId(), layout.getClusterId());
        }
        //phase 1: prepare with a given rank.
        Layout alreadyProposedLayout = prepare(epoch, rank);
        Layout layoutToPropose = alreadyProposedLayout != null ? alreadyProposedLayout : layout;
        //phase 2: propose the new layout.
        propose(epoch, rank, layoutToPropose);
        //phase 3: committed
        committed(epoch, layoutToPropose);
    }

    /**
     * Sends prepare to the current layout and can proceed only if it is accepted by a quorum.
     *
     * @param rank The rank for the proposed layout.
     * @return layout
     * @throws QuorumUnreachableException Thrown if responses not received from a majority of
     *                                    layout servers.
     * @throws OutrankedException outranked exception, i.e., higher rank.
     * @throws WrongEpochException wrong epoch number.
     */
    @SuppressWarnings("unchecked")
    public Layout prepare(long epoch, long rank)
            throws QuorumUnreachableException, OutrankedException, WrongEpochException {

        CompletableFuture<LayoutPrepareResponse>[] prepareList = getLayout().getLayoutServers()
                .stream()
                .map(x -> {
                    CompletableFuture<LayoutPrepareResponse> cf = new CompletableFuture<>();
                    try {
                        // Connection to router can cause network exception too.
                        cf = getRuntimeLayout().getLayoutClient(x).prepare(epoch, rank);
                    } catch (Exception e) {
                        cf.completeExceptionally(e);
                    }
                    return cf;
                })
                .toArray(CompletableFuture[]::new);
        LayoutPrepareResponse[] acceptList;
        long timeouts = 0L;
        long wrongEpochRejected = 0L;
        while (true) {
            // do we still have enough for a quorum?
            if (prepareList.length < getQuorumNumber()) {
                log.debug("prepare: Quorum unreachable, remaining={}, required={}", prepareList,
                        getQuorumNumber());
                throw new QuorumUnreachableException(prepareList.length, getQuorumNumber());
            }

            // wait for someone to complete.
            try {
                CFUtils.getUninterruptibly(CompletableFuture.anyOf(prepareList),
                        OutrankedException.class, TimeoutException.class, NetworkException.class,
                        WrongEpochException.class);
            } catch (TimeoutException | NetworkException e) {
                timeouts++;
            } catch (WrongEpochException we) {
                wrongEpochRejected++;
            }

            // remove errors.
            prepareList = stream(prepareList)
                    .filter(x -> !x.isCompletedExceptionally())
                    .toArray(CompletableFuture[]::new);
            // count successes.
            acceptList = stream(prepareList)
                    .map(x -> {
                        try {
                            return x.getNow(null);
                        } catch (Exception e) {
                            return null;
                        }
                    })
                    .filter(x -> x != null)
                    .toArray(LayoutPrepareResponse[]::new);

            log.debug("prepare: Successful responses={}, needed={}, timeouts={}, "
                            + "wrongEpochRejected={}",
                    acceptList.length, getQuorumNumber(), timeouts, wrongEpochRejected);

            if (acceptList.length >= getQuorumNumber()) {
                break;
            }
        }
        // Return any layouts that have been proposed before.
        List<LayoutPrepareResponse> list = Arrays.stream(acceptList)
                .filter(x -> x.getLayout() != null)
                .collect(Collectors.toList());
        if (list.isEmpty()) {
            return null;
        } else {
            // Choose the layout with the highest rank proposed before.
            long highestReturnedRank = Long.MIN_VALUE;
            Layout layoutWithHighestRank = null;

            for (LayoutPrepareResponse layoutPrepareResponse : list) {
                if (layoutPrepareResponse.getRank() > highestReturnedRank) {
                    highestReturnedRank = layoutPrepareResponse.getRank();
                    layoutWithHighestRank = layoutPrepareResponse.getLayout();
                }
            }
            return layoutWithHighestRank;
        }
    }

    /**
     * Proposes new layout to all the servers in the current layout.
     *
     * @throws QuorumUnreachableException Thrown if responses not received from a majority of
     *                                    layout servers.
     * @throws OutrankedException outranked exception, i.e., higher rank.
     */
    @SuppressWarnings("unchecked")
    public Layout propose(long epoch, long rank, Layout layout)
            throws QuorumUnreachableException, OutrankedException {
        CompletableFuture<Boolean>[] proposeList = getLayout().getLayoutServers().stream()
                .map(x -> {
                    CompletableFuture<Boolean> cf = new CompletableFuture<>();
                    try {
                        // Connection to router can cause network exception too.
                        cf = getRuntimeLayout().getLayoutClient(x).propose(epoch, rank, layout);
                    } catch (NetworkException e) {
                        cf.completeExceptionally(e);
                    }
                    return cf;
                })
                .toArray(CompletableFuture[]::new);

        long timeouts = 0L;
        long wrongEpochRejected = 0L;
        while (true) {
            // do we still have enough for a quorum?
            if (proposeList.length < getQuorumNumber()) {
                log.debug("propose: Quorum unreachable, remaining={}, required={}", proposeList,
                        getQuorumNumber());
                throw new QuorumUnreachableException(proposeList.length, getQuorumNumber());
            }

            // wait for someone to complete.
            try {
                CFUtils.getUninterruptibly(CompletableFuture.anyOf(proposeList),
                        OutrankedException.class, TimeoutException.class, NetworkException.class,
                        WrongEpochException.class);
            } catch (TimeoutException | NetworkException e) {
                timeouts++;
            } catch (WrongEpochException we) {
                wrongEpochRejected++;
            }

            // remove errors.
            proposeList = stream(proposeList)
                    .filter(x -> !x.isCompletedExceptionally())
                    .toArray(CompletableFuture[]::new);

            // count successes.
            long count = stream(proposeList)
                    .map(x -> {
                        try {
                            return x.getNow(false);
                        } catch (Exception e) {
                            return false;
                        }
                    })
                    .filter(x -> x)
                    .count();

            log.debug("propose: Successful responses={}, needed={}, timeouts={}, "
                            + "wrongEpochRejected={}",
                    count, getQuorumNumber(), timeouts, wrongEpochRejected);

            if (count >= getQuorumNumber()) {
                break;
            }
        }

        return layout;
    }

    /**
     * Send committed layout to the old Layout servers and the new Layout Servers.
     * TODO Current policy is to send the committed layout once. Need to revisit this in order
     * TODO to drive the new layout to all the involved LayoutServers.
     * TODO The new layout servers are not bootstrapped and will reject committed messages.
     * TODO Need to fix this.
     *
     * @throws WrongEpochException wrong epoch number.
     */
    public void committed(long epoch, Layout layout) {
        committed(epoch, layout, false);
    }

    /**
     * Send committed layout to the old Layout servers and the new Layout Servers.
     * If force is true, then the layout forced on all layout servers.
     */
    @SuppressWarnings("unchecked")
    public void committed(long epoch, Layout layout, boolean force)
            throws WrongEpochException {
        CompletableFuture<Boolean>[] commitList = layout.getLayoutServers().stream()
                .map(x -> {
                    CompletableFuture<Boolean> cf = new CompletableFuture<>();
                    try {
                        // Connection to router can cause network exception too.
                        if (force) {
                            cf = getRuntimeLayout(layout).getLayoutClient(x).force(layout);
                        } else {
                            cf = getRuntimeLayout(layout).getLayoutClient(x).committed(epoch, layout);
                        }
                    } catch (NetworkException e) {
                        cf.completeExceptionally(e);
                    }
                    return cf;
                })
                .toArray(CompletableFuture[]::new);

        int timeouts = 0;
        int responses = 0;
        while (responses < commitList.length) {
            // wait for someone to complete.
            try {
                CFUtils.getUninterruptibly(CompletableFuture.anyOf(commitList),
                        WrongEpochException.class, TimeoutException.class, NetworkException.class);
            } catch (WrongEpochException e) {
                if (!force) {
                    throw  e;
                }
                log.warn("committed: Error while force committing", e);
            } catch (TimeoutException | NetworkException e) {
                log.warn("committed: Error while committing", e);
                timeouts++;
            }
            responses++;
            commitList = Arrays.stream(commitList)
                    .filter(x -> !x.isCompletedExceptionally())
                    .toArray(CompletableFuture[]::new);

            log.debug("committed: Successful responses={}, timeouts={}", responses, timeouts);
        }
    }


    /**
     * Collect a majority of layout query responses.
     *
     * @param allResponses
     * @return An array of successful layout query responses
     */
    LayoutQueryResponse[] getQuorumResp(CompletableFuture<LayoutQueryResponse>[] allResponses) {
        LayoutQueryResponse[] respOk = Arrays.stream(allResponses)
                .filter(f -> f.isDone() && !f.isCompletedExceptionally() && !f.isCancelled())
                .toArray(LayoutQueryResponse[]::new);

        if (respOk.length < (allResponses.length / 2) + 1) {
            // quorum is not available
            int reachable = respOk.length;
            int required = (allResponses.length / 2) + 1;
            throw new QuorumUnreachableException(reachable, required);
        }

        return respOk;
    }

    /**
     * Attempts to find the majority epoch.
     *
     * @param responses query results of layout servers that responded
     * @param majorityCount the majority
     * @return possibly a majority epoch
     */
    Optional<Long> findMajorityEpoch(LayoutQueryResponse[] responses, int majorityCount) {
        Map<Long, Integer> epochCounts = new HashMap<>();
        for (LayoutQueryResponse resp : responses) {
            int freq = epochCounts.getOrDefault(resp.getCurrentEpoch(), 0) + 1;
            if (freq >= majorityCount) {
                return Optional.of(resp.getCurrentEpoch());
            }
            epochCounts.put(resp.getCurrentEpoch(), freq);
        }
        return Optional.empty();
    }

    /**
     * This method is used to discover the latest committed layout. It is to be used
     * when there exists no majority epoch[1] on the layout servers and we need to complete sealing.
     * Since we we only seal committed layouts, then the latest epoch and the last successfully
     * committed layout's epoch can only differ by one[2]. Because of [1] and [2] a phase2 layout
     * majority has to exist (except for the case the min replication factor is violated).
     *
     * @param responses
     * @param majorityCount majority quorum size
     * @return The latest known committed layout
     */
    Optional<Layout> findLastCommittedLayout(LayoutQueryResponse[] responses, int majorityCount) {
        Map<Phase2Data, Integer> phase2DataCounts = new HashMap<>();
        for (LayoutQueryResponse resp : responses) {

            if (resp.getCurrentEpochPhase2().isPresent()) {
                int freq = phase2DataCounts.getOrDefault(resp.getCurrentEpochPhase2().get(), 0) + 1;
                if (freq >= majorityCount) {
                    return Optional.of(resp.getCurrentEpochPhase2().get().getLayout());
                }
                phase2DataCounts.put(resp.getCurrentEpochPhase2().get(), freq);
            }

            if (resp.getLastEpochPhase2().isPresent()) {
                int freq = phase2DataCounts.getOrDefault(resp.getLastEpochPhase2().get(), 0) + 1;
                if (freq >= majorityCount) {
                    return Optional.of(resp.getLastEpochPhase2().get().getLayout());
                }
                phase2DataCounts.put(resp.getLastEpochPhase2().get(), freq);
            }
        }

        // TODO(Maithem) keep all majorities and pick the greatest epoch at the end
        return Optional.empty();
    }

    /**
     * Attempts a quorum read on the latest phase 2 data. If a quorum read is not possible, then
     * the quorum write failed and it it needs to be completed, or that the layouts are influx.
     * @param responses
     * @param majorityCount majority quorum size
     * @return
     */
    Optional<Phase2Data> findPhase2DataMajority(LayoutQueryResponse[] responses, int majorityCount) {
        Map<Phase2Data, Integer> phase2DataCounts = new HashMap<>();
        for (LayoutQueryResponse resp : responses) {
            if (resp.getCurrentEpochPhase2().isPresent()) {
                int freq = phase2DataCounts.getOrDefault(resp.getCurrentEpochPhase2().get(), 0) + 1;
                if (freq >= majorityCount) {
                    return Optional.of(resp.getCurrentEpochPhase2().get());
                }
                phase2DataCounts.put(resp.getCurrentEpochPhase2().get(), freq);
            }
        }
        return Optional.empty();
    }

    /**
     * Queries the layout servers and returns only valid responses (i.e. queries that completed).
     * If a majority of queries can't be collected this method will fail with a QuorumUnreachableException
     * @param layoutServers layout servers to query
     * @return an array of completed queries
     */
    LayoutQueryResponse[] queryLayouts(List<String> layoutServers) {
        CompletableFuture<LayoutQueryResponse>[] futures = new CompletableFuture[layoutServers.size()];
        int ind = 0;

        for (String address : layoutServers) {
            //TODO(Maithem) since this uses a runtime client don't use the passed layoutServers argument?
            CompletableFuture<LayoutQueryResponse> resp = runtime.getLayoutView()
                    .getRuntimeLayout()
                    .getLayoutClient(address)
                    .query();
            futures[ind++] = resp;
        }

        // Wait on all futures to complete/fail.
        CompletableFuture.allOf(futures);

        // If a quorum doesn't respond this method will fail because we don't enough information
        LayoutQueryResponse[] respOk = getQuorumResp(futures);
        return respOk;
    }

    // TODO(Maithem): seperate the read logic from error detection and repair logic
    Layout quorumReadLayout(List<String> layoutServers) {
        LayoutQueryResponse[] responses = queryLayouts(layoutServers);

        int majorityCount = (layoutServers.size() / 2) + 1;

        Optional<Phase2Data> latestPhase2DataQuorum = findPhase2DataMajority(responses, majorityCount);

        if (latestPhase2DataQuorum.isPresent()) {
            return latestPhase2DataQuorum.get().getLayout();
        }

        // At this point, we are unable to quorum read a layout because of possible
        // partial failures in seal and quorum write.
        try {
            repairLayoutServers(responses, majorityCount);
        } catch (QuorumUnreachableException | OutrankedException | WrongEpochException e) {
            log.error("quorumReadLayout: failed to repair layout", e);
            throw new IllegalStateException("Layout Repair Failed!");
        }

        // Attempt to read again after repair
        LayoutQueryResponse[] queriesAfterRepair = queryLayouts(layoutServers);

        latestPhase2DataQuorum = findPhase2DataMajority(queriesAfterRepair, majorityCount);

        if (latestPhase2DataQuorum.isPresent()) {
            // TODO(Maithem): should trigger an async layout committed task?
            return latestPhase2DataQuorum.get().getLayout();
        }

        throw new IllegalStateException("failed to read and repair the layout servers");
    }


    /**
     * Attempts to repair the layout servers.
     * 1. epoch repair (resealing)
     * 2. phase 2 data repair (completing rounds)
     * 2. patching layout committed? (or should this be completed by the periodic tasks)
     * @param responses
     * @param majorityCount
     */
    void repairLayoutServers(LayoutQueryResponse[] responses, int majorityCount) throws
            QuorumUnreachableException, OutrankedException, WrongEpochException {

        Optional<Long> majorityEpoch = findMajorityEpoch(responses, majorityCount);

        // Repair epochs by completing seal (using the last known committed layout
        if (!majorityEpoch.isPresent()) {
            // This can happen when the last seal fails
            // this method (i.e. repairLayoutServers) is called when a quorum
            // read on the phase2Data fails, therefore we need to find the layout
            // that that was committed for the last epoch
            Optional<Layout> lastCommittedLayout = findLastCommittedLayout(responses, majorityCount);

            if (!lastCommittedLayout.isPresent()) {
                throw new IllegalStateException("Couldn't find a committed layout");
            }

            // check that the max epoch is lastCommittedLayout epoch - 1
            sealEpoch(lastCommittedLayout.get());
        }

        // At this point the epochs should have been repair, we proceed to fix the
        // layouts. We either attempt to recommit a layout with a higher rank (in the
        // case of partial failure in the phase2), or use
        Optional<Phase2Data> highestRankedPhase2 = getHighestRankedLayout(responses, majorityCount);

        if (highestRankedPhase2.isPresent()) {
            // attempt consensus
            Layout partiallyProposedLayout = new Layout(highestRankedPhase2.get().getLayout());
            runtime.getLayoutView().updateLayout(partiallyProposedLayout,
                    highestRankedPhase2.get().getRank().getRank() + 1);

        } else {
            // Fill in the new slot (new epoch) from the last layout
            Optional<Layout> lastCommittedLayout = findLastCommittedLayout(responses, majorityCount);
            Layout layoutFromTheLastEpoch = lastCommittedLayout.get();
            Layout newLayout = new LayoutBuilder(layoutFromTheLastEpoch)
                    .setEpoch(layoutFromTheLastEpoch.getEpoch() + 1)
                    .build();
            // verify that this layout matches the epoch we are trying to update
            runtime.getLayoutView().updateLayout(newLayout, 1L);
            // log success
        }
    }

    /**
     * Scan the latest phase2Data for the latest epoch (repaired) and find the max ranked
     * layout
     * @param responses
     * @return
     */
    Optional<Phase2Data> getHighestRankedLayout(LayoutQueryResponse[] responses, int targetEpoch) {
        Optional<Phase2Data> highestRankPhase2Data = Optional.empty();
        for (LayoutQueryResponse response : responses) {
            if(response.getCurrentEpochPhase2().isPresent()) {
                if (!highestRankPhase2Data.isPresent()) {
                    // Initialize first max
                    highestRankPhase2Data = Optional.of(response.getCurrentEpochPhase2().get());
                } else {
                    // We need to find the max rank between the last known max and this response
                    if (response.getCurrentEpochPhase2().get().getRank().getRank() >
                            highestRankPhase2Data.get().getRank().getRank()) {
                        // adopt a new max
                        highestRankPhase2Data = Optional.of(response.getCurrentEpochPhase2().get());
                    }
                }
            }
        }
        return highestRankPhase2Data;
    }

    /**
     * Repairing the epochs (via completing the seal) should happen on all
     * components and not just the layout servers.
     * @param layout
     * @throws QuorumUnreachableException
     */
    private void sealEpoch(Layout layout) throws QuorumUnreachableException {
        layout.setEpoch(layout.getEpoch() + 1);
        runtime.getLayoutView().getRuntimeLayout(layout).sealMinServerSet();
    }
}
