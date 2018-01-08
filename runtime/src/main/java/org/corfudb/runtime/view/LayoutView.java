package org.corfudb.runtime.view;

import static java.util.Arrays.stream;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.LayoutPrepareResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LayoutClient;
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
        return layoutHelper(l -> {
            return l;
        });
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
        // For some reason, the alreadyProposedLayout sometimes doesn't have a runtime
        // we need to remove runtime from the layout, but for now, let's manually take
        // it from the original layout.
        layoutToPropose.setRuntime(layout.getRuntime());
        //phase 2: propose the new layout.
        propose(epoch, rank, layoutToPropose);
        //phase 3: commited
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
                        LayoutClient layoutClient = runtime.getRouter(x)
                                .getClient(LayoutClient.class);
                        cf = layoutClient.prepare(epoch, rank);
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
                        LayoutClient layoutClient = runtime.getRouter(x)
                                .getClient(LayoutClient.class);
                        cf =  layoutClient.propose(epoch, rank, layout);
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
    @SuppressWarnings("unchecked")
    public void committed(long epoch, Layout layout)
            throws WrongEpochException {
        CompletableFuture<Boolean>[] commitList = layout.getLayoutServers().stream()
                .map(x -> {
                    CompletableFuture<Boolean> cf = new CompletableFuture<>();
                    try {
                        // Connection to router can cause network exception too.
                        LayoutClient layoutClient = runtime.getRouter(x)
                                .getClient(LayoutClient.class);
                        cf = layoutClient.committed(epoch, layout);
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
            } catch (TimeoutException | NetworkException e) {
                timeouts++;
            }
            responses++;
            commitList = Arrays.stream(commitList)
                    .filter(x -> !x.isCompletedExceptionally())
                    .toArray(CompletableFuture[]::new);

            log.debug("committed: Successful responses={}, timeouts={}", responses, timeouts);
        }
    }
}
