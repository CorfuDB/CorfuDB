package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LayoutPrepareResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.util.CFUtils;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.Arrays.stream;

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
    public void updateLayout(Layout layout, long rank)
            throws QuorumUnreachableException, OutrankedException, WrongEpochException {
        // Note this step is done because we have added the layout to the Epoch.
        long epoch = layout.getEpoch();
        Layout currentLayout = getLayout();
        if (currentLayout.getEpoch() != epoch - 1) {
            log.error("updateLayout: Runtime layout has epoch {} but expected {} to move to {}",
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
     * Sends prepare to current layout servers and can proceed only if it is promised by a quorum.
     *
     * @param epoch The epoch for the new consensus.
     * @param rank The rank for the proposed layout.
     * @return layout Accepted layout with the highest rank from acceptors or null.
     *
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
                .map(x -> getRuntimeLayout().getLayoutClient(x).prepare(epoch, rank))
                .toArray(CompletableFuture[]::new);

        int promises = 0;
        int timeouts = 0;
        int outranks = 0;
        int wrongEpochRejected = 0;
        TreeSet<OutrankedException> outrankedExceptions
                = new TreeSet<>(OutrankedException.OUTRANKED_EXCEPTION_COMPARATOR);
        TreeSet<LayoutPrepareResponse> acceptedLayouts
                = new TreeSet<>(LayoutPrepareResponse.LAYOUT_PREPARE_RESPONSE_COMPARATOR);

        int requestNum = prepareList.length;
        for (int i = 0; i < requestNum; i++) {
            // utilize CompletableFuture.anyOf to find a potential completed one.
            // handle exceptions only when removing cfs from list
            try {
                CFUtils.getUninterruptibly(CompletableFuture.anyOf(prepareList));
            } catch (Exception e) {
                log.trace("prepare: ", e);
            }

            Set<CompletableFuture<LayoutPrepareResponse>> completedSet = stream(prepareList)
                    .filter(CompletableFuture::isDone)
                    .collect(Collectors.toSet());

            prepareList = stream(prepareList)
                    .filter(o -> !completedSet.contains(o))
                    .toArray(CompletableFuture[]::new);

            for (CompletableFuture<LayoutPrepareResponse> cf : completedSet) {
                try {
                    LayoutPrepareResponse response = CFUtils.getUninterruptibly(
                            cf, OutrankedException.class, TimeoutException.class,
                            NetworkException.class, WrongEpochException.class);
                    acceptedLayouts.add(response);
                    promises++;
                } catch (TimeoutException | NetworkException e) {
                    timeouts++;
                } catch (OutrankedException oe) {
                    outranks++;
                    outrankedExceptions.add(oe);
                } catch (WrongEpochException we) {
                    wrongEpochRejected++;
                }
            }

            if (promises >= getQuorumNumber()) {
                break;
            }

            // when quorum is unreachable, check if outranks is majority and throw OutrankedException
            if (promises + prepareList.length < getQuorumNumber()) {
                log.debug("prepare: Quorum unreachable, promises={}, required={}, timeouts={}, " +
                                "wrongEpochRejected={}, outranks={}",
                        promises, getQuorumNumber(), timeouts, wrongEpochRejected, outranks);
                if (outranks >= getQuorumNumber() && !outrankedExceptions.isEmpty()) {
                    OutrankedException first = outrankedExceptions.first();
                    throw new OutrankedException(first.getNewRank(), first.getLayout());
                } else {
                    throw new QuorumUnreachableException(promises, getQuorumNumber());
                }
            }
        }

        log.debug("prepare: Successful responses={}, needed={}, timeouts={}, " +
                        "wrongEpochRejected={}, outranks={}",
                promises, getQuorumNumber(), timeouts, wrongEpochRejected, outranks);

        // Return accepted layout with the highest rank or null.
        if (acceptedLayouts.isEmpty()) {
            return null;
        }

        return acceptedLayouts.first().getLayout();
    }

    /**
     * Proposes new layout to all the servers in the current layout,
     * and can proceed only if it is accepted by a quorum.
     *
     * @param epoch The epoch for the new consensus.
     * @param rank The rank for the proposed layout.
     * @param layout The layout to propose.
     *
     * @throws QuorumUnreachableException Thrown if responses not received from a majority of
     *                                    layout servers.
     */
    @SuppressWarnings("unchecked")
    public Layout propose(long epoch, long rank, Layout layout)
            throws QuorumUnreachableException, OutrankedException {
        CompletableFuture<Boolean>[] proposeList = getLayout().getLayoutServers().stream()
                .map(x -> getRuntimeLayout().getLayoutClient(x).propose(epoch, rank, layout))
                .toArray(CompletableFuture[]::new);

        int accepts = 0;
        int timeouts = 0;
        int outranks = 0;
        int wrongEpochRejected = 0;
        TreeSet<OutrankedException> outrankedExceptions
                = new TreeSet<>(OutrankedException.OUTRANKED_EXCEPTION_COMPARATOR);

        int requestNum = proposeList.length;
        for (int i = 0; i < requestNum; i++) {
            // utilize CompletableFuture.anyOf to find a potential completed one.
            // handle exceptions only when removing cfs from list
            try {
                CFUtils.getUninterruptibly(CompletableFuture.anyOf(proposeList));
            } catch (Exception e) {
                log.trace("prepare: ", e);
            }

            Set<CompletableFuture<Boolean>> completedSet = stream(proposeList)
                    .filter(CompletableFuture::isDone)
                    .collect(Collectors.toSet());

            proposeList = stream(proposeList)
                    .filter(o -> !completedSet.contains(o))
                    .toArray(CompletableFuture[]::new);

            for (CompletableFuture<Boolean> cf : completedSet) {
                try {
                    CFUtils.getUninterruptibly(
                            cf, OutrankedException.class, TimeoutException.class,
                            NetworkException.class, WrongEpochException.class);
                    accepts++;
                } catch (TimeoutException | NetworkException e) {
                    timeouts++;
                } catch (OutrankedException oe) {
                    outranks++;
                    outrankedExceptions.add(oe);
                } catch (WrongEpochException we) {
                    wrongEpochRejected++;
                }
            }

            if (accepts >= getQuorumNumber()) {
                break;
            }

            if (accepts + proposeList.length < getQuorumNumber()) {
                log.debug("propose: Quorum unreachable, accepts={}, required={}, timeouts={}, " +
                                "wrongEpochRejected={}, outranks={}",
                        accepts, getQuorumNumber(), timeouts, wrongEpochRejected, outranks);
                if (outranks >= getQuorumNumber() && !outrankedExceptions.isEmpty()) {
                    OutrankedException first = outrankedExceptions.first();
                    throw new OutrankedException(first.getNewRank(), first.getLayout());
                } else {
                    throw new QuorumUnreachableException(accepts, getQuorumNumber());
                }
            }
        }

        log.debug("propose: Successful responses={}, needed={}, timeouts={}, "
                        + "wrongEpochRejected={}, outranks={}",
                accepts, getQuorumNumber(), timeouts, wrongEpochRejected, outranks);
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
                    if (force) {
                        return getRuntimeLayout(layout).getLayoutClient(x).force(layout);
                    }
                    return getRuntimeLayout(layout).getLayoutClient(x).committed(epoch, layout);
                })
                .toArray(CompletableFuture[]::new);


        int responses = 0;
        for (CompletableFuture cf : commitList) {
            try {
                CFUtils.getUninterruptibly(cf, WrongEpochException.class,
                        TimeoutException.class, NetworkException.class, NoBootstrapException.class);
                responses++;
            } catch (WrongEpochException e) {
                if (!force) {
                    throw e;
                }
                log.warn("committed: encountered exception", e);
            } catch (NoBootstrapException |  TimeoutException | NetworkException e) {
                log.warn("committed: encountered exception", e);
            }
        }
        log.debug("committed: Successful requests={}, responses={}", commitList.length, responses);
    }

    /**
     * Bootstraps the layout server of the specified node.
     * If already bootstrapped, it completes silently.
     *
     * @param endpoint Endpoint to bootstrap.
     * @param layout   Layout to bootstrap with.
     * @return Completable Future which completes with True when the layout server is bootstrapped.
     */
    CompletableFuture<Boolean> bootstrapLayoutServer(@Nonnull String endpoint, @Nonnull Layout layout) {
        return getRuntimeLayout(layout).getLayoutClient(endpoint).bootstrapLayout(layout)
                .exceptionally(throwable -> {
                    try {
                        CFUtils.unwrap(throwable, AlreadyBootstrappedException.class);
                    } catch (AlreadyBootstrappedException e) {
                        log.info("bootstrapLayoutServer: Layout Server {} already bootstrapped.", endpoint);
                    }
                    return true;
                })
                .thenApply(result -> {
                    log.info("bootstrapLayoutServer: Layout Server {} bootstrap successful", endpoint);
                    return true;
                });
    }
}
