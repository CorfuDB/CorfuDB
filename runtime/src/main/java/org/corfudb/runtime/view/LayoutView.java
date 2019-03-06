package org.corfudb.runtime.view;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.LayoutPrepareResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
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
     * Currently the code can drive only one Layout change.
     * If it has to drive a previously incomplete round it will drop it's own set of changes.
     * A change of layout proposal consists of a rank and the desired layout.
     * The new to-be-proposed layout can only have an epoch ONE greater than the current epoch.
     *
     * @param layout The layout to propose.
     * @param rank   The rank for the proposed layout.
     * @throws QuorumUnreachableException Thrown if responses not received from a majority of layout servers.
     * @throws OutrankedException         outranked exception, i.e., higher rank.
     * @throws WrongEpochException        wrong epoch number.
     */
    public void updateLayout(Layout layout, long rank) throws OutrankedException {

        // Note this step is done because we have added the layout to the Epoch.
        long epoch = layout.getEpoch();

        // We now fetch the latest layout known to the runtime.
        Layout currentLayout = getLayout();

        // Check if the proposed layout has epoch one greater than the current epoch.
        // The layout increments are densely monotonic.
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

        // phase 1: prepare with a given rank.
        Layout alreadyProposedLayout = prepare(currentLayout, epoch, rank);
        // Note: If a layout has already been proposed, we drop our layout and complete the round with the proposed
        // layout.
        Layout layoutToPropose = alreadyProposedLayout != null ? alreadyProposedLayout : layout;
        // phase 2: propose the new layout.
        propose(currentLayout, epoch, rank, layoutToPropose);
        // phase 3: committed
        committed(epoch, layoutToPropose);
    }

    private <T> List<CompletableFuture<T>> requestParallel(final Layout currentLayout,
                                                           final Function<String, CompletableFuture<T>> function) {
        return currentLayout.getLayoutServers().stream()
                .map(layoutServer -> {
                    CompletableFuture<T> cf = new CompletableFuture<>();
                    try {
                        cf = function.apply(layoutServer);
                    } catch (Exception e) {
                        cf.completeExceptionally(e);
                    }
                    return cf;
                })
                .collect(Collectors.toList());
    }

    private <T> void waitForAnyCompletion(final List<CompletableFuture<T>> completableFutureList) {
        try {
            CompletableFuture.anyOf(completableFutureList.stream().toArray(CompletableFuture[]::new)).get();
        } catch (Exception ignored) {
            // We only wait for a completable future to complete here. All exceptions are ignored.
        }
    }

    /**
     * Sends prepare to the current layout and can proceed only if it is accepted by a quorum.
     *
     * @param rank The rank for the proposed layout.
     * @return layout
     * @throws QuorumUnreachableException Thrown if responses not received from a majority of layout servers.
     * @throws OutrankedException         outranked exception, i.e., higher rank.
     * @throws WrongEpochException        wrong epoch number.
     */
    public Layout prepare(Layout currentLayout, long epoch, long rank) {

        final List<LayoutPrepareResponse> acceptList = new ArrayList<>();
        final AtomicInteger rejected = new AtomicInteger(0);

        final List<CompletableFuture<LayoutPrepareResponse>> pendingList = requestParallel(currentLayout,
                layoutServer -> getRuntimeLayout().getLayoutClient(layoutServer).prepare(epoch, rank));

        // Stay in loop until we get enough responses.
        // Or until we get enough rejects in which case we throw a QuorumUnreachableException.
        while (acceptList.size() < getQuorumNumber()) {

            if (pendingList.size() + acceptList.size() < getQuorumNumber()) {
                log.debug("prepare: Quorum unreachable, remaining={}, required={}",
                        pendingList.size(), getQuorumNumber());
                throw new QuorumUnreachableException(pendingList.size(), getQuorumNumber());
            }

            // wait for someone to complete.
            waitForAnyCompletion(pendingList);

            // remove errors or completed futures.
            pendingList.removeIf(cf -> {
                try {
                    LayoutPrepareResponse layoutPrepareResponse = cf.getNow(null);
                    if (layoutPrepareResponse != null) {
                        // count successes.
                        acceptList.add(layoutPrepareResponse);
                        return true;
                    }
                } catch (Exception e) {
                    // Exception is instance of CompletionException wrapping the thrown Exception.
                    if (e.getCause() instanceof OutrankedException) {
                        throw (OutrankedException) e.getCause();
                    }
                    rejected.incrementAndGet();
                    return true;
                }

                // Do not remove from pending list if the future has not been completed.
                return false;
            });

            log.debug("prepare: Successful responses={}, needed={}, rejected={}",
                    acceptList.size(), getQuorumNumber(), rejected);
        }

        // Return any layouts that have been proposed before.
        return acceptList.stream()
                .filter(x -> x.getLayout() != null)
                // Choose the layout with the highest rank proposed before.
                .max(Comparator.comparingLong(LayoutPrepareResponse::getRank))
                .map(LayoutPrepareResponse::getLayout)
                .orElse(null);
    }

    /**
     * Proposes new layout to all the servers in the current layout.
     *
     * @throws QuorumUnreachableException Thrown if responses not received from a majority of
     *                                    layout servers.
     * @throws OutrankedException         outranked exception, i.e., higher rank.
     */
    public Layout propose(Layout currentLayout, long epoch, long rank, Layout layout) {

        final AtomicInteger acceptedProposals = new AtomicInteger(0);
        final AtomicInteger rejected = new AtomicInteger(0);

        final List<CompletableFuture<Boolean>> proposeList = requestParallel(currentLayout,
                layoutServer -> getRuntimeLayout().getLayoutClient(layoutServer).propose(epoch, rank, layout));

        while (acceptedProposals.get() < getQuorumNumber()) {
            // do we still have enough for a quorum?
            if ((proposeList.size() + acceptedProposals.get()) < getQuorumNumber()) {
                log.debug("propose: Quorum unreachable, remaining={}, required={}", proposeList, getQuorumNumber());
                throw new QuorumUnreachableException(proposeList.size(), getQuorumNumber());
            }

            // wait for someone to complete.
            waitForAnyCompletion(proposeList);

            // remove errors.
            proposeList.removeIf(cf -> {
                try {
                    if (cf.getNow(false)) {
                        // count successes.
                        acceptedProposals.incrementAndGet();
                        return true;
                    }
                } catch (Exception e) {
                    // Exception is instance of CompletionException wrapping the thrown Exception.
                    if (e.getCause() instanceof OutrankedException) {
                        throw (OutrankedException) e.getCause();
                    }
                    rejected.incrementAndGet();
                    return true;
                }
                return false;
            });

            log.debug("propose: Successful responses={}, needed={}, rejected={}",
                    acceptedProposals.get(), getQuorumNumber(), rejected);
        }

        return layout;
    }

    /**
     * Send committed layout to the old Layout servers and the new Layout Servers.
     *
     * @throws WrongEpochException wrong epoch number.
     */
    public void committed(long epoch, Layout layout) {
        committed(epoch, layout, false);
    }

    /**
     * Send committed layout to the old Layout servers and the new Layout Servers.
     * If force is true, then the layout forced on all layout servers.
     *
     * @throws WrongEpochException wrong epoch number.
     */
    public void committed(long epoch, Layout layout, boolean force) {
        List<CompletableFuture<Boolean>> commitList = requestParallel(layout,
                layoutServer -> {
                    if (force) {
                        return getRuntimeLayout(layout).getLayoutClient(layoutServer).force(layout);
                    }
                    return getRuntimeLayout(layout).getLayoutClient(layoutServer).committed(epoch, layout);
                });

        final AtomicInteger acceptedCommits = new AtomicInteger(0);
        final AtomicInteger rejected = new AtomicInteger(0);
        while (acceptedCommits.get() < commitList.size()) {

            waitForAnyCompletion(commitList);

            commitList.removeIf(cf -> {
                try {
                    if (cf.getNow(false)) {
                        // count successes.
                        acceptedCommits.incrementAndGet();
                        return true;
                    }
                } catch (Exception e) {
                    log.warn("committed: Error while committing", e);
                    rejected.incrementAndGet();
                    return true;
                }
                return false;
            });

            log.debug("committed: Successful responses={}, rejected={}", acceptedCommits.get(), rejected.get());
        }
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
