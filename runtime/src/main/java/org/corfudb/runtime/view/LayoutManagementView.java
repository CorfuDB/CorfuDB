package org.corfudb.runtime.view;

import static org.corfudb.util.Utils.getLogTail;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nonnull;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.LayoutModificationException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.RecoveryException;
import org.corfudb.util.CFUtils;

/**
 * A view of the Layout Manager to manage reconfigurations of the Corfu Cluster.
 * <p>
 * <p>Created by zlokhandwala on 11/1/17.</p>
 */
@Slf4j
public class LayoutManagementView extends AbstractView {

    public LayoutManagementView(@NonNull CorfuRuntime runtime) {
        super(runtime);
    }

    private volatile long prepareRank = 1L;

    private final ReentrantLock recoverSequencerLock = new ReentrantLock();

    /**
     * Future which is reset every time a new task to bootstrap the sequencer is launched.
     * This is to avoid multiple bootstrap requests.
     */
    private final AtomicReference<CompletableFuture<Boolean>> sequencerRecoveryFuture
            = new AtomicReference<>(CompletableFuture.completedFuture(true));

    private volatile long lastKnownSequencerEpoch = Layout.INVALID_EPOCH;

    /**
     * On restart, if MANAGEMENT_LAYOUT exists in the local datastore.
     * the Management Server attempts to recover the cluster from that layout.
     *
     * @param recoveryLayout Layout to use to recover
     * @return True if the cluster was recovered, False otherwise
     */
    public boolean attemptClusterRecovery(Layout recoveryLayout) {

        try {
            Layout layout = new Layout(recoveryLayout);
            sealEpoch(layout);
            attemptConsensus(layout);
        } catch (Exception e) {
            log.error("Error: recovery: {}", e);
            return false;
        }
        return true;
    }

    /**
     * Takes in the existing layout and a set of failed nodes.
     * It first generates a new layout by removing the failed nodes from the existing layout.
     * It then seals the epoch to prevent any client from accessing the stale layout.
     * Finally we run paxos to update all servers with the new layout.
     *
     * @param currentLayout The current layout
     * @param failedServers Set of failed server addresses
     */
    public void handleFailure(IReconfigurationHandlerPolicy failureHandlerPolicy,
                              Layout currentLayout,
                              Set<String> failedServers)
            throws OutrankedException, LayoutModificationException {

        // Generates a new layout by removing the failed nodes from the existing layout
        Layout newLayout = failureHandlerPolicy
                .generateLayout(currentLayout,
                        runtime,
                        failedServers,
                        Collections.emptySet());
        runLayoutReconfiguration(currentLayout, newLayout, false);
    }

    /**
     * Bootstraps the new node with the current layout.
     * This action is invoked as a part of the add node workflow which requires the new node
     * to be added to the cluster to be bootstrapped with the existing layout of this cluster.
     * This bootstraps the Layout and the Management server with the existing layout which
     * initiates failure handling capabilities on the management server.
     *
     * @param endpoint New node endpoint.
     * @return Completable Future which completes when the node's layout and management servers are bootstrapped.
     */
    public CompletableFuture<Boolean> bootstrapNewNode(String endpoint) {

        // Bootstrap the to-be added node with the old layout.
        Layout layout = new Layout(runtime.getLayoutView().getLayout());
        return runtime.getLayoutView().bootstrapLayoutServer(endpoint, layout)
                .thenCompose(result -> runtime.getManagementView().bootstrapManagementServer(endpoint, layout))
                .thenApply(result -> {
                    log.info("bootstrapNewNode: New node {} bootstrapped.", endpoint);
                    return true;
                });
    }

    /**
     * Adds a new node to the existing layout.
     *
     * @param currentLayout        Current layout.
     * @param endpoint             New endpoint to be added.
     * @param isLayoutServer       is a layout server
     * @param isSequencerServer    is a sequencer server
     * @param isLogUnitServer      is a log unit server
     * @param isUnresponsiveServer is an unresponsive server
     * @param logUnitStripeIndex   stripe index to be added into if its a log unit.
     * @throws OutrankedException if consensus outranked.
     */
    public void addNode(@Nonnull Layout currentLayout,
                        @Nonnull String endpoint,
                        boolean isLayoutServer,
                        boolean isSequencerServer,
                        boolean isLogUnitServer,
                        boolean isUnresponsiveServer,
                        int logUnitStripeIndex)
            throws OutrankedException {
        Layout newLayout;
        if (!currentLayout.getAllServers().contains(endpoint)) {

            sealEpoch(currentLayout);

            // Reset log unit server. This would set the requireStateTransfer flag on log unit.
            if (isLogUnitServer) {
                CFUtils.getUninterruptibly(runtime.getLayoutView().getRuntimeLayout(currentLayout)
                        .getLogUnitClient(endpoint).resetLogUnit(currentLayout.getEpoch()));
            }

            LayoutBuilder layoutBuilder = new LayoutBuilder(currentLayout);
            if (isLayoutServer) {
                layoutBuilder.addLayoutServer(endpoint);
            }
            if (isSequencerServer) {
                layoutBuilder.addSequencerServer(endpoint);
            }
            if (isLogUnitServer) {
                layoutBuilder.addLogunitServer(logUnitStripeIndex,
                        getLogTail(currentLayout, runtime),
                        endpoint);
            }
            if (isUnresponsiveServer) {
                layoutBuilder.addUnresponsiveServers(Collections.singleton(endpoint));
            }
            newLayout = layoutBuilder.build();

            attemptConsensus(newLayout);
        } else {
            log.info("Node {} already exists in the layout, skipping.", endpoint);
            newLayout = currentLayout;
        }

        // Add node is successful even if reconfigure sequencer fails.
        // TODO: Optimize this by retrying or submitting a workflow to retry.
        reconfigureSequencerServers(currentLayout, newLayout, false);
    }

    /**
     * Heals an existing node in the layout.
     *
     * @param currentLayout Current layout.
     * @param endpoint      New endpoint to be added.
     * @throws OutrankedException if consensus outranked.
     */
    public void healNode(Layout currentLayout, String endpoint) throws OutrankedException {

        Layout newLayout;

        // It is important to note that the node to be healed is a part of the unresponsive
        // servers list and is NOT a part of any of the segments. Else, resets can be triggered
        // while the node is a part of the chain causing correctness issues.
        if (currentLayout.getUnresponsiveServers().contains(endpoint)) {
            sealEpoch(currentLayout);

            // Seal the node to be healed.
            CFUtils.getUninterruptibly(runtime.getLayoutView().getRuntimeLayout(currentLayout)
                    .getBaseClient(endpoint).sealRemoteServer(currentLayout.getEpoch()));

            // Reset the node to be healed. This would set the requireStateTransfer flag on log unit.
            CFUtils.getUninterruptibly(runtime.getLayoutView().getRuntimeLayout(currentLayout)
                    .getLogUnitClient(endpoint).resetLogUnit(currentLayout.getEpoch()));

            LayoutBuilder layoutBuilder = new LayoutBuilder(currentLayout);
            layoutBuilder.addLogunitServer(0,
                    getLogTail(currentLayout, runtime),
                    endpoint);
            layoutBuilder.removeUnresponsiveServers(Collections.singleton(endpoint));
            newLayout = layoutBuilder.build();

            attemptConsensus(newLayout);
        } else {
            log.info("Node {} already exists in the layout, skipping.", endpoint);
            newLayout = currentLayout;
        }

        // Heal node is successful even if reconfigure sequencer fails.
        // TODO: Optimize this by retrying or submitting a workflow to retry.
        reconfigureSequencerServers(currentLayout, newLayout, false);
    }

    /**
     * Attempts to merge the last 2 segments.
     *
     * @param currentLayout Current layout
     * @throws OutrankedException if consensus is outranked.
     */
    public void mergeSegments(Layout currentLayout) throws OutrankedException {

        Layout newLayout;
        if (currentLayout.getSegments().size() > 1) {

            log.info("mergeSegments: layout is {}", currentLayout);

            sealEpoch(currentLayout);

            LayoutBuilder layoutBuilder = new LayoutBuilder(currentLayout);
            newLayout = layoutBuilder
                    .mergePreviousSegment(1)
                    .build();
            attemptConsensus(newLayout);
        } else {
            log.info("mergeSegments: skipping, no segments to merge {}", currentLayout);
            newLayout = currentLayout;
        }
        reconfigureSequencerServers(currentLayout, newLayout, false);
    }

    /**
     * Add the log unit to the segment to increase redundancy. This is preceded by state transfer.
     * Adds the specified log unit to the stripe in all segments, increments the epoch and
     * proposes the new layout. This method is idempotent - adds the new log unit to each segment
     * only once.
     *
     * @param currentLayout Current layout.
     * @param endpoint      Endpoint to be added to the segment.
     * @param stripeIndex   Stripe index.
     * @throws OutrankedException if consensus is outranked.
     */
    public void addLogUnitReplica(@Nonnull Layout currentLayout,
                                  @Nonnull String endpoint,
                                  int stripeIndex) throws OutrankedException {
        boolean isTaskDone = currentLayout.getSegments().stream()
                .map(layoutSegment -> layoutSegment.getStripes().get(stripeIndex))
                .allMatch(layoutStripe -> layoutStripe.getLogServers().contains(endpoint));

        if (!isTaskDone) {
            LayoutBuilder builder = new LayoutBuilder(currentLayout);
            for (int i = 0; i < currentLayout.getSegments().size(); i++) {
                builder.addLogunitServerToSegment(endpoint, i, stripeIndex);
            }
            Layout newLayout = builder.setEpoch(currentLayout.getEpoch() + 1).build();
            runLayoutReconfiguration(currentLayout, newLayout, false);
        } else {
            log.info("addLogUnitReplica: Ignoring task because {} present in all segments in {}",
                    endpoint, currentLayout);
        }
    }

    /**
     * Best effort attempt to removes a node from the layout.
     *
     * @param currentLayout the layout to remove the node from
     * @param endpoint      the node to remove
     */
    public void removeNode(@Nonnull Layout currentLayout,
                           @Nonnull String endpoint) throws OutrankedException {

        Layout newLayout;
        if (currentLayout.getAllServers().contains(endpoint)) {
            LayoutBuilder builder = new LayoutBuilder(currentLayout);
            newLayout = builder.removeLayoutServer(endpoint)
                    .removeSequencerServer(endpoint)
                    .assignResponsiveSequencerAsPrimary(Collections.emptySet())
                    .removeLogunitServer(endpoint)
                    .removeUnresponsiveServer(endpoint)
                    .setEpoch(currentLayout.getEpoch() + 1)
                    .build();

            // Seal after constructing the layout, so that the system
            // isn't blocked if the builder throws an exception
            sealEpoch(currentLayout);

            attemptConsensus(newLayout);
        } else {
            newLayout = currentLayout;
            log.info("removeNode: Ignoring remove node on {} because it doesn't exist in {}",
                    endpoint, currentLayout);
        }

        reconfigureSequencerServers(currentLayout, newLayout, false);
    }

    /**
     * Attempts to force commit a new layout to the cluster.
     *
     * @param currentLayout the current layout
     * @param forceLayout   the new layout to force
     * @throws QuorumUnreachableException
     */
    public void forceLayout(@Nonnull Layout currentLayout, @Nonnull Layout forceLayout) {
        try {
            sealEpoch(currentLayout);
        } catch (QuorumUnreachableException e) {
            log.warn("forceLayout: error while sealing ", e);
        }

        runtime.getLayoutView().committed(forceLayout.getEpoch(), forceLayout, true);
    }


    /**
     * Runs the layout reconfiguration process.
     * Seals the layout.
     * Runs paxos.
     * During paxos if in recovery mode, we increment rank until committed on same epoch.
     * If not in recovery, we fail with outrankedException.
     * The new committed layout is then verified by invalidating the runtime.
     * Finally we reconfigure the servers (bootstrapping sequencer.)
     *
     * @param currentLayout          Layout which needs to be sealed.
     * @param newLayout              New layout to be committed.
     * @param forceSequencerRecovery Flag. True if want to force sequencer recovery.
     * @throws OutrankedException if consensus is outranked.
     */
    private void runLayoutReconfiguration(Layout currentLayout, Layout newLayout,
                                          final boolean forceSequencerRecovery)
            throws OutrankedException {

        // Seals the incremented epoch (Assumes newLayout epoch = currentLayout epoch + 1).
        sealEpoch(currentLayout);

        attemptConsensus(newLayout);

        //TODO: Since sequencer reset is moved after paxos. Make sure the runtime has the latest
        //TODO: layout view and latest client router epoch. (Use quorum layout fetch.)
        //TODO: Handle condition if primary sequencer is not marked ready, reset fails.
        // Reconfigure servers if required
        // Primary sequencer would be in a not-ready state if its in recovery mode.
        reconfigureSequencerServers(currentLayout, newLayout, forceSequencerRecovery);
    }

    /**
     * Seals the epoch.
     *
     * @param layout Layout to be sealed
     */
    private void sealEpoch(Layout layout) throws QuorumUnreachableException {
        layout.nextEpoch();
        runtime.getLayoutView().getRuntimeLayout(layout).sealMinServerSet();
    }

    /**
     * Attempt consensus.
     *
     * @param layout Layout to propose.
     * @throws OutrankedException if consensus is outranked.
     */
    private void attemptConsensus(Layout layout)
            throws OutrankedException {
        // Attempts to update all the layout servers with the modified layout.
        try {
            runtime.getLayoutView().updateLayout(layout, prepareRank);
            prepareRank = 1L;
        } catch (OutrankedException oe) {
            // Update rank since outranked.
            log.error("Conflict in updating layout by attemptConsensus: {}", oe);
            // Update rank to be able to outrank other competition and complete paxos.
            prepareRank = oe.getNewRank() + 1;
            throw oe;
        }

        // Check if our proposed layout got selected and committed.
        for (int x = 0; x < runtime.getParameters().getInvalidateRetry(); x++) {
            runtime.invalidateLayout();
            if (runtime.getLayoutView().getLayout().equals(layout)) {
                log.info("New Layout Committed = {}", layout);
                return;
            } else {
                log.warn("Runtime recovered with a different layout = {}",
                        runtime.getLayoutView().getLayout());
            }
            log.warn("attemptConsensus: Checking if {} has been accepted, retry {}", layout, x);
        }
    }

    /**
     * Reconfigures the sequencer.
     * If the primary sequencer has changed in the new layout,
     * the global tail of the log units are queried and used to set
     * the initial token of the new primary sequencer.
     *
     * @param originalLayout   Current layout to get the latest state of servers.
     * @param newLayout        New Layout to be reconfigured.
     * @param forceReconfigure Flag to force reconfiguration.
     */
    public void reconfigureSequencerServers(Layout originalLayout, Layout newLayout,
                                            boolean forceReconfigure) {

        boolean acquiredLocked = recoverSequencerLock.tryLock();
        if (acquiredLocked) {
            try {

                // Avoids stale bootstrap requests from being processed.
                if (newLayout.getEpoch() <= lastKnownSequencerEpoch) {
                    log.warn("reconfigureSequencerServers: Sequencer bootstrap failed. "
                            + "Already bootstrapped.");
                    return;
                }

                long maxTokenRequested = -1L;
                Map<UUID, StreamAddressSpace> streamsAddressSpace = Collections.emptyMap();
                boolean bootstrapWithoutTailsUpdate = true;

                // Reconfigure Primary Sequencer if required
                if (forceReconfigure
                        || !originalLayout.getPrimarySequencer()
                        .equals(newLayout.getPrimarySequencer())) {

                    StreamsAddressResponse streamsAddressesResponse = runtime.getAddressSpaceView()
                            .getLogAddressSpace();

                    maxTokenRequested = streamsAddressesResponse.getLogTail();
                    streamsAddressSpace = streamsAddressesResponse.getAddressMap();

                    // Incrementing the maxTokenRequested value for sequencer reset.
                    maxTokenRequested++;
                    bootstrapWithoutTailsUpdate = false;
                }

                // Configuring the new sequencer.
                boolean sequencerBootstrapResult = CFUtils.getUninterruptibly(
                        runtime.getLayoutView().getRuntimeLayout(newLayout)
                                .getPrimarySequencerClient()
                                .bootstrap(maxTokenRequested, streamsAddressSpace, newLayout.getEpoch(),
                                        bootstrapWithoutTailsUpdate));
                lastKnownSequencerEpoch = newLayout.getEpoch();
                if (sequencerBootstrapResult) {
                    log.info("reconfigureSequencerServers: Sequencer bootstrap successful.");
                } else {
                    log.warn("reconfigureSequencerServers: Sequencer bootstrap failed. "
                            + "Already bootstrapped.");
                }
            } finally {
                recoverSequencerLock.unlock();
            }
        } else {
            log.info("reconfigureSequencerServers: Sequencer reconfiguration already in progress.");
        }
    }

    /**
     * Triggers a new task to bootstrap the sequencer for the specified layout. If there is already
     * a task in progress, this is a no-op.
     *
     * @param layout Layout to use to bootstrap the primary sequencer.
     * @return Future which completes when the task completes successfully or with a failure.
     */
    public CompletableFuture<Boolean> asyncSequencerBootstrap(
            @NonNull Layout layout, @NonNull ExecutorService service) {

        return sequencerRecoveryFuture.updateAndGet(sequencerRecovery -> {
            if (!sequencerRecovery.isDone()) {
                log.info("triggerSequencerBootstrap: a bootstrap task is already in progress.");
                return sequencerRecovery;
            }

            return CompletableFuture.supplyAsync(() -> {
                log.info("triggerSequencerBootstrap: a bootstrap task is triggered.");
                try {
                    reconfigureSequencerServers(layout, layout, true);
                } catch (Exception e) {
                    log.error("triggerSequencerBootstrap: Failed with Exception: ", e);
                }
                return true;
            }, service);
        });
    }

    /**
     * Verifies whether there are any invalid stream tails.
     *
     * @param streamTails Stream tails map obtained from the fastSMRLoader.
     */
    private void verifyStreamTailsMap(Map<UUID, Long> streamTails) {
        for (Long value : streamTails.values()) {
            if (value < 0) {
                log.error("Stream Tails map verification failed. Map = {}", streamTails);
                throw new RecoveryException("Invalid stream tails found in map.");
            }
        }
    }
}
