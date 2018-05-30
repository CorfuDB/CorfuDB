package org.corfudb.runtime.view;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.annotation.Nonnull;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.recovery.FastObjectLoader;
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

    /**
     * On restart, if MANAGEMENT_LAYOUT exists in the local datastore.
     * the Management Server attempts to recover the cluster from that layout.
     *
     * @param recoveryLayout Layout to use to recover
     */
    public void recoverCluster(Layout recoveryLayout)
            throws QuorumUnreachableException, OutrankedException {

        Layout newLayout = new Layout(recoveryLayout);
        newLayout.setEpoch(recoveryLayout.getEpoch() + 1);
        runLayoutReconfiguration(recoveryLayout, newLayout, true);
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
     * This bootstraps the Layout and the Management server with the existing layout and also
     * initiates failure handling capabilities on the management server.
     *
     * @param endpoint New node endpoint.
     */
    public void bootstrapNewNode(String endpoint) {

        // Bootstrap the to-be added node with the old layout.
        Layout layout = new Layout(runtime.getLayoutView().getLayout());
        // Ignoring call result as the call returns ACK or throws an exception.
        CFUtils.getUninterruptibly(runtime.getLayoutView().getRuntimeLayout(layout)
                .getLayoutClient(endpoint)
                .bootstrapLayout(layout));
        CFUtils.getUninterruptibly(runtime.getLayoutView().getRuntimeLayout(layout)
                .getManagementClient(endpoint)
                .bootstrapManagement(layout));

        log.info("bootstrapNewNode: New node {} bootstrapped.", endpoint);
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

            LayoutBuilder layoutBuilder = new LayoutBuilder(currentLayout);
            if (isLayoutServer) {
                layoutBuilder.addLayoutServer(endpoint);
            }
            if (isSequencerServer) {
                layoutBuilder.addSequencerServer(endpoint);
            }
            if (isLogUnitServer) {
                Layout.LayoutSegment latestSegment =
                        currentLayout.getSegments().get(currentLayout.getSegments().size() - 1);
                layoutBuilder.addLogunitServer(logUnitStripeIndex,
                        getMaxGlobalTail(currentLayout, latestSegment),
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
        if (currentLayout.getUnresponsiveServers().contains(endpoint)) {
            sealEpoch(currentLayout);

            LayoutBuilder layoutBuilder = new LayoutBuilder(currentLayout);
            Layout.LayoutSegment latestSegment =
                    currentLayout.getSegments().get(currentLayout.getSegments().size() - 1);
            layoutBuilder.addLogunitServer(0,
                    getMaxGlobalTail(currentLayout, latestSegment),
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
     * @param forceLayout the new layout to force
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
        layout.setEpoch(layout.getEpoch() + 1);
        runtime.getLayoutView().getRuntimeLayout(layout).moveServersToEpoch();
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
     * Fetches the max global log tail from the log unit cluster. This depends on the mode of
     * replication being used.
     * CHAIN: Block on fetch of global log tail from the head log unitin every stripe.
     * QUORUM: Block on fetch of global log tail from a majority in every stripe.
     *
     * @param layout  Latest layout to get clients to fetch tails.
     * @param segment Latest layout segment.
     * @return The max global log tail obtained from the log unit servers.
     */
    private long getMaxGlobalTail(Layout layout, Layout.LayoutSegment segment) {
        long maxTokenRequested = 0;

        // Query the tail of the head log unit in every stripe.
        if (segment.getReplicationMode().equals(Layout.ReplicationMode.CHAIN_REPLICATION)) {
            for (Layout.LayoutStripe stripe : segment.getStripes()) {
                maxTokenRequested = Math.max(maxTokenRequested,
                        CFUtils.getUninterruptibly(
                                runtime.getLayoutView().getRuntimeLayout(layout)
                                        .getLogUnitClient(stripe.getLogServers().get(0))
                                        .getTail()));

            }
        } else if (segment.getReplicationMode()
                .equals(Layout.ReplicationMode.QUORUM_REPLICATION)) {
            for (Layout.LayoutStripe stripe : segment.getStripes()) {
                CompletableFuture<Long>[] completableFutures = stripe.getLogServers()
                        .stream()
                        .map(s -> runtime.getLayoutView().getRuntimeLayout(layout)
                                .getLogUnitClient(s).getTail())
                        .toArray(CompletableFuture[]::new);
                QuorumFuturesFactory.CompositeFuture<Long> quorumFuture =
                        QuorumFuturesFactory.getQuorumFuture(Comparator.naturalOrder(),
                                completableFutures);
                maxTokenRequested = Math.max(maxTokenRequested,
                        CFUtils.getUninterruptibly(quorumFuture));

            }
        }
        return maxTokenRequested;
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

        long maxTokenRequested = 0L;
        Map<UUID, Long> streamTails = Collections.emptyMap();
        boolean bootstrapWithoutTailsUpdate = true;

        // Reconfigure Primary Sequencer if required
        if (forceReconfigure
                || !originalLayout.getSequencers().get(0).equals(newLayout.getSequencers()
                .get(0))) {
            Layout.LayoutSegment latestSegment = newLayout.getSegments()
                    .get(newLayout.getSegments().size() - 1);
            maxTokenRequested = getMaxGlobalTail(newLayout, latestSegment);

            FastObjectLoader fastObjectLoader = new FastObjectLoader(runtime);
            fastObjectLoader.setRecoverSequencerMode(true);
            fastObjectLoader.setLoadInCache(false);

            // FastSMRLoader sets the logHead based on trim mark.
            fastObjectLoader.setLogTail(maxTokenRequested);
            fastObjectLoader.loadMaps();
            streamTails = fastObjectLoader.getStreamTails();
            verifyStreamTailsMap(streamTails);

            // Incrementing the maxTokenRequested value for sequencer reset.
            maxTokenRequested++;
            bootstrapWithoutTailsUpdate = false;
        }

        // Configuring the new sequencer.
        boolean sequencerBootstrapResult = CFUtils.getUninterruptibly(
                runtime.getLayoutView().getRuntimeLayout(newLayout)
                        .getPrimarySequencerClient()
                        .bootstrap(maxTokenRequested, streamTails, newLayout.getEpoch(),
                                bootstrapWithoutTailsUpdate));
        if (sequencerBootstrapResult) {
            log.info("Sequencer bootstrap successful.");
        } else {
            log.warn("Sequencer bootstrap failed. Already bootstrapped.");
        }
    }

    /**
     * Verifies whether there are any invalid streamTails.
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
