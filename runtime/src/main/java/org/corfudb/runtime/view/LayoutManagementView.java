package org.corfudb.runtime.view;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.recovery.FastObjectLoader;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.RecoveryException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
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
            throws QuorumUnreachableException, OutrankedException, ExecutionException {

        Layout newLayout = new Layout(recoveryLayout);
        newLayout.setEpoch(recoveryLayout.getEpoch() + 1);
        newLayout.setRuntime(runtime);
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
     * @param healedServers Set of healed server addresses
     */
    public void handleFailure(IFailureHandlerPolicy failureHandlerPolicy,
                              Layout currentLayout,
                              Set<String> failedServers,
                              Set<String> healedServers)
            throws QuorumUnreachableException, OutrankedException, ExecutionException {

        // Generates a new layout by removing the failed nodes from the existing layout
        Layout newLayout = failureHandlerPolicy
                .generateLayout(currentLayout,
                        runtime,
                        failedServers,
                        healedServers);
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
        IClientRouter newEndpointRouter = runtime.getRouter(endpoint);
        Layout layout = new Layout(runtime.getLayoutView().getLayout());
        // Ignoring call result as the call returns ACK or throws an exception.
        CFUtils.getUninterruptibly(newEndpointRouter.getClient(LayoutClient.class)
                .bootstrapLayout(layout));
        CFUtils.getUninterruptibly(newEndpointRouter.getClient(ManagementClient.class)
                .bootstrapManagement(layout));
        CFUtils.getUninterruptibly(newEndpointRouter.getClient(ManagementClient.class)
                .initiateFailureHandler());

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
     * @throws QuorumUnreachableException if seal or consensus cannot be achieved.
     * @throws OutrankedException         if consensus outranked.
     * @throws ExecutionException         if fetching global tail failed.
     */
    public void addNode(Layout currentLayout,
                        String endpoint,
                        boolean isLayoutServer,
                        boolean isSequencerServer,
                        boolean isLogUnitServer,
                        boolean isUnresponsiveServer,
                        int logUnitStripeIndex)
            throws QuorumUnreachableException, OutrankedException {
        Layout newLayout;
        if (!currentLayout.getAllServers().contains(endpoint)) {

            currentLayout.setRuntime(runtime);
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
                        getMaxGlobalTail(latestSegment),
                        endpoint);
            }
            if (isUnresponsiveServer) {
                layoutBuilder.addUnresponsiveServers(Collections.singleton(endpoint));
            }
            newLayout = layoutBuilder.build();
            newLayout.setRuntime(runtime);

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
     * Attempts to merge the last 2 segments.
     *
     * @param currentLayout Current layout
     * @return True if merge successful, else False.
     * @throws QuorumUnreachableException if seal or consensus could not be achieved.
     * @throws OutrankedException         if consensus is outranked.
     */
    public boolean mergeSegments(Layout currentLayout)
            throws QuorumUnreachableException, OutrankedException, ExecutionException {

        Layout newLayout;
        if (currentLayout.getSegments().size() > 1) {

            log.info("mergeSegments: layout is {}", currentLayout);

            currentLayout.setRuntime(runtime);
            sealEpoch(currentLayout);

            LayoutBuilder layoutBuilder = new LayoutBuilder(currentLayout);
            newLayout = layoutBuilder
                    .mergePreviousSegment(currentLayout.getSegments().size() - 1)
                    .build();
            newLayout.setRuntime(runtime);
            attemptConsensus(newLayout);
        } else {
            log.info("mergeSegments: skipping, no segments to merge {}", currentLayout);
            newLayout = currentLayout;
        }
        reconfigureSequencerServers(currentLayout, newLayout, false);
        return true;
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
     * @throws QuorumUnreachableException if seal or consensus could not be achieved.
     * @throws OutrankedException         if consensus is outranked.
     * @throws ExecutionException         if fetching global tail failed.
     */
    @SuppressWarnings("checkstyle:printStackTrace")
    private void runLayoutReconfiguration(Layout currentLayout, Layout newLayout,
                                          final boolean forceSequencerRecovery)
            throws QuorumUnreachableException, OutrankedException, ExecutionException {

        // Seals the incremented epoch (Assumes newLayout epoch = currentLayout epoch + 1).
        currentLayout.setRuntime(runtime);
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
        layout.moveServersToEpoch();
    }

    /**
     * Attempt consensus.
     *
     * @param layout Layout to propose.
     * @throws OutrankedException         if consensus is outranked.
     * @throws QuorumUnreachableException if consensus could not be achieved.
     */
    private void attemptConsensus(Layout layout)
            throws OutrankedException, QuorumUnreachableException {
        // Attempts to update all the layout servers with the modified layout.
        try {
            runtime.getLayoutView().updateLayout(layout, prepareRank);
            prepareRank = 1L;
        } catch (OutrankedException oe) {
            // Update rank since outranked.
            log.error("Conflict in updating layout by failureHandlerDispatcher: {}", oe);
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
     * @param segment Latest layout segment.
     * @return The max global log tail obtained from the log unit servers.
     */
    private long getMaxGlobalTail(Layout.LayoutSegment segment) {
        long maxTokenRequested = 0;

        // Query the tail of the head log unit in every stripe.
        if (segment.getReplicationMode().equals(Layout.ReplicationMode.CHAIN_REPLICATION)) {
            for (Layout.LayoutStripe stripe : segment.getStripes()) {
                maxTokenRequested = Math.max(maxTokenRequested, CFUtils.getUninterruptibly(runtime.getRouter(stripe
                        .getLogServers().get(0))
                        .getClient(LogUnitClient.class).getTail()));

            }
        } else if (segment.getReplicationMode()
                .equals(Layout.ReplicationMode.QUORUM_REPLICATION)) {
            for (Layout.LayoutStripe stripe : segment.getStripes()) {
                CompletableFuture<Long>[] completableFutures = stripe.getLogServers()
                        .stream()
                        .map(s -> runtime.getRouter(s).getClient(LogUnitClient.class).getTail())
                        .toArray(CompletableFuture[]::new);
                QuorumFuturesFactory.CompositeFuture<Long> quorumFuture =
                        QuorumFuturesFactory.getQuorumFuture(Comparator.naturalOrder(),
                                completableFutures);
                maxTokenRequested = Math.max(maxTokenRequested, CFUtils.getUninterruptibly(quorumFuture));

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
    private void reconfigureSequencerServers(Layout originalLayout, Layout newLayout,
                                             boolean forceReconfigure) {

        long maxTokenRequested = 0L;
        Map<UUID, Long> streamTails = Collections.emptyMap();

        // Reconfigure Primary Sequencer if required
        if (forceReconfigure
                || !originalLayout.getSequencers().get(0).equals(newLayout.getSequencers()
                .get(0))) {
            Layout.LayoutSegment latestSegment = newLayout.getSegments()
                    .get(newLayout.getSegments().size() - 1);
            maxTokenRequested = getMaxGlobalTail(latestSegment);

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
        }

        // Configuring the new sequencer.
        boolean sequencerBootstrapResult = CFUtils.getUninterruptibly(newLayout.getSequencer(0)
                .bootstrap(maxTokenRequested, streamTails, newLayout.getEpoch()));
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
