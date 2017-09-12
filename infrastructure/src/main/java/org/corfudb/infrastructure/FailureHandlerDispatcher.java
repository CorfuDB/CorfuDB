package org.corfudb.infrastructure;

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.recovery.FastObjectLoader;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.LayoutModificationException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.RecoveryException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.QuorumFuturesFactory;

/**
 * The FailureHandlerDispatcher handles the trigger provided by any source
 * or policy detecting a failure in the cluster.
 *
 * <p>Created by zlokhandwala on 11/18/16.
 */
@Slf4j
public class FailureHandlerDispatcher {

    /**
     * Rank used to update layout.
     */
    private volatile long prepareRank = 1;

    /**
     * Adds a new node to the existing layout.
     *
     * @param currentLayout        Current layout.
     * @param corfuRuntime         Connected instance of the corfu runtime.
     * @param endpoint             New endpoint to be added.
     * @param isLayoutServer       is a layout server
     * @param isSequencerServer    is a sequencer server
     * @param isLogUnitServer      is a log unit server
     * @param isUnresponsiveServer is an unresponsive server
     * @param logUnitStripeIndex   stripe index to be added into if its a log unit.
     * @return True if add node is successful else False.
     * @throws CloneNotSupportedException if layout clone fails.
     * @throws QuorumUnreachableException if seal or consensus cannot be achieved.
     * @throws OutrankedException         if consensus outranked.
     * @throws InterruptedException       if fetching global tail interrupted.
     * @throws ExecutionException         if fetching global tail failed.
     */
    public boolean addNode(Layout currentLayout, CorfuRuntime corfuRuntime,
                           String endpoint,
                           boolean isLayoutServer,
                           boolean isSequencerServer,
                           boolean isLogUnitServer,
                           boolean isUnresponsiveServer,
                           int logUnitStripeIndex)
            throws CloneNotSupportedException, QuorumUnreachableException, OutrankedException,
            InterruptedException, ExecutionException {


        currentLayout.setRuntime(corfuRuntime);
        sealEpoch(currentLayout);

        LayoutWorkflowManager layoutWorkflowManager = new LayoutWorkflowManager(currentLayout);
        if (isLayoutServer) {
            layoutWorkflowManager.addLayoutServer(endpoint);
        }
        if (isSequencerServer) {
            layoutWorkflowManager.addSequencerServer(endpoint);
        }
        if (isLogUnitServer) {
            long maxGlobalTail = getMaxGlobalTail(corfuRuntime, currentLayout);
            layoutWorkflowManager.addLogunitServer(logUnitStripeIndex, maxGlobalTail, endpoint);
        }
        if (isUnresponsiveServer) {
            layoutWorkflowManager.addUnresponsiveServers(Collections.singleton(endpoint));
        }
        Layout newLayout = layoutWorkflowManager.build();
        newLayout.setRuntime(corfuRuntime);

        attemptConsensus(newLayout, corfuRuntime);

        try {
            // Add node is successful even if reconfigure sequencer fails.
            // TODO: Optimize this by retrying or submitting a workflow to retry.
            reconfigureSequencerServers(corfuRuntime, currentLayout, newLayout, true);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Bootstrapping sequencer failed due to exception : ", e);
        }

        return true;
    }

    /**
     * Merges the last 2 segments if possible.
     *
     * @param currentLayout Current layout
     * @param corfuRuntime  Connected instance of the corfu runtime.
     * @return True if merge successful, else False.
     * @throws CloneNotSupportedException  if layout clne fails.
     * @throws QuorumUnreachableException  if seal or consensus could not be achieved.
     * @throws LayoutModificationException if merge not possible in layout.
     * @throws OutrankedException          if consensus is outranked.
     */
    public boolean mergeSegments(Layout currentLayout, CorfuRuntime corfuRuntime)
            throws CloneNotSupportedException, QuorumUnreachableException,
            LayoutModificationException, OutrankedException {

        currentLayout.setRuntime(corfuRuntime);
        sealEpoch(currentLayout);

        LayoutWorkflowManager layoutWorkflowManager = new LayoutWorkflowManager(currentLayout);
        Layout newLayout = layoutWorkflowManager
                .mergePreviousSegment(currentLayout.getSegments().size() - 1)
                .build();
        newLayout.setRuntime(corfuRuntime);

        attemptConsensus(newLayout, corfuRuntime);

        try {
            reconfigureSequencerServers(corfuRuntime, currentLayout, newLayout, true);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Bootstrapping sequencer failed due to exception : ", e);
        }

        return true;
    }

    /**
     * Recover cluster from layout.
     *
     * @param recoveryLayout Layout to use to recover
     * @param corfuRuntime   Connected runtime
     * @return True if the cluster was recovered, False otherwise
     */
    public boolean recoverCluster(Layout recoveryLayout, CorfuRuntime corfuRuntime) {

        try {
            // Seals and increments the epoch.
            recoveryLayout.setRuntime(corfuRuntime);
            sealEpoch(recoveryLayout);

            // Attempts to update all the layout servers with the modified layout.
            while (true) {
                try {
                    attemptConsensus(recoveryLayout, corfuRuntime);
                } catch (OutrankedException oe) {
                    continue;
                }
                break;
            }

            //TODO: Since sequencer reset is moved after paxos. Make sure the runtime has the latest
            //TODO: layout view and latest client router epoch. (Use quorum layout fetch.)
            //TODO: Handle condition if primary sequencer is not marked ready, reset fails.
            // Reconfigure servers if required
            // Primary sequencer would already be in a not-ready state since its in recovery.
            reconfigureSequencerServers(corfuRuntime, recoveryLayout, recoveryLayout, true);

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
     * @param corfuRuntime  Connected corfu runtime instance
     * @param failedServers Set of failed server addresses
     */
    public void dispatchHandler(IFailureHandlerPolicy failureHandlerPolicy, Layout currentLayout,
                                CorfuRuntime corfuRuntime, Set<String> failedServers) {

        try {

            // Generates a new layout by removing the failed nodes from the existing layout
            Layout newLayout = failureHandlerPolicy.generateLayout(currentLayout, corfuRuntime,
                    failedServers);

            // Seals and increments the epoch.
            currentLayout.setRuntime(corfuRuntime);
            sealEpoch(currentLayout);

            try {
                attemptConsensus(newLayout, corfuRuntime);
            } catch (OutrankedException ignore) {
            }

            //TODO: Since sequencer reset is moved after paxos. Make sure the runtime has the latest
            //TODO: layout view and latest client router epoch. (Use quorum layout fetch.)
            //TODO: Handle condition if primary sequencer is not marked ready, reset fails.
            // Reconfigure servers if required
            reconfigureSequencerServers(corfuRuntime, currentLayout, newLayout, false);

        } catch (Exception e) {
            log.error("Error: dispatchHandler: {}", e);
        }
    }

    /**
     * Seals the epoch.
     * Set local epoch and then attempt to move all servers to new epoch
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
     * @param layout       Layout to propose.
     * @param corfuRuntime Connected instance of the corfu runtime.
     * @throws OutrankedException         if consensus is outranked.
     * @throws QuorumUnreachableException if consensus could not be achieved.
     */
    private void attemptConsensus(Layout layout, CorfuRuntime corfuRuntime)
            throws OutrankedException, QuorumUnreachableException {
        // Attempts to update all the layout servers with the modified layout.
        try {
            corfuRuntime.getLayoutView().updateLayout(layout, prepareRank);
            prepareRank++;
        } catch (OutrankedException oe) {
            // Update rank since outranked.
            log.error("Conflict in updating layout by failureHandlerDispatcher: {}", oe);
            // Update rank to be able to outrank other competition and complete paxos.
            prepareRank = oe.getNewRank() + 1;
            throw oe;
        }

        // Check if our proposed layout got selected and committed.
        corfuRuntime.invalidateLayout();
        if (corfuRuntime.getLayoutView().getLayout().equals(layout)) {
            log.info("New Layout Committed = {}", layout);
        } else {
            log.warn("Runtime recovered with a different layout = {}",
                    corfuRuntime.getLayoutView().getLayout());
        }
    }

    /**
     * Fetches the max global log tail from the log unit cluster. This depends on the mode of
     * replication being used.
     * CHAIN: Block on fetch of global log tail from the head log unitin every stripe.
     * QUORUM: Block on fetch of global log tail from a majority in every stripe.
     *
     * @param runtime A connected instance of the corfu runtime.
     * @param layout  Current layout.
     * @return The max global log tail obtained from the log unit servers.
     */
    private long getMaxGlobalTail(CorfuRuntime runtime, Layout layout)
            throws ExecutionException, InterruptedException {
        long maxTokenRequested = 0;
        for (Layout.LayoutSegment segment : layout.getSegments()) {

            // Query the tail of every log unit in every stripe.
            if (segment.getReplicationMode().equals(Layout.ReplicationMode.CHAIN_REPLICATION)) {
                for (Layout.LayoutStripe stripe : segment.getStripes()) {
                    maxTokenRequested = Math.max(maxTokenRequested, runtime.getRouter(stripe
                            .getLogServers().get(0))
                            .getClient(LogUnitClient.class).getTail().get());
                }
            } else if (segment.getReplicationMode()
                    .equals(Layout.ReplicationMode.QUORUM_REPLICATION)) {
                for (Layout.LayoutStripe stripe : segment.getStripes()) {
                    CompletableFuture<Long>[] completableFutures = stripe.getLogServers()
                            .stream()
                            .map(s -> runtime.getRouter(s).getClient(LogUnitClient.class)
                                    .getTail())
                            .toArray(CompletableFuture[]::new);
                    QuorumFuturesFactory.CompositeFuture<Long> quorumFuture =
                            QuorumFuturesFactory.getQuorumFuture(Comparator.naturalOrder(),
                                    completableFutures);
                    maxTokenRequested = Math.max(maxTokenRequested, quorumFuture.get());
                }
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
     * @param runtime          Runtime to reconfigure new servers.
     * @param originalLayout   Current layout to get the latest state of servers.
     * @param newLayout        New Layout to be reconfigured.
     * @param forceReconfigure Flag to force reconfiguration.
     */
    private void reconfigureSequencerServers(CorfuRuntime runtime, Layout originalLayout, Layout
            newLayout, boolean forceReconfigure)
            throws InterruptedException, ExecutionException {

        // Reconfigure Primary Sequencer if required
        if (forceReconfigure
                || !originalLayout.getSequencers().get(0).equals(newLayout.getSequencers()
                .get(0))) {
            long maxTokenRequested = getMaxGlobalTail(runtime, originalLayout);

            FastObjectLoader fastObjectLoader = new FastObjectLoader(runtime);
            fastObjectLoader.setRecoverSequencerMode(true);
            fastObjectLoader.setLoadInCache(false);

            // FastSMRLoader sets the logHead based on trim mark.
            fastObjectLoader.setLogTail(maxTokenRequested);
            fastObjectLoader.loadMaps();
            Map<UUID, Long> streamTails = fastObjectLoader.getStreamTails();
            verifyStreamTailsMap(streamTails);

            // Configuring the new sequencer.
            boolean sequencerBootstrapResult = newLayout.getSequencer(0)
                    .bootstrap(maxTokenRequested + 1, streamTails,
                            newLayout.getEpoch()).get();
            if (sequencerBootstrapResult) {
                log.info("Sequencer bootstrap successful.");
            } else {
                log.warn("Sequencer bootstrap failed. Already bootstrapped.");
            }
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