package org.corfudb.infrastructure.management;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.recovery.FastObjectLoader;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.RecoveryException;
import org.corfudb.runtime.view.Layout;

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
     * Recover cluster from layout.
     *
     * @param recoveryLayout Layout to use to recover
     * @param corfuRuntime   Connected runtime
     * @return True if the cluster was recovered, False otherwise
     */
    public boolean recoverCluster(Layout recoveryLayout, CorfuRuntime corfuRuntime) {

        try {
            Layout newLayout = (Layout)recoveryLayout.clone();
            newLayout.setEpoch(recoveryLayout.getEpoch() + 1);
            return runLayoutReconfiguration(recoveryLayout, newLayout, corfuRuntime, true);
        } catch (Exception e) {
            log.error("Error: recovery: {}", e);
            return false;
        }
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
    public boolean dispatchHandler(IFailureHandlerPolicy failureHandlerPolicy,
                                   Layout currentLayout,
                                   CorfuRuntime corfuRuntime,
                                   Set<String> failedServers,
                                   Set<String> healedServers) {

        try {
            // Generates a new layout by removing the failed nodes from the existing layout
            Layout newLayout = failureHandlerPolicy
                    .generateLayout(currentLayout, corfuRuntime, failedServers, healedServers);

            return runLayoutReconfiguration(currentLayout, newLayout, corfuRuntime, true);
        } catch (Exception e) {
            log.error("Error: dispatchHandler: {}", e);
            return false;
        }
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
     * @param currentLayout   Layout which needs to be sealed.
     * @param newLayout    New layout to be committed.
     * @param corfuRuntime Runtime to use to commit new layout.
     * @param recovery     Flag. True if recovery mode.
     * @return Boolean - Result of reconfiguration.
     * @throws Exception Exception if reconfiguration failure.
     */
    private boolean runLayoutReconfiguration(Layout currentLayout, Layout newLayout,
                                             CorfuRuntime corfuRuntime, final boolean recovery)
            throws Exception {

        // Seals the incremented epoch (Assumes newLayout epoch = currentLayout epoch + 1).
        newLayout.setRuntime(corfuRuntime);
        currentLayout.setRuntime(corfuRuntime);
        currentLayout.setEpoch(currentLayout.getEpoch() + 1);
        sealEpoch(currentLayout);

        try {
            corfuRuntime.getLayoutView().updateLayout(newLayout, prepareRank);
            prepareRank++;
        } catch (OutrankedException oe) {
            // Update rank since outranked.
            log.error("Layout update attempt outranked: {}", oe);
            // Update rank to be able to outrank other competition and complete paxos.
            prepareRank = oe.getNewRank() + 1;
            // If outranked, we retry in the next scheduled cycle.
            return false;
        }

        // Check if our proposed layout got selected and committed.
        corfuRuntime.getLayoutView().getLayout();
        corfuRuntime.invalidateLayout();
        if (corfuRuntime.getLayoutView().getLayout().equals(newLayout)) {
            log.info("New Layout committed = {}", newLayout);
        } else {
            log.warn("Layout recovered with a different layout = {}",
                    corfuRuntime.getLayoutView().getLayout());
        }

        //TODO: Since sequencer reset is moved after paxos. Make sure the runtime has the latest
        //TODO: layout view and latest client router epoch. (Use quorum layout fetch.)
        //TODO: Handle condition if primary sequencer is not marked ready, reset fails.
        // Reconfigure servers if required
        // Primary sequencer would be in a not-ready state if its in recovery mode.
        reconfigureServers(corfuRuntime, currentLayout, newLayout, recovery);

        return true;
    }

    /**
     * Seals the epoch.
     * Set local epoch and then attempt to move all servers to new epoch
     *
     * @param layout Layout to be sealed
     */
    private void sealEpoch(Layout layout) throws QuorumUnreachableException {
        layout.moveServersToEpoch();
    }

    /**
     * Reconfigures the servers in the new layout if reconfiguration required.
     *
     * @param runtime          Runtime to reconfigure new servers.
     * @param originalLayout   Current layout to get the latest state of servers.
     * @param newLayout        New Layout to be reconfigured.
     * @param forceReconfigure Flag to force reconfiguration.
     */
    private void reconfigureServers(CorfuRuntime runtime, Layout originalLayout, Layout
            newLayout, boolean forceReconfigure)
            throws ExecutionException {

        // Reconfigure the primary Sequencer Server if changed.
        reconfigureSequencerServers(runtime, originalLayout, newLayout, forceReconfigure);

        // TODO: Reconfigure log units if new log unit added.
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
            throws ExecutionException {

        // Reconfigure Primary Sequencer if required
        if (forceReconfigure
                || !originalLayout.getSequencers().get(0).equals(newLayout.getSequencers()
                .get(0))) {
            long maxTokenRequested = 0;
            for (Layout.LayoutSegment segment : originalLayout.getSegments()) {
                // Query the tail of every log unit in every stripe.
                for (Layout.LayoutStripe stripe : segment.getStripes()) {
                    for (String logServer : stripe.getLogServers()) {
                        try {
                            long tail = runtime.getRouter(logServer).getClient(LogUnitClient
                                    .class).getTail().get();
                            if (tail != 0) {
                                maxTokenRequested = maxTokenRequested > tail ? maxTokenRequested
                                        : tail;
                            }
                        } catch (Exception e) {
                            log.error("Exception while fetching log unit tail : {}", e);
                        }
                    }
                }
            }

            try {

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

            } catch (InterruptedException e) {
                log.error("Sequencer bootstrap interrupted : {}", e);
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