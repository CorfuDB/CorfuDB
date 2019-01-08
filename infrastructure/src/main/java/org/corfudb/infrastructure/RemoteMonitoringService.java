package org.corfudb.infrastructure;

import static org.corfudb.util.LambdaUtils.runSansThrow;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.management.ClusterRecommendationEngine;
import org.corfudb.infrastructure.management.ClusterRecommendationEngineFactory;
import org.corfudb.infrastructure.management.ClusterRecommendationStrategy;
import org.corfudb.infrastructure.management.ClusterStateContext;
import org.corfudb.infrastructure.management.IDetector;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics.SequencerStatus;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.QuorumFuturesFactory;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.concurrent.SingletonResource;

/**
 * Remote Monitoring Service constitutes of failure and healing monitoring and handling.
 * This service is responsible for heartbeat and aggregating the cluster view. This is
 * updated in the shared context with the management server which serves the heartbeat responses.
 * The failure detector updates the unreachable nodes in the layout.
 * The healing detector heals nodes which were previously marked unresponsive but have now healed.
 * Created by zlokhandwala on 11/2/18.
 */
@Slf4j
public class RemoteMonitoringService implements MonitoringService {

    /**
     * Detectors to be used to detect failures and healing.
     */
    @Getter
    private final IDetector failureDetector;
    @Getter
    private final IDetector healingDetector;

    /**
     * Detection Task Scheduler Service
     * This service schedules the following tasks every POLICY_EXECUTE_INTERVAL (1 sec):
     * - Detection of failed nodes.
     * - Detection of healed nodes.
     */
    @Getter
    private final ScheduledExecutorService detectionTasksScheduler;
    /**
     * To dispatch tasks for failure or healed nodes detection.
     */
    @Getter
    private final ExecutorService detectionTaskWorkers;

    private final ServerContext serverContext;
    private final SingletonResource<CorfuRuntime> runtimeSingletonResource;

    /**
     * Cluster state context to update the connectivity graph populated by the failure and healing
     * detectors.
     */
    private final ClusterStateContext clusterStateContext;

    /**
     * Future for periodic failure and healed nodes detection task.
     */
    private Future failureDetectorFuture = CompletableFuture.completedFuture(true);
    private Future healingDetectorFuture = CompletableFuture.completedFuture(true);

    ClusterRecommendationEngine recommendationEngine = ClusterRecommendationEngineFactory
            .createForStrategy(ClusterRecommendationStrategy.FULLY_CONNECTED_CLUSTER);

    /**
     * The management agent attempts to bootstrap a NOT_READY sequencer if the
     * sequencerNotReadyCounter counter exceeds this value.
     */
    private static final int SEQUENCER_NOT_READY_THRESHOLD = 3;

    /**
     * This tuple maintains, in an epoch, how many heartbeats the primary sequencer has responded
     * in not bootstrapped (NOT_READY) state.
     */
    @Getter
    @Setter
    @AllArgsConstructor
    private class SequencerNotReadyCounter {
        private final long epoch;
        private int counter;
    }

    private SequencerNotReadyCounter sequencerNotReadyCounter;

    RemoteMonitoringService(@NonNull ServerContext serverContext,
                            @NonNull SingletonResource<CorfuRuntime> runtimeSingletonResource,
                            @NonNull ClusterStateContext clusterStateContext,
                            @NonNull IDetector failureDetector,
                            @NonNull IDetector healingDetector) {
        this.serverContext = serverContext;
        this.runtimeSingletonResource = runtimeSingletonResource;
        this.clusterStateContext = clusterStateContext;

        this.failureDetector = failureDetector;
        this.healingDetector = healingDetector;

        final int managementServiceCount = 1;
        final int detectionWorkersCount = 3;

        this.detectionTasksScheduler = Executors.newScheduledThreadPool(
                managementServiceCount,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(serverContext.getThreadPrefix() + "ManagementService")
                        .build());

        // Creating the detection worker thread pool.
        // This thread pool is utilized to dispatch detection tasks at regular intervals in the
        // detectorTaskScheduler.
        this.detectionTaskWorkers = Executors.newFixedThreadPool(
                detectionWorkersCount,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(serverContext.getThreadPrefix() + "DetectionWorker-%d")
                        .build());
    }

    private CorfuRuntime getCorfuRuntime() {
        return runtimeSingletonResource.get();
    }

    /**
     * Executes task to run failure and healing detection every poll interval. (Default: 1 sec)
     */
    @Override
    public void start(Duration monitoringInterval) {
        // Trigger sequencer bootstrap on startup.
        getCorfuRuntime().getLayoutManagementView()
                .asyncSequencerBootstrap(serverContext.copyManagementLayout(), detectionTaskWorkers);

        detectionTasksScheduler.scheduleAtFixedRate(() -> runSansThrow(this::runDetectionTasks),
                0, monitoringInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Checks if this management client is allowed to handle reconfigurations.
     * - This client is not authorized to trigger reconfigurations if this node is not a part
     * of the current layout.
     *
     * @return True if node is allowed to handle reconfigurations. False otherwise.
     */
    private boolean canHandleReconfigurations() {

        // We check for the following condition here: If the node is NOT a part of the
        // current layout, it should not attempt to change layout.
        Layout layout = serverContext.getManagementLayout();
        if (!layout.getAllServers().contains(serverContext.getLocalEndpoint())) {
            log.debug("This Server is not a part of the active layout. Aborting reconfiguration handling.");
            return false;
        }
        return true;
    }


    /**
     * Schedules the detection tasks run by detectorTaskScheduler.
     * It schedules exactly one instance of the following tasks.
     * - Failure detection tasks.
     * - Healing detection tasks.
     */
    private synchronized void runDetectionTasks() {

        CorfuRuntime corfuRuntime = getCorfuRuntime();
        getCorfuRuntime().invalidateLayout();
        serverContext.saveManagementLayout(corfuRuntime.getLayoutView().getLayout());

        if (!canHandleReconfigurations()) {
            return;
        }

        clusterStateContext.refreshClusterView(serverContext.getLocalEndpoint(), serverContext.copyManagementLayout());

        runFailureDetectorTask();
        runHealingDetectorTask();
    }


    /**
     * This contains the healing mechanism.
     * - This task is executed in intervals of 1 second (default). This task is blocked until
     * the management server is bootstrapped and has a connected runtime.
     * - On every invocation, this task refreshes the runtime to fetch the latest layout and also
     * updates the local persisted copy of the latest layout
     * - It then executes the poll using the healingDetector which generates a pollReport at the
     * end of the round.
     * - The poll report contains servers that are now responsive and healed.
     * - The healed servers in the pollReport are then handled based on the healing handler policy.
     * - The healing handler policy dictates whether the healing node should be added back as a
     * logUnit node or should only operate as a layout server or a primary/backup sequencer.
     */
    private void runHealingDetectorTask() {

        if (!healingDetectorFuture.isDone()) {
            log.debug("Cannot initiate new healing polling task. Polling in progress.");
            return;
        }

        healingDetectorFuture = detectionTaskWorkers.submit(() -> {

            CorfuRuntime corfuRuntime = getCorfuRuntime();
            final Layout layout = serverContext.copyManagementLayout();
            final PollReport pollReport = healingDetector.poll(layout, corfuRuntime);

            clusterStateContext.updateResponsiveNodes(pollReport.getChangedNodes());
            pollReport.getClusterStateMap().forEach((s, clusterStatus) ->
                    clusterStateContext.updateNodeState(clusterStatus, layout));

            Set<String> healedNodes = new TreeSet<>(recommendationEngine
                    .healedServers(clusterStateContext.getClusterState(), layout));

            if (healedNodes.isEmpty()) {
                return;
            }

            try {
                log.info("[{}] : Attempting to heal nodes in poll report: {}",
                        serverContext.getLocalEndpoint(), pollReport);

                corfuRuntime.getLayoutView().getRuntimeLayout(layout)
                        .getManagementClient(serverContext.getLocalEndpoint())
                        .handleHealing(pollReport.getPollEpoch(), healedNodes)
                        .get();
                log.info("Healing nodes successful: {}", pollReport);
            } catch (ExecutionException ee) {
                log.error("Healing nodes failed: ", ee);
            } catch (InterruptedException ie) {
                log.error("Healing nodes interrupted: ", ie);
                throw new UnrecoverableCorfuInterruptedError(ie);
            }
        });
    }

    /**
     * This contains the failure detection and handling mechanism.
     * - This task is executed in intervals of 1 second (default). This task is blocked until
     * the management server is bootstrapped and has a connected runtime.
     * - On every invocation, this task refreshes the runtime to fetch the latest layout and also
     * updates the local persisted copy of the latest layout
     * - It then executes the poll using the failureDetector which generates a pollReport at the
     * end of the round.
     * - The poll report contains servers that are unresponsive and servers whose epochs do not
     * match the current cluster epoch
     * - The outOfPhase epoch server errors are corrected by resealing and patching these trailing
     * layout servers if needed.
     * - Finally all unresponsive server failures are handled by either removing or marking them
     * as unresponsive based on a failure handling policy.
     */
    private void runFailureDetectorTask() {

        if (!failureDetectorFuture.isDone()) {
            log.debug("Cannot initiate new polling task. Polling in progress.");
            return;
        }

        failureDetectorFuture = detectionTaskWorkers.submit(() -> {

            CorfuRuntime corfuRuntime = getCorfuRuntime();
            final Layout layout = serverContext.copyManagementLayout();
            // Execute the failure detection poll round.
            final PollReport pollReport = failureDetector.poll(layout, corfuRuntime);

            clusterStateContext.updateUnresponsiveNodes(pollReport.getChangedNodes());
            pollReport.getClusterStateMap().forEach((s, clusterStatus) ->
                    clusterStateContext.updateNodeState(clusterStatus, layout));

            // Corrects out of phase epoch issues if present in the report. This method
            // performs re-sealing of all nodes if required and catchup of a layout server to
            // the current state.
            correctOutOfPhaseEpochs(pollReport);

            // Analyze the poll report and trigger failure handler if needed.
            handleFailures(pollReport);

        });
    }

    /**
     * All active Layout servers have been sealed but there is no client to take this forward and
     * fill the slot by proposing a new layout. This is determined by the outOfPhaseEpochNodes map.
     * This map contains a map of nodes and their server router epochs iff that server responded
     * with a WrongEpochException to the heartbeat message.
     * In this case we can pass an empty set to propose the same layout again and fill the layout
     * slot to un-block the data plane operations.
     *
     * @param pollReport Report from the polling task
     * @return True if latest layout slot is vacant. Else False.
     */
    private boolean isCurrentLayoutSlotUnFilled(PollReport pollReport) {
        final Layout layout = serverContext.copyManagementLayout();
        // Check if all active layout servers are present in the outOfPhaseEpochNodes map.
        boolean result = pollReport.getOutOfPhaseEpochNodes().keySet()
                .containsAll(layout.getLayoutServers().stream()
                        // Unresponsive servers are excluded as they do not respond with a
                        // WrongEpochException.
                        .filter(s -> !layout.getUnresponsiveServers().contains(s))
                        .collect(Collectors.toList()));
        if (result) {
            log.info("Current layout slot is empty. Filling slot with current layout.");
        }
        return result;
    }

    private SequencerStatus getPrimarySequencerStatus(Layout layout, PollReport pollReport) {
        String primarySequencer = layout.getSequencers().get(0);
        // Fetches clusterStatus from map or creates a default ClusterState object.
        NodeState primarySequencerNodeState = clusterStateContext.getClusterState()
                .getNodeStatusMap().getOrDefault(primarySequencer,
                        NodeState.getDefaultNodeState(NodeLocator.parseString(primarySequencer)));

        // If we have a stale poll report, we should discard this and continue polling.
        if (layout.getEpoch() > pollReport.getPollEpoch()) {
            log.warn("getPrimarySequencerStatus: Received poll report for epoch {} but currently "
                    + "at epoch {}", pollReport.getPollEpoch(), layout.getEpoch());
            return SequencerStatus.UNKNOWN;
        } else {
            return primarySequencerNodeState.getSequencerMetrics().getSequencerStatus();
        }
    }

    /**
     * Analyzes the poll report and triggers the failure handler if status change
     * of node detected.
     *
     * @param pollReport Poll report obtained from failure detection policy.
     */
    private void handleFailures(PollReport pollReport) {
        try {
            // These conditions are mutually exclusive. If there is a failure to be
            // handled, we don't need to explicitly fix the unfilled layout slot. Else we do.
            Layout layout = serverContext.copyManagementLayout();
            Set<String> failedNodes = new TreeSet<>(recommendationEngine
                    .failedServers(clusterStateContext.getClusterState(), layout));

            if (!failedNodes.isEmpty() || isCurrentLayoutSlotUnFilled(pollReport)) {

                log.info("Detected failed nodes in node responsiveness: Failed:{}, pollReport:{}",
                        failedNodes, pollReport);
                getCorfuRuntime().getLayoutView().getRuntimeLayout(layout)
                        .getManagementClient(serverContext.getLocalEndpoint())
                        .handleFailure(pollReport.getPollEpoch(), failedNodes).get();

            } else if (!getPrimarySequencerStatus(layout, pollReport)
                    .equals(SequencerStatus.READY)) {
                // If failures are not present we can check if the primary sequencer has been
                // bootstrapped from the heartbeat responses received.
                if (sequencerNotReadyCounter == null
                        || sequencerNotReadyCounter.getEpoch() != layout.getEpoch()) {
                    // If the epoch is different than the poll epoch, we reset the timeout state.
                    sequencerNotReadyCounter = new SequencerNotReadyCounter(layout.getEpoch(), 1);

                } else if (sequencerNotReadyCounter.getEpoch() == layout.getEpoch()) {
                    // If the epoch is same as the epoch being tracked in the tuple, we need to
                    // increment the count and attempt to bootstrap the sequencer if the count has
                    // crossed the threshold.
                    sequencerNotReadyCounter.setCounter(sequencerNotReadyCounter.getCounter() + 1);
                    if (sequencerNotReadyCounter.getCounter() >= SEQUENCER_NOT_READY_THRESHOLD) {
                        // Launch task to bootstrap the primary sequencer.
                        log.info("Attempting to bootstrap the primary sequencer.");
                        // We do not care about the result of the trigger.
                        // If it fails, we detect this again and retry in the next polling cycle.
                        getCorfuRuntime().getLayoutManagementView()
                                .asyncSequencerBootstrap(layout, detectionTaskWorkers);
                    }
                }
            }

        } catch (Exception e) {
            log.error("Exception invoking failure handler : {}", e);
        }
    }

    /**
     * Get the layout from a particular layout server requested by a Layout request message stamped
     * with the epoch from the specified layout.
     *
     * @param layout   Layout epoch to stamp the layout request.
     * @param endpoint Layout Server endpoint to request the layout from.
     * @return Completable future which returns the result of the RPC request.
     */
    private CompletableFuture<Layout> getLayoutFromServer(Layout layout, String endpoint) {
        CompletableFuture<Layout> completableFuture = new CompletableFuture<>();
        try {
            completableFuture = getCorfuRuntime().getLayoutView().getRuntimeLayout(layout)
                    .getLayoutClient(endpoint)
                    .getLayout();
        } catch (Exception e) {
            completableFuture.completeExceptionally(e);
        }
        return completableFuture;
    }

    /**
     * Corrects out of phase epochs by resealing the servers.
     * This would also need to update trailing layout servers.
     *
     * @param pollReport Poll Report from running the failure detection policy.
     */
    private void correctOutOfPhaseEpochs(PollReport pollReport) {

        final Map<String, Long> outOfPhaseEpochNodes = pollReport.getOutOfPhaseEpochNodes();
        if (outOfPhaseEpochNodes.isEmpty()) {
            return;
        }

        try {
            Layout layout = serverContext.copyManagementLayout();
            // Query all layout servers to get quorum Layout.
            Map<String, CompletableFuture<Layout>> layoutCompletableFutureMap = new HashMap<>();
            for (String layoutServer : layout.getLayoutServers()) {
                layoutCompletableFutureMap.put(layoutServer, getLayoutFromServer(layout, layoutServer));
            }

            // Retrieve the correct layout from quorum of members to reseal servers.
            // If we are unable to reach a consensus from a quorum we get an exception and
            // abort the epoch correction phase.
            Layout quorumLayout = fetchQuorumLayout(layoutCompletableFutureMap.values()
                    .toArray(new CompletableFuture[layoutCompletableFutureMap.size()]));

            // Update local layout copy.
            serverContext.saveManagementLayout(quorumLayout);
            layout = serverContext.copyManagementLayout();

            // In case of a partial seal, a set of servers can be sealed with a higher epoch.
            // We should be able to detect this and bring the rest of the servers to this epoch.
            long maxOutOfPhaseEpoch = Collections.max(outOfPhaseEpochNodes.values());
            if (maxOutOfPhaseEpoch > layout.getEpoch()) {
                layout.setEpoch(maxOutOfPhaseEpoch);
            }

            // Re-seal all servers with the latestLayout epoch.
            // This has no effect on up-to-date servers. Only the trailing servers are caught up.
            getCorfuRuntime().getLayoutView().getRuntimeLayout(layout).sealMinServerSet();

            // Check if any layout server has a stale layout.
            // If yes patch it (commit) with the latestLayout (received from quorum).
            updateTrailingLayoutServers(layoutCompletableFutureMap);

        } catch (QuorumUnreachableException e) {
            log.error("Error in correcting server epochs: {}", e);
        }
    }

    /**
     * Fetches the updated layout from quorum of layout servers.
     *
     * @return quorum agreed layout.
     * @throws QuorumUnreachableException If unable to receive consensus on layout.
     */
    private Layout fetchQuorumLayout(CompletableFuture<Layout>[] completableFutures) {

        QuorumFuturesFactory.CompositeFuture<Layout> quorumFuture = QuorumFuturesFactory
                .getQuorumFuture(
                        Comparator.comparing(Layout::asJSONString),
                        completableFutures);
        try {
            return quorumFuture.get();
        } catch (ExecutionException ee) {
            if (ee.getCause() instanceof QuorumUnreachableException) {
                throw (QuorumUnreachableException) ee.getCause();
            }

            int reachableServers = (int) Arrays.stream(completableFutures)
                    .filter(booleanCompletableFuture -> !booleanCompletableFuture
                            .isCompletedExceptionally()).count();
            throw new QuorumUnreachableException(reachableServers, completableFutures.length);
        } catch (InterruptedException ie) {
            log.error("fetchQuorumLayout: Interrupted Exception.");
            throw new UnrecoverableCorfuInterruptedError(ie);
        }
    }

    /**
     * Finds all trailing layout servers and patches them with the latest persisted layout
     * retrieved by quorum.
     *
     * @param layoutCompletableFutureMap Map of layout server endpoints to their layout requests.
     */
    private void updateTrailingLayoutServers(
            Map<String, CompletableFuture<Layout>> layoutCompletableFutureMap) {

        // Patch trailing layout servers with latestLayout.
        Layout latestLayout = serverContext.copyManagementLayout();
        layoutCompletableFutureMap.keySet().forEach(layoutServer -> {
            Layout layout = null;
            try {
                layout = layoutCompletableFutureMap.get(layoutServer).get();
            } catch (ExecutionException ee) {
                // Expected wrong epoch exception if layout server fell behind and has stale
                // layout and server epoch.
                log.warn("updateTrailingLayoutServers: layout fetch from {} failed: {}",
                        layoutServer, ee);
            } catch (InterruptedException ie) {
                log.error("updateTrailingLayoutServers: layout fetch from {} failed: {}",
                        layoutServer, ie);
                throw new UnrecoverableCorfuInterruptedError(ie);
            }

            // Do nothing if this layout server is updated with the latestLayout.
            if (layout != null && layout.equals(latestLayout)) {
                return;
            }
            try {
                // Committing this layout directly to the trailing layout servers.
                // This is safe because this layout is acquired by a quorum fetch which confirms
                // that there was a consensus on this layout and has been committed to a quorum.
                boolean result = getCorfuRuntime().getLayoutView().getRuntimeLayout(latestLayout)
                        .getLayoutClient(layoutServer)
                        .committed(latestLayout.getEpoch(), latestLayout).get();
                if (result) {
                    log.debug("Layout Server: {} successfully patched with latest layout : {}",
                            layoutServer, latestLayout);
                } else {
                    log.debug("Layout Server: {} patch with latest layout failed : {}",
                            layoutServer, latestLayout);
                }
            } catch (ExecutionException ee) {
                log.error("Updating layout servers failed due to : {}", ee);
            } catch (InterruptedException ie) {
                log.error("Updating layout servers failed due to : {}", ie);
                throw new UnrecoverableCorfuInterruptedError(ie);
            }
        });
    }

    @Override
    public void shutdown() {
        // Shutting the fault detector.
        detectionTasksScheduler.shutdownNow();
        detectionTaskWorkers.shutdownNow();
        log.info("Fault Detection MonitoringService shutting down.");
    }
}
