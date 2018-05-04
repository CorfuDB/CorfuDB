package org.corfudb.infrastructure;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.management.IDetector;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.infrastructure.management.ReconfigurationEventHandler;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.QuorumFuturesFactory;
import org.corfudb.util.Sleep;
import org.corfudb.util.concurrent.SingletonResource;

/**
 * Instantiates and performs failure detection and handling asynchronously.
 *
 * <p>Failure Detector:
 * Executes detection policy to detect failed and healed nodes.
 * It then checks for status of the nodes. If there are failed or healed nodes to be addressed,
 * this then triggers the respective handler which then responds to these reconfiguration changes
 * based on a policy.
 *
 * <p>Created by zlokhandwala on 1/15/18.
 */
@Slf4j
public class ManagementAgent {

    private final ServerContext serverContext;
    private final Map<String, Object> opts;

    /**
     * Detectors to be used to detect failures and healing.
     */
    @Getter
    private IDetector failureDetector;
    @Getter
    private IDetector healingDetector;
    /**
     * Failure Handler Dispatcher to launch configuration changes or recovery.
     */
    @Getter
    private final ReconfigurationEventHandler reconfigurationEventHandler;
    /**
     * Interval in executing the failure detection policy.
     * In milliseconds.
     */
    @Getter
    private final long policyExecuteInterval = 1000;
    /**
     * To dispatch initialization tasks for recovery and sequencer bootstrap.
     */
    @Getter
    private Thread initializationTaskThread;
    /**
     * Detection Task Scheduler Service
     * This service schedules the following tasks every policyExecuteInterval (1 sec):
     * - Detection of failed nodes.
     * - Detection of healed nodes.
     */
    @Getter
    private ScheduledExecutorService detectionTasksScheduler;
    /**
     * To dispatch tasks for failure or healed nodes detection.
     */
    @Getter
    private ExecutorService detectionTaskWorkers;
    /**
     * Future for periodic failure and healed nodes detection task.
     */
    private Future failureDetectorFuture = null;
    private Future healingDetectorFuture = null;
    private boolean recovered = false;

    private final SingletonResource<CorfuRuntime> runtimeSingletonResource;
    private final String bootstrapEndpoint;

    private volatile boolean shutdown = false;

    /**
     * Future which is marked completed if:
     * If Recovery, recovered successfully
     * Else, after bootstrapping the primary sequencer successfully.
     */
    @Getter
    private volatile CompletableFuture<Boolean> sequencerBootstrappedFuture;

    /**
     * Checks and restores if a layout is present in the local datastore to recover from.
     * Spawns the initialization task which recovers if required, bootstraps sequencer and
     * schedules detector tasks.
     *
     * @param runtimeSingletonResource Singleton resource to fetch runtime.
     * @param serverContext            Server Context.
     */
    ManagementAgent(SingletonResource<CorfuRuntime> runtimeSingletonResource,
                    ServerContext serverContext) {
        this.runtimeSingletonResource = runtimeSingletonResource;
        this.serverContext = serverContext;
        this.opts = serverContext.getServerConfig();

        bootstrapEndpoint = (opts.get("--management-server") != null)
                ? opts.get("--management-server").toString() : null;

        sequencerBootstrappedFuture = new CompletableFuture<>();

        // If no state was preserved, there is no layout to recover.
        if (serverContext.getManagementLayout() == null) {
            recovered = true;
        }

        // The management server needs to check both the Layout Server's persisted layout as well
        // as the Management Server's previously persisted layout. We try to recover from both of
        // these as the more recent layout (with higher epoch is retained).
        // When a node does not contain a layout server component and is trying to recover, we
        // would completely rely on recovering from the management server's persisted layout.
        // Else in every other case, the layout server is active and will contain the latest layout
        // (In case of trailing layout server, the management server's persisted layout helps.)
        serverContext.installSingleNodeLayoutIfAbsent();
        serverContext.saveManagementLayout(serverContext.getCurrentLayout());
        serverContext.saveManagementLayout(serverContext.getManagementLayout());

        if (!recovered) {
            log.info("Attempting to recover. Layout before shutdown: {}",
                    serverContext.getManagementLayout());
        }

        this.failureDetector = serverContext.getFailureDetector();
        this.healingDetector = serverContext.getHealingDetector();
        this.reconfigurationEventHandler = new ReconfigurationEventHandler();

        final int managementServiceCount = 1;
        final int detectionWorkersCount = 2;

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

        // Creating the initialization task thread.
        // This thread pool is utilized to dispatch one time recovery and sequencer bootstrap tasks.
        // One these tasks finish successfully, they initiate the detection tasks.
        this.initializationTaskThread = new Thread(this::initializationTask);
        this.initializationTaskThread.setUncaughtExceptionHandler(
                (thread, throwable) -> log.error("Error in initialization task: {}", throwable));
        this.initializationTaskThread.start();
    }

    /**
     * Initialization task.
     * Performs recovery if required.
     * Bootstraps the primary sequencer if the server has been bootstrapped.
     * Initiates the failure detection and healing detection tasks.
     */
    private void initializationTask() {

        try {
            while (!shutdown) {
                if (serverContext.getManagementLayout() == null && bootstrapEndpoint == null) {
                    log.warn("Management Server waiting to be bootstrapped");
                    Sleep.MILLISECONDS.sleepRecoverably(policyExecuteInterval);
                    continue;
                }

                // Recover if flag is false
                while (!recovered) {
                    recovered = runRecoveryReconfiguration();
                    if (!recovered) {
                        log.error("detectorTaskScheduler: Recovery failed. Retrying.");
                        continue;
                    }
                    // If recovery succeeds, reconfiguration was successful.
                    sequencerBootstrappedFuture.complete(true);
                    log.info("Recovery completed");
                }
                break;
            }

            // Sequencer bootstrap required if this is fresh startup (not recovery).
            if (!sequencerBootstrappedFuture.isDone()) {
                bootstrapPrimarySequencerServer();
            }

            // Initiating periodic task to poll for failures.
            try {
                detectionTasksScheduler.scheduleAtFixedRate(
                        this::detectorTaskScheduler,
                        0,
                        policyExecuteInterval,
                        TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException err) {
                log.error("Error scheduling failure detection task, {}", err);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private String getLocalEndpoint() {
        return this.opts.get("--address") + ":" + this.opts.get("<port>");
    }

    /**
     * Returns a connected instance of the CorfuRuntime.
     *
     * @return A connected instance of runtime.
     */
    public CorfuRuntime getCorfuRuntime() {
        return runtimeSingletonResource.get();
    }

    /**
     * Bootstraps the primary sequencer on a fresh startup (not recovery).
     */
    private void bootstrapPrimarySequencerServer() {
        try {
            Layout layout = serverContext.getManagementLayout();
            boolean bootstrapResult = getCorfuRuntime().getLayoutView().getRuntimeLayout(layout)
                    .getPrimarySequencerClient()
                    .bootstrap(0L, Collections.emptyMap(), layout.getEpoch(), false)
                    .get();
            sequencerBootstrappedFuture.complete(bootstrapResult);
            // If false, the sequencer is already bootstrapped with a higher epoch.
            if (!bootstrapResult) {
                log.warn("Sequencer already bootstrapped.");
            } else {
                log.info("Bootstrapped sequencer server at epoch:{}", layout.getEpoch());
            }
        } catch (InterruptedException | ExecutionException e) {
            log.error("Bootstrapping sequencer failed: ", e);
        }
    }

    /**
     * This is called when the management server detects an existing layout in the local datastore
     * on startup. This requires a recovery from the same layout and attempt to rejoin the cluster.
     * Recovery is carried out as follows:
     * - Attempt to run reconfiguration on the cluster from the recovered layout found in the
     * local data store by incrementing the epoch.
     * The reconfiguration succeeds if the attempt to reach consensus by re-proposing this
     * recovery layout with its epoch incremented succeeds.
     * - If reconfiguration succeeds, the node is added to the layout and recovery was successful.
     * - If reconfiguration fails, the cluster has moved ahead.
     * * - This node now cannot force its inclusion into the cluster (it has a stale layout).
     * * - This node if marked unresponsive will be detected and unmarked by its peers in cluster.
     * - If multiple nodes are trying to recover, they will retry until they have recovered with
     * the latest layout previously accepted by the majority.
     * eg. Consider 3 nodes [Node(Epoch)]:
     * A(1), B(2), C(2). All 3 nodes crash and attempt to recover at the same time.
     * Node A should not be able to recover as it will detect a higher epoch in the rest of the
     * cluster. Hence either node B or C will succeed in recovering the cluster to epoch 3 with
     * their persisted layout.
     *
     * @return True if recovery was successful. False otherwise.
     */
    private boolean runRecoveryReconfiguration() {
        Layout layout = new Layout(serverContext.getManagementLayout());
        boolean recoveryReconfigurationResult = reconfigurationEventHandler
                .recoverCluster(layout, getCorfuRuntime());
        log.info("Recovery reconfiguration attempt result: {}", recoveryReconfigurationResult);

        getCorfuRuntime().invalidateLayout();
        Layout clusterLayout = getCorfuRuntime().getLayoutView().getLayout();

        log.info("Recovery layout epoch:{}, Cluster epoch: {}",
                serverContext.getManagementLayout().getEpoch(), clusterLayout.getEpoch());
        // The cluster has moved ahead. This node should not force any layout. Let the other
        // members detect that this node has healed and include it in the layout.
        boolean recoveryResult =
                clusterLayout.getEpoch() > serverContext.getManagementLayout().getEpoch()
                        || recoveryReconfigurationResult;

        if (recoveryResult) {
            sequencerBootstrappedFuture.complete(true);
        }

        return recoveryResult;
    }

    /**
     * Schedules the detection tasks run by detectorTaskScheduler.
     * It schedules exactly one instance of the following tasks.
     * - Failure detection tasks.
     * - Healing detection tasks.
     */
    private synchronized void detectorTaskScheduler() {

        CorfuRuntime corfuRuntime = getCorfuRuntime();
        getCorfuRuntime().invalidateLayout();
        serverContext.saveManagementLayout(corfuRuntime.getLayoutView().getLayout());

        if (!canHandleReconfigurations()) {
            return;
        }

        runFailureDetectorTask();

        runHealingDetectorTask();
    }

    /**
     * Checks if this management client is allowed to handle reconfigurations.
     * - This client is not authorized to trigger reconfigurations if this node is not a part
     * of the current layout OR it has been marked as unresponsive in the latest layout.
     * The node is marked responsive by any of the other nodes which can poll and sense this
     * node's recovery.
     *
     * @return True of node is allowed to handle reconfigurations. False otherwise.
     */
    private boolean canHandleReconfigurations() {

        // We check for 2 conditions here: If the node is NOT a part of the current layout
        // or has it been marked as unresponsive. If either is true, it should not
        // attempt to change layout.
        Layout layout = serverContext.getManagementLayout();
        if (!layout.getAllServers().contains(getLocalEndpoint())
                || layout.getUnresponsiveServers().contains(getLocalEndpoint())) {
            log.debug("This Server is not a part of the active layout. "
                    + "Aborting reconfiguration handling.");
            return false;
        }

        return true;
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

        if (healingDetectorFuture == null || healingDetectorFuture.isDone()) {
            healingDetectorFuture = detectionTaskWorkers.submit(() -> {

                CorfuRuntime corfuRuntime = getCorfuRuntime();
                PollReport pollReport =
                        healingDetector.poll(serverContext.getManagementLayout(), corfuRuntime);

                if (!pollReport.getHealingNodes().isEmpty()) {


                    try {
                        log.info("Attempting to heal nodes in poll report: {}", pollReport);
                        Layout layout = serverContext.getManagementLayout();
                        corfuRuntime.getLayoutView().getRuntimeLayout(layout)
                                .getManagementClient(getLocalEndpoint())
                                .handleHealing(pollReport.getPollEpoch(),
                                        pollReport.getHealingNodes())
                                .get();
                        log.info("Healing nodes successful: {}", pollReport);
                    } catch (InterruptedException | ExecutionException e) {
                        log.error("Healing nodes failed: ", e);
                    }
                }

            });
        } else {
            log.debug("Cannot initiate new healing polling task. Polling in progress.");
        }
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

        if (failureDetectorFuture == null || failureDetectorFuture.isDone()) {
            failureDetectorFuture = detectionTaskWorkers.submit(() -> {

                CorfuRuntime corfuRuntime = getCorfuRuntime();
                // Execute the failure detection poll round.
                PollReport pollReport =
                        failureDetector.poll(serverContext.getManagementLayout(), corfuRuntime);

                // Corrects out of phase epoch issues if present in the report. This method
                // performs re-sealing of all nodes if required and catchup of a layout server to
                // the current state.
                correctOutOfPhaseEpochs(pollReport);

                // Analyze the poll report and trigger failure handler if needed.
                handleFailures(pollReport);

            });
        } else {
            log.debug("Cannot initiate new polling task. Polling in progress.");
        }
    }

    /**
     * We check if these servers are the same set of servers which are marked as unresponsive in
     * the layout.
     * Check if this new detected failure has already been recognized.
     *
     * @param pollReport Report from the polling task
     * @return Set of nodes which have failed, relative to the latest local copy of the layout.
     */
    private Set<String> getNewFailures(PollReport pollReport) {
        return Sets.difference(
                pollReport.getFailingNodes(),
                new HashSet<>(serverContext.getManagementLayout().getUnresponsiveServers()));
    }

    /**
     * All Layout servers have been sealed but there is no client to take this forward and fill the
     * slot by proposing a new layout.
     * In this case we can pass an empty set to propose the same layout again and fill the layout
     * slot to un-block the data plane operations.
     *
     * @param pollReport Report from the polling task
     * @return True if latest layout slot is vacant. Else False.
     */
    private boolean isCurrentLayoutSlotUnFilled(PollReport pollReport) {
        boolean result = pollReport.getOutOfPhaseEpochNodes().keySet()
                .containsAll(serverContext.getManagementLayout().getLayoutServers());
        if (result) {
            log.info("Current layout slot is empty. Filling slot with current layout.");
        }
        return result;
    }

    /**
     * Analyzes the poll report and triggers the failure handler if status change
     * of node detected.
     *
     * @param pollReport Poll report obtained from failure detection policy.
     */
    private void handleFailures(PollReport pollReport) {
        try {
            Set<String> failedNodes = new HashSet<>(getNewFailures(pollReport));

            // These conditions are mutually exclusive. If there is a failure to be
            // handled, we don't need to explicitly fix the unfilled layout slot. Else we do.
            if (!failedNodes.isEmpty() || isCurrentLayoutSlotUnFilled(pollReport)) {

                log.info("Detected changes in node responsiveness: Failed:{}, pollReport:{}",
                        failedNodes, pollReport);
                Layout layout = serverContext.getManagementLayout();
                getCorfuRuntime().getLayoutView().getRuntimeLayout(layout)
                        .getManagementClient(getLocalEndpoint())
                        .handleFailure(pollReport.getPollEpoch(), failedNodes).get();
            }

        } catch (Exception e) {
            log.error("Exception invoking failure handler : {}", e);
        }
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
            Layout layout = serverContext.getManagementLayout();
            // Query all layout servers to get quorum Layout.
            Map<String, CompletableFuture<Layout>> layoutCompletableFutureMap = new HashMap<>();
            for (String layoutServer : layout.getLayoutServers()) {
                CompletableFuture<Layout> completableFuture = new CompletableFuture<>();
                try {
                    completableFuture = getCorfuRuntime().getLayoutView().getRuntimeLayout(layout)
                            .getLayoutClient(layoutServer)
                            .getLayout();
                } catch (Exception e) {
                    completableFuture.completeExceptionally(e);
                }
                layoutCompletableFutureMap.put(layoutServer, completableFuture);
            }

            // Retrieve the correct layout from quorum of members to reseal servers.
            // If we are unable to reach a consensus from a quorum we get an exception and
            // abort the epoch correction phase.
            Layout quorumLayout = fetchQuorumLayout(layoutCompletableFutureMap.values()
                    .toArray(new CompletableFuture[layoutCompletableFutureMap.size()]));

            // Update local layout copy.
            serverContext.saveManagementLayout(quorumLayout);
            layout = serverContext.getManagementLayout();

            // In case of a partial seal, a set of servers can be sealed with a higher epoch.
            // We should be able to detect this and bring the rest of the servers to this epoch.
            long maxOutOfPhaseEpoch = Collections.max(outOfPhaseEpochNodes.values());
            if (maxOutOfPhaseEpoch > layout.getEpoch()) {
                layout.setEpoch(maxOutOfPhaseEpoch);
            }

            // Re-seal all servers with the latestLayout epoch.
            // This has no effect on up-to-date servers. Only the trailing servers are caught up.
            getCorfuRuntime().getLayoutView().getRuntimeLayout(layout).moveServersToEpoch();

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
    private Layout fetchQuorumLayout(CompletableFuture<Layout>[] completableFutures)
            throws QuorumUnreachableException {

        QuorumFuturesFactory.CompositeFuture<Layout> quorumFuture = QuorumFuturesFactory
                .getQuorumFuture(
                        Comparator.comparing(Layout::asJSONString),
                        completableFutures);
        try {
            return quorumFuture.get();
        } catch (ExecutionException | InterruptedException e) {
            if (e.getCause() instanceof QuorumUnreachableException) {
                throw (QuorumUnreachableException) e.getCause();
            }

            int reachableServers = (int) Arrays.stream(completableFutures)
                    .filter(booleanCompletableFuture -> !booleanCompletableFuture
                            .isCompletedExceptionally()).count();
            throw new QuorumUnreachableException(reachableServers, completableFutures.length);
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
        Layout latestLayout = serverContext.getManagementLayout();
        layoutCompletableFutureMap.keySet().forEach(layoutServer -> {
            Layout layout = null;
            try {
                layout = layoutCompletableFutureMap.get(layoutServer).get();
            } catch (InterruptedException | ExecutionException e) {
                // Expected wrong epoch exception if layout server fell behind and has stale
                // layout and server epoch.
                log.warn("updateTrailingLayoutServers: layout fetch failed: {}", e);
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
            } catch (InterruptedException | ExecutionException e) {
                log.error("Updating layout servers failed due to : {}", e);
            }
        });
    }

    /**
     * Shutdown the detectorTaskScheduler, workers and initializationTaskThread.
     */
    public void shutdown() {
        // Shutting the fault detector.
        shutdown = true;
        detectionTasksScheduler.shutdownNow();
        detectionTaskWorkers.shutdownNow();

        try {
            initializationTaskThread.interrupt();
            initializationTaskThread.join(ServerContext.SHUTDOWN_TIMER.toMillis());
            detectionTasksScheduler.awaitTermination(ServerContext.SHUTDOWN_TIMER.getSeconds(),
                    TimeUnit.SECONDS);
            detectionTaskWorkers.awaitTermination(ServerContext.SHUTDOWN_TIMER.getSeconds(),
                    TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            log.debug("detectionTaskWorkers awaitTermination interrupted : {}", ie);
            Thread.currentThread().interrupt();
        }
        log.info("Management Agent shutting down.");
    }

}
