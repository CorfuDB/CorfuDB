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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.management.IDetector;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.infrastructure.management.ReconfigurationEventHandler;
import org.corfudb.protocols.wireprotocol.NetworkMetrics;
import org.corfudb.protocols.wireprotocol.NodeView;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.SequencerMetrics.SequencerStatus;
import org.corfudb.protocols.wireprotocol.ServerMetrics;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.ServerNotReadyException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.QuorumFuturesFactory;
import org.corfudb.util.CFUtils;
import org.corfudb.util.NodeLocator;
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
    private final Thread initializationTaskThread;
    /**
     * Detection Task Scheduler Service
     * This service schedules the following tasks every policyExecuteInterval (1 sec):
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
     * Future which is reset every time a new task to bootstrap the sequencer is launched by the
     * ManagementAgent. This is to avoid multiple bootstrap requests.
     * Using AtomicReference here to avoid multiple sequencer recovery tasks being triggered.
     */
    private final AtomicReference<Future<Boolean>> sequencerRecoveryFuture
            = new AtomicReference<>(CompletableFuture.completedFuture(true));

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

    /**
     * Cluster metrics collection.
     */
    //  Service to poll local node metrics.
    private final ScheduledExecutorService localMetricsPollingService;
    //  Locally collected server metrics polling interval.
    private static final Duration METRICS_POLL_INTERVAL = Duration.ofMillis(3000);
    //  Local copy of the local node's server metrics.
    @Getter(AccessLevel.PROTECTED)
    private volatile ServerMetrics localServerMetrics;
    //  A view of peers' connectivity.
    // Connectivity of any unresponsive nodes responding. Updated by HealingDetector.
    private Set<String> responsiveNodesPeerView = Collections.emptySet();
    // Connectivity of any responsive nodes not responding. Updated by FailureDetector.
    private Set<String> unresponsiveNodesPeerView = Collections.emptySet();

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

        localServerMetrics = new ServerMetrics(NodeLocator.parseString(getLocalEndpoint()),
                new SequencerMetrics(SequencerStatus.UNKNOWN));

        Layout managementLayout = serverContext.copyManagementLayout();
        // If no state was preserved, there is no layout to recover.
        if (managementLayout == null) {
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
        serverContext.saveManagementLayout(managementLayout);

        if (!recovered) {
            log.info("Attempting to recover. Layout before shutdown: {}", managementLayout);
        }

        this.failureDetector = serverContext.getFailureDetector();
        this.healingDetector = serverContext.getHealingDetector();
        this.reconfigurationEventHandler = new ReconfigurationEventHandler();

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

        this.localMetricsPollingService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(serverContext.getThreadPrefix() + "LocalMetricsPolling")
                        .build());

        // Creating the initialization task thread.
        // This thread pool is utilized to dispatch one time recovery and sequencer bootstrap tasks.
        // One these tasks finish successfully, they initiate the detection tasks.
        this.initializationTaskThread = new Thread(this::initializationTask, "initializationTaskThread");
        this.initializationTaskThread.setUncaughtExceptionHandler(
                (thread, throwable) -> {
                    log.error("Error in initialization task: {}", throwable);
                    shutdown();
                });
        this.initializationTaskThread.start();
    }

    /**
     * Triggers a new task to bootstrap the sequencer for the specified layout. If there is already
     * a task in progress, this is a no-op.
     *
     * @param layout Layout to use to bootstrap the primary sequencer.
     * @return Future which completes when the task completes successfully or with a failure.
     */
    public Future<Boolean> triggerSequencerBootstrap(@NonNull Layout layout) {
        return sequencerRecoveryFuture.updateAndGet(sequencerRecovery -> {
            if (sequencerRecovery.isDone()) {
                return detectionTaskWorkers.submit(() -> {
                    log.info("triggerSequencerBootstrap: a bootstrap task is triggered.");
                    try {
                        getCorfuRuntime().getLayoutManagementView()
                                .reconfigureSequencerServers(layout, layout, true);
                    } catch (Exception e) {
                        log.error("triggerSequencerBootstrap: Failed with Exception: ", e);
                    }
                    return true;
                });
            }

            log.info("triggerSequencerBootstrap: a bootstrap task is already in progress.");
            return sequencerRecovery;
        });
    }

    /**
     * Initialization task.
     * Performs recovery if required.
     * Bootstraps the primary sequencer if the server has been bootstrapped.
     * Initiates the failure detection and healing detection tasks.
     */
    private void initializationTask() {

        try {
            while (!shutdown && serverContext.getManagementLayout() == null
                    && bootstrapEndpoint == null) {
                log.warn("Management Server waiting to be bootstrapped");
                Sleep.MILLISECONDS.sleepRecoverably(policyExecuteInterval);
            }

            // Recover if flag is false
            while (!recovered) {
                recovered = runRecoveryReconfiguration();
                if (!recovered) {
                    log.error("detectorTaskScheduler: Recovery failed. Retrying.");
                    continue;
                }
                // If recovery succeeds, reconfiguration was successful.
                // Save the latest management layout.
                serverContext.saveManagementLayout(getCorfuRuntime().getLayoutView().getLayout());

                log.info("Recovery completed");
            }

            // Trigger sequencer bootstrap if in recovery mode or fresh startup
            triggerSequencerBootstrap(serverContext.copyManagementLayout());

            // Initiating periodic task to poll for failures.
            localMetricsPollingService.scheduleAtFixedRate(
                    () -> runSansThrow(this::updateLocalMetrics),
                    0,
                    METRICS_POLL_INTERVAL.toMillis(),
                    TimeUnit.MILLISECONDS);

            detectionTasksScheduler.scheduleAtFixedRate(
                    () -> runSansThrow(this::detectorTaskScheduler),
                    0,
                    policyExecuteInterval,
                    TimeUnit.MILLISECONDS);

        } catch (InterruptedException e) {
            log.error("initializationTask: InitializationTask interrupted.");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("initializationTask: Error in initializationTask.", e);
            throw new UnrecoverableCorfuError(e);
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
        Layout layout = serverContext.copyManagementLayout();
        if (layout == null) {
            log.error("Management layout is null. Cannot recover.");
            return false;
        }
        Layout localRecoveryLayout = new Layout(layout);
        boolean recoveryReconfigurationResult = reconfigurationEventHandler
                .recoverCluster(layout, getCorfuRuntime());
        log.info("Recovery reconfiguration attempt result: {}", recoveryReconfigurationResult);

        getCorfuRuntime().invalidateLayout();
        Layout clusterLayout = getCorfuRuntime().getLayoutView().getLayout();

        log.info("Recovery layout epoch:{}, Cluster epoch: {}",
                localRecoveryLayout.getEpoch(), clusterLayout.getEpoch());
        // The cluster has moved ahead. This node should not force any layout. Let the other
        // members detect that this node has healed and include it in the layout.
        return clusterLayout.getEpoch() > localRecoveryLayout.getEpoch()
                || recoveryReconfigurationResult;
    }

    /**
     * Queries the local Sequencer Server for SequencerMetrics. This returns UNKNOWN if unable
     * to fetch the status.
     *
     * @param layout Current Management layout.
     * @return Sequencer Metrics.
     */
    private SequencerMetrics queryLocalSequencerMetrics(Layout layout) {
        SequencerMetrics sequencerMetrics;
        // This is an optimization. If this node is not the primary sequencer for the current
        // layout, there is no reason to request metrics from this sequencer.
        if (layout.getSequencers().get(0).equals(getLocalEndpoint())) {
            try {
                sequencerMetrics = CFUtils.getUninterruptibly(
                        getCorfuRuntime()
                                .getLayoutView().getRuntimeLayout(layout)
                                .getSequencerClient(getLocalEndpoint())
                                .requestMetrics());
            } catch (ServerNotReadyException snre) {
                sequencerMetrics = new SequencerMetrics(SequencerStatus.NOT_READY);
            } catch (Exception e) {
                log.error("Error while requesting metrics from the sequencer: ", e);
                sequencerMetrics = new SequencerMetrics(SequencerStatus.UNKNOWN);
            }
        } else {
            sequencerMetrics = new SequencerMetrics(SequencerStatus.UNKNOWN);
        }
        return sequencerMetrics;
    }

    /**
     * Task to collect local node metrics.
     * This task collects the following:
     * Boolean Status of layout, sequencer and logunit servers.
     * These metrics are then composed into ServerMetrics model and stored locally.
     */
    private void updateLocalMetrics() {

        // Initializing the status of components
        // No need to poll unless node is bootstrapped.
        Layout layout = serverContext.copyManagementLayout();
        if (layout == null) {
            return;
        }

        // Build and replace existing locally stored nodeMetrics
        localServerMetrics = new ServerMetrics(NodeLocator.parseString(getLocalEndpoint()),
                queryLocalSequencerMetrics(layout));
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
     * of the current layout.
     *
     * @return True if node is allowed to handle reconfigurations. False otherwise.
     */
    private boolean canHandleReconfigurations() {

        // We check for the following condition here: If the node is NOT a part of the
        // current layout, it should not attempt to change layout.
        Layout layout = serverContext.getManagementLayout();
        if (!layout.getAllServers().contains(getLocalEndpoint())) {
            log.debug("This Server is not a part of the active layout. "
                    + "Aborting reconfiguration handling.");
            return false;
        }

        return true;
    }

    /**
     * Combines the peer connectivity view from the failure and the healing detectors.
     * This map containing the view of the detectors is used to create the NodeView.
     * This NodeView is sent in the HeartbeatResponse message.
     *
     * @return Peer connectivity view map
     */
    NetworkMetrics getConnectivityView() {
        Map<String, Boolean> peerConnectivityDeltaMap = new HashMap<>();
        responsiveNodesPeerView.forEach(s -> peerConnectivityDeltaMap.put(s, true));
        unresponsiveNodesPeerView.forEach(s -> peerConnectivityDeltaMap.put(s, false));
        // If the management server is not bootstrapped, stamp with INVALID_EPOCH.
        long peerConnectivitySnapshotEpoch = serverContext.getManagementLayout() != null
                ? serverContext.getManagementLayout().getEpoch() : Layout.INVALID_EPOCH;
        return new NetworkMetrics(peerConnectivitySnapshotEpoch, peerConnectivityDeltaMap);
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
                Layout layout = serverContext.copyManagementLayout();
                PollReport pollReport = healingDetector.poll(layout, corfuRuntime);

                responsiveNodesPeerView = pollReport.getHealingNodes();

                if (!pollReport.getHealingNodes().isEmpty()) {
                    try {
                        log.info("Attempting to heal nodes in poll report: {}", pollReport);
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
                PollReport pollReport = failureDetector.poll(serverContext.copyManagementLayout(),
                        corfuRuntime);

                unresponsiveNodesPeerView = pollReport.getFailingNodes();

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
                new HashSet<>(serverContext.copyManagementLayout().getUnresponsiveServers()));
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
        // Fetches nodeView from map or creates a default NodeView object.
        NodeView nodeView = pollReport.getNodeViewMap().getOrDefault(primarySequencer,
                NodeView.getDefaultNodeView(NodeLocator.parseString(primarySequencer)));
        // If we have a stale poll report, we should discard this and continue polling.
        if (layout.getEpoch() > pollReport.getPollEpoch()) {
            log.warn("getPrimarySequencerStatus: Received poll report for epoch {} but currently "
                    + "at epoch {}", pollReport.getPollEpoch(), layout.getEpoch());
            return SequencerStatus.UNKNOWN;
        } else {
            return nodeView.getServerMetrics().getSequencerMetrics().getSequencerStatus();
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
            Set<String> failedNodes = new HashSet<>(getNewFailures(pollReport));

            // These conditions are mutually exclusive. If there is a failure to be
            // handled, we don't need to explicitly fix the unfilled layout slot. Else we do.
            Layout layout = serverContext.copyManagementLayout();
            if (!failedNodes.isEmpty() || isCurrentLayoutSlotUnFilled(pollReport)) {

                log.info("Detected changes in node responsiveness: Failed:{}, pollReport:{}",
                        failedNodes, pollReport);
                getCorfuRuntime().getLayoutView().getRuntimeLayout(layout)
                        .getManagementClient(getLocalEndpoint())
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
                        triggerSequencerBootstrap(layout);
                    }
                }
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
            Layout layout = serverContext.copyManagementLayout();
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
            layout = serverContext.copyManagementLayout();

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
        Layout latestLayout = serverContext.copyManagementLayout();
        layoutCompletableFutureMap.keySet().forEach(layoutServer -> {
            Layout layout = null;
            try {
                layout = layoutCompletableFutureMap.get(layoutServer).get();
            } catch (InterruptedException | ExecutionException e) {
                // Expected wrong epoch exception if layout server fell behind and has stale
                // layout and server epoch.
                log.warn("updateTrailingLayoutServers: layout fetch from {} failed: {}",
                        layoutServer, e);
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
        localMetricsPollingService.shutdownNow();

        try {
            initializationTaskThread.interrupt();
            initializationTaskThread.join(ServerContext.SHUTDOWN_TIMER.toMillis());
        } catch (InterruptedException ie) {
            log.error("initializationTask interrupted : {}", ie);
        }
        try {
            detectionTasksScheduler.awaitTermination(ServerContext.SHUTDOWN_TIMER.getSeconds(),
                    TimeUnit.SECONDS);
            detectionTaskWorkers.awaitTermination(ServerContext.SHUTDOWN_TIMER.getSeconds(),
                    TimeUnit.SECONDS);
            localMetricsPollingService.awaitTermination(ServerContext.SHUTDOWN_TIMER.getSeconds(),
                    TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            log.debug("detectionTaskWorkers awaitTermination interrupted : {}", ie);
            Thread.currentThread().interrupt();
        }
        log.info("Management Agent shutting down.");
    }

}
