package org.corfudb.infrastructure;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.management.ClusterAdvisor;
import org.corfudb.infrastructure.management.ClusterAdvisorFactory;
import org.corfudb.infrastructure.management.ClusterStateContext;
import org.corfudb.infrastructure.management.ClusterType;
import org.corfudb.infrastructure.management.IDetector;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.infrastructure.management.failuredetector.ClusterGraph;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.SequencerMetrics.SequencerStatus;
import org.corfudb.protocols.wireprotocol.failuredetector.FailureDetectorMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.FailureDetectorMetrics.FailureDetectorAction;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.LambdaUtils;
import org.corfudb.util.concurrent.SingletonResource;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

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

    private static final CompletableFuture<DetectorTask> DETECTOR_TASK_COMPLETED
            = CompletableFuture.completedFuture(DetectorTask.COMPLETED);

    private static final CompletableFuture<DetectorTask> DETECTOR_TASK_NOT_COMPLETED
            = CompletableFuture.completedFuture(DetectorTask.NOT_COMPLETED);

    private static final CompletableFuture<DetectorTask> DETECTOR_TASK_SKIPPED
            = CompletableFuture.completedFuture(DetectorTask.SKIPPED);

    /**
     * Detectors to be used to detect failures and healing.
     */
    @Getter
    private final IDetector failureDetector;

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
    private final ExecutorService failureDetectorWorker;

    private final ServerContext serverContext;
    private final SingletonResource<CorfuRuntime> runtimeSingletonResource;
    private final ClusterStateContext clusterContext;
    private final LocalMonitoringService localMonitoringService;

    /**
     * Future for periodic failure and healed nodes detection task.
     */
    private CompletableFuture<DetectorTask> failureDetectorFuture = DETECTOR_TASK_NOT_COMPLETED;

    private final ClusterAdvisor advisor;

    /**
     * The management agent attempts to bootstrap a NOT_READY sequencer if the
     * sequencerNotReadyCounter counter exceeds this value.
     */
    private static final int SEQUENCER_NOT_READY_THRESHOLD = 3;

    /**
     * Failure detector counter. Keeps count of missing runs of failure detector
     * if the detector is busy from previous run
     */
    private final AtomicLong counter = new AtomicLong(1);

    /**
     * Number of workers for failure detector. Two workers used by dafult:
     * - failure/healing detection
     * - bootstrap sequencer
     */
    private final int detectionWorkersCount = 2;

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

        public void increment() {
            counter += 1;
        }
    }

    private volatile SequencerNotReadyCounter sequencerNotReadyCounter = new SequencerNotReadyCounter(0, 0);

    RemoteMonitoringService(@NonNull ServerContext serverContext,
                            @NonNull SingletonResource<CorfuRuntime> runtimeSingletonResource,
                            @NonNull ClusterStateContext clusterContext,
                            @NonNull IDetector failureDetector,
                            @NonNull LocalMonitoringService localMonitoringService) {
        this.serverContext = serverContext;
        this.runtimeSingletonResource = runtimeSingletonResource;
        this.clusterContext = clusterContext;
        this.failureDetector = failureDetector;
        this.localMonitoringService = localMonitoringService;
        this.advisor = ClusterAdvisorFactory.createForStrategy(
                ClusterType.COMPLETE_GRAPH,
                serverContext.getLocalEndpoint()
        );

        this.detectionTasksScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(serverContext.getThreadPrefix() + "ManagementService")
                        .build());

        // Creating the detection worker thread pool.
        // This thread pool is utilized to dispatch detection tasks at regular intervals in the
        // detectorTaskScheduler.
        this.failureDetectorWorker = Executors.newFixedThreadPool(
                detectionWorkersCount,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(serverContext.getThreadPrefix() + "DetectionWorker-%d")
                        .build()
        );
    }

    private CorfuRuntime getCorfuRuntime() {
        return runtimeSingletonResource.get();
    }

    /**
     * Executes task to run failure and healing detection every poll interval. (Default: 1 sec)
     * <p>
     * During the initialization step, the method:
     * - triggers sequencer bootstrap
     * - starts failure and healing detection mechanism, by running <code>runDetectionTasks</code>
     * every second (by default). Next iteration of detection task can be run only if current iteration is completed.
     */
    @Override
    public void start(Duration monitoringInterval) {
        // Trigger sequencer bootstrap on startup.
        sequencerBootstrap(serverContext);

        detectionTasksScheduler.scheduleAtFixedRate(
                () -> LambdaUtils.runSansThrow(this::runDetectionTasks),
                0,
                monitoringInterval.toMillis(),
                TimeUnit.MILLISECONDS
        );
    }

    /**
     * Trigger sequencer bootstrap mechanism. Get current layout management view and execute async bootstrap
     *
     * @param serverContext server context
     */
    private CompletableFuture<DetectorTask> sequencerBootstrap(ServerContext serverContext) {
        log.info("Trigger sequencer bootstrap on startup");
        return getCorfuRuntime()
                .getLayoutManagementView()
                .asyncSequencerBootstrap(serverContext.copyManagementLayout(), failureDetectorWorker)
                .thenApply(DetectorTask::fromBool);
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
     * Schedules the failure detection and handling mechanism by detectorTaskScheduler.
     * It schedules exactly one instance of the following tasks.
     * - Failure detection tasks.
     * - Healing detection tasks.
     *
     * <pre>
     * The algorithm:
     *  - wait until previous iteration finishes
     *  - On every invocation, this task refreshes the runtime to fetch the latest layout and also updates
     *  the local persisted copy of the latest layout.
     *  - get corfu metrics (Sequencer, LogUnit etc).
     *  - executes the poll using the failureDetector which generates a pollReport at the end of the round.
     *  The report contains latest information about cluster: connectivity graph, failed and healed nodes, wrong epochs.
     *  - refresh cluster state context by latest {@link ClusterState} collected on poll report step.
     *  - run failure detector task composed from:
     *     - the outOfPhase epoch server errors are corrected by resealing and patching these trailing layout servers.
     *     - all unresponsive server failures are handled by either removing or marking
     *     them as unresponsive based on a failure handling policy.
     *    - make healed node responsive based on a healing detection mechanism
     *  </pre>
     */
    private synchronized void runDetectionTasks() {

        CorfuRuntime corfuRuntime = getCorfuRuntime();
        getCorfuRuntime().invalidateLayout();

        Layout layout = corfuRuntime.getLayoutView().getLayout();
        serverContext.saveManagementLayout(layout);

        if (!canHandleReconfigurations()) {
            log.error("Can't run failure detector. This Server: {}, is not a part of the active layout: {}",
                    serverContext.getLocalEndpoint(), layout);
            return;
        }

        if (!failureDetectorFuture.isDone()) {
            log.debug("Cannot initiate new failure detection task. Polling in progress. Counter: {}", counter.get());
            counter.incrementAndGet();
            return;
        }

        counter.set(1);

        failureDetectorFuture =
                //Get metrics from local monitoring service (local monitoring works in it's own thread)
                localMonitoringService.getMetrics()
                //Poll report asynchronously using failureDetectorWorker executor
                .thenCompose(this::pollReport)
                //Update cluster view by latest cluster state given by the poll report. No need to be asynchronous
                .thenApply(pollReport -> {
                    log.trace("Update cluster view: {}", pollReport.getClusterState());
                    clusterContext.refreshClusterView(layout, pollReport);
                    return pollReport;
                })
                //Execute failure detector task using failureDetectorWorker executor
                .thenCompose(this::runFailureDetectorTask)
                //Print exceptions to log
                .whenComplete((taskResult, ex) -> {
                    if (ex != null) {
                        log.error("Failure detection task finished with error", ex);
                    }
                });
    }

    private CompletableFuture<PollReport> pollReport(SequencerMetrics sequencerMetrics) {
        return CompletableFuture.supplyAsync(() -> {
            Layout layout = serverContext.copyManagementLayout();
            CorfuRuntime corfuRuntime = getCorfuRuntime();

            return failureDetector.poll(layout, corfuRuntime, sequencerMetrics);
        }, failureDetectorWorker);
    }

    /**
     * <pre>
     * Failure/Healing detector task. Detects faults in the cluster and heals servers also corrects wrong epochs:
     *  - correct wrong epochs by resealing the servers and updating trailing layout servers.
     *    Fetch quorum layout and update the epoch according to the quorum.
     *  - check if current layout slot is unfilled then update layout to latest one.
     *  - Looking for link failures in the cluster, handle failure if found
     *  - bootstrap sequencer if needed
     *  - handle healed nodes
     * </pre>
     *
     * @param pollReport cluster status
     * @return async detection task
     */
    private CompletableFuture<DetectorTask> runFailureDetectorTask(PollReport pollReport) {

        if (!pollReport.getClusterState().isReady()) {
            log.info("Cluster state is not ready: {}", pollReport.getClusterState());
            return DETECTOR_TASK_SKIPPED;
        }

        return CompletableFuture.supplyAsync(() -> {

            // Corrects out of phase epoch issues if present in the report. This method
            // performs re-sealing of all nodes if required and catchup of a layout server to
            // the current state.
            correctWrongEpochs(pollReport);

            try {
                if (pollReport.isCurrentLayoutSlotUnFilled()) {
                    handleFailure(Collections.emptySet(), pollReport).get();
                    return DetectorTask.COMPLETED;
                }

                if (!pollReport.getWrongEpochs().isEmpty()) {
                    log.debug("Wait for next iteration. Poll report contains wrong epochs: {}",
                            pollReport.getWrongEpochs()
                    );
                    return DetectorTask.COMPLETED;
                }
            } catch (Exception ex) {
                log.error("Can't fill slot. Poll report: {}", pollReport, ex);
            }

            DetectorTask healing = handleHealing(pollReport, serverContext.copyManagementLayout());

            if (healing == DetectorTask.COMPLETED){
                return DetectorTask.COMPLETED;
            }

            // Analyze the poll report and trigger failure handler if needed.
            DetectorTask handleFailure = handleFailure(pollReport);

            //If a failure is detected (which means we have updated a layout)
            // then don't try to heal anything, wait for next iteration.
            if (handleFailure == DetectorTask.COMPLETED) {
                return DetectorTask.COMPLETED;
            }

            handleSequencer(serverContext.copyManagementLayout());

            return DetectorTask.COMPLETED;
        }, failureDetectorWorker);
    }

    /**
     * Handle healed node.
     * Cluster advisor provides healed node based on current cluster state if healed node found then
     * save healed node in the history and send a message with the detected healed node
     * to the relevant management server
     *
     * @param pollReport poll report
     * @param layout     current layout
     */
    private DetectorTask handleHealing(PollReport pollReport, Layout layout) {
        log.trace("Handle healing, layout: {}", layout);

        Optional<NodeRank> healed = advisor.healedServer(
                pollReport.getClusterState(),
                layout.getUnresponsiveServers()
        );

        //Tranform Optional value to a Set
        Set<String> healedNodes = healed
                .map(NodeRank::getEndpoint)
                .map(ImmutableSet::of)
                .orElse(ImmutableSet.of());

        if (healedNodes.isEmpty()) {
            log.trace("Nothing to heal");
            return DetectorTask.SKIPPED;
        }

        //save history
        FailureDetectorMetrics history = FailureDetectorMetrics.builder()
                .localNode(serverContext.getLocalEndpoint())
                .graph(advisor.getGraph(pollReport.getClusterState()).connectivityGraph())
                .healed(healed.get())
                .action(FailureDetectorAction.HEAL)
                .unresponsiveNodes(layout.getUnresponsiveServers())
                .layout(layout.getLayoutServers())
                .epoch(layout.getEpoch())
                .build();

        log.info("Handle healing. Failure detector state: {}", history.toJson());

        try {
            CorfuRuntime corfuRuntime = getCorfuRuntime();

            corfuRuntime.getLayoutView()
                    .getRuntimeLayout(layout)
                    .getManagementClient(serverContext.getLocalEndpoint())
                    //handle healing asynchronously
                    .handleHealing(pollReport.getPollEpoch(), healedNodes)
                    //completable future: wait this future to complete and get result
                    .get();

            serverContext.saveFailureDetectorMetrics(history);

            log.info("Healing local node successful: {}", history.toJson());

            return DetectorTask.COMPLETED;
        } catch (ExecutionException ee) {
            log.error("Healing local node failed: ", ee);
        } catch (InterruptedException ie) {
            log.error("Healing local node interrupted: ", ie);
            throw new UnrecoverableCorfuInterruptedError(ie);
        }

        return DetectorTask.NOT_COMPLETED;
    }

    /**
     * Analyzes the poll report and triggers the failure handler if node failure detected.
     * ClusterAdvisor provides a failed node in the cluster.
     * If a failed node have found:
     * - save detected failure in the history
     * - handle failure
     *
     * @param pollReport Poll report obtained from failure detection policy.
     * @return boolean result if failure was handled. False if there is no failure
     */
    private DetectorTask handleFailure(PollReport pollReport) {
        log.trace("Handle failures for the report: {}", pollReport);

        Layout layout = serverContext.copyManagementLayout();

        try {
            ClusterState clusterState = pollReport.getClusterState();

            if (clusterState.size() != layout.getAllServers().size()) {
                String err = String.format(
                        "Cluster representation is different than layout. Cluster: %s, layout: %s",
                        clusterState, layout
                );
                throw new IllegalStateException(err);
            }

            Optional<NodeRank> maybeFailedNode = advisor.failedServer(clusterState, layout.getUnresponsiveServers());

            if (maybeFailedNode.isPresent()) {
                NodeRank failedNode = maybeFailedNode.get();

                //Collect failures history
                FailureDetectorMetrics history = FailureDetectorMetrics.builder()
                        .localNode(serverContext.getLocalEndpoint())
                        .graph(advisor.getGraph(pollReport.getClusterState()).connectivityGraph())
                        .healed(failedNode)
                        .action(FailureDetectorAction.FAIL)
                        .unresponsiveNodes(layout.getUnresponsiveServers())
                        .layout(layout.getLayoutServers())
                        .epoch(layout.getEpoch())
                        .build();

                serverContext.saveFailureDetectorMetrics(history);

                Set<String> failedNodes = new HashSet<>();
                failedNodes.add(failedNode.getEndpoint());
                return handleFailure(failedNodes, pollReport).get();
            }
        } catch (Exception e) {
            log.error("Exception invoking failure handler : {}", e);
        }

        return DetectorTask.NOT_COMPLETED;
    }

    /**
     * Checks sequencer state, triggers a new task to bootstrap the sequencer for the specified layout (if needed).
     *
     * @param layout current layout
     */
    private CompletableFuture<DetectorTask> handleSequencer(Layout layout) {
        log.trace("Handling sequencer failures");

        if (localMonitoringService.getMetrics().join().getSequencerStatus() == SequencerStatus.READY) {
            log.trace("Primary sequencer is ready. Nothing to do");
            return DETECTOR_TASK_SKIPPED;
        }

        // If failures are not present we can check if the primary sequencer has been
        // bootstrapped from the heartbeat responses received.
        if (sequencerNotReadyCounter.getEpoch() != layout.getEpoch()) {
            // If the epoch is different than the poll epoch, we reset the timeout state.
            log.trace("Current epoch is different to layout epoch. Update current epoch to: {}", layout.getEpoch());
            sequencerNotReadyCounter = new SequencerNotReadyCounter(layout.getEpoch(), 1);
            return DETECTOR_TASK_SKIPPED;
        }

        // If the epoch is same as the epoch being tracked in the tuple, we need to
        // increment the count and attempt to bootstrap the sequencer if the count has
        // crossed the threshold.
        sequencerNotReadyCounter.increment();
        if (sequencerNotReadyCounter.getCounter() < SEQUENCER_NOT_READY_THRESHOLD) {
            return DETECTOR_TASK_SKIPPED;
        }

        // Launch task to bootstrap the primary sequencer.
        log.info("Attempting to bootstrap the primary sequencer.");
        // We do not care about the result of the trigger.
        // If it fails, we detect this again and retry in the next polling cycle.
        return getCorfuRuntime()
                .getLayoutManagementView()
                .asyncSequencerBootstrap(serverContext.copyManagementLayout(), failureDetectorWorker)
                .thenApply(DetectorTask::fromBool);
    }

    /**
     * Handle failures, sending message with detected failure to relevant management server.
     *
     * @param failedNodes list of failed nodes
     * @param pollReport  poll report
     */
    private CompletableFuture<DetectorTask> handleFailure(Set<String> failedNodes, PollReport pollReport) {

        ClusterGraph graph = advisor.getGraph(pollReport.getClusterState());

        log.info("Detected failed nodes in node responsiveness: Failed:{}, is slot unfilled: {}, clusterState:{}",
                failedNodes, pollReport.isCurrentLayoutSlotUnFilled(), graph.toJson()
        );

        Layout layout = serverContext.copyManagementLayout();

        return getCorfuRuntime()
                .getLayoutView()
                .getRuntimeLayout(layout)
                .getManagementClient(serverContext.getLocalEndpoint())
                .handleFailure(pollReport.getPollEpoch(), failedNodes)
                .thenApply(DetectorTask::fromBool);
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
            completableFuture = getCorfuRuntime()
                    .getLayoutView()
                    .getRuntimeLayout(layout)
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
    private void correctWrongEpochs(PollReport pollReport) {

        Map<String, Long> wrongEpochs = pollReport.getWrongEpochs();
        if (wrongEpochs.isEmpty()) {
            return;
        }

        log.debug("Correct wrong epochs. Poll report: {}", pollReport);

        try {
            Layout layout = serverContext.copyManagementLayout();
            final Layout layout1 = layout;
            // Query all layout servers to get quorum Layout.
            Map<String, CompletableFuture<Layout>> layoutCompletableFutureMap = layout
                    .getLayoutServers()
                    .stream()
                    .collect(Collectors.toMap(Function.identity(), server -> getLayoutFromServer(layout1, server)));

            // Retrieve the correct layout from quorum of members to reseal servers.
            // If we are unable to reach a consensus from a quorum we get an exception and
            // abort the epoch correction phase.
            Optional<Layout> latestLayout = fetchLatestLayout(layoutCompletableFutureMap);

            if (!latestLayout.isPresent()){
                log.error("Can't get a layout from any server in the cluster. Layout servers: {}, wrong epochs: {}",
                        layout.getLayoutServers(), wrongEpochs
                );
                throw new IllegalStateException("Error in correcting server epochs. Local node is disconnected");
            }

            // Update local layout copy.
            serverContext.saveManagementLayout(latestLayout.get());
            layout = serverContext.copyManagementLayout();

            // In case of a partial seal, a set of servers can be sealed with a higher epoch.
            // We should be able to detect this and bring the rest of the servers to this epoch.
            long maxOutOfPhaseEpoch = Collections.max(wrongEpochs.values());
            if (maxOutOfPhaseEpoch > layout.getEpoch()) {
                layout.setEpoch(maxOutOfPhaseEpoch);
            }

            // Re-seal all servers with the latestLayout epoch.
            // This has no effect on up-to-date servers. Only the trailing servers are caught up.
            getCorfuRuntime()
                    .getLayoutView()
                    .getRuntimeLayout(layout)
                    .sealMinServerSet();

            // Check if any layout server has a stale layout.
            // If yes patch it (commit) with the latestLayout.
            updateTrailingLayoutServers(layoutCompletableFutureMap, layout);

        } catch (QuorumUnreachableException e) {
            log.error("Error in correcting server epochs: {}", e);
        }
    }

    /**
     * Fetches the latest layout from the cluster.
     *
     * @return quorum agreed layout.
     * @throws QuorumUnreachableException If unable to receive consensus on layout.
     */
    private Optional<Layout> fetchLatestLayout(Map<String, CompletableFuture<Layout>> futureLayouts) {
        //Sort layouts according to epochs
        TreeSet<Layout> layouts = new TreeSet<>(Layout.LAYOUT_COMPARATOR);

        futureLayouts.values()
                .stream()
                //transform exceptions (connection errors) to optional values
                .map(async -> async.handle((layout, ex) -> {
                    //Ignore all connection errors
                    if (ex != null) {
                        return Optional.<Layout>empty();
                    }

                    return Optional.of(layout);
                }))
                //Get results synchronously
                .map(CompletableFuture::join)
                //Add all layouts to the set
                .forEach(optionalLayout -> optionalLayout.ifPresent(layouts::add));

        return Optional.ofNullable(layouts.first());
    }

    /**
     * Finds all trailing layout servers and patches them with the latest persisted layout
     * retrieved by quorum.
     *
     * @param layoutCompletableFutureMap Map of layout server endpoints to their layout requests.
     */
    private void updateTrailingLayoutServers(
            Map<String, CompletableFuture<Layout>> layoutCompletableFutureMap, Layout updatedLayout) {

        // Patch trailing layout servers with latestLayout.
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
            if (layout != null && layout.equals(updatedLayout)) {
                return;
            }
            try {
                // Committing this layout directly to the trailing layout servers.
                // This is safe because this layout is acquired by a quorum fetch which confirms
                // that there was a consensus on this layout and has been committed to a quorum.
                boolean result = getCorfuRuntime()
                        .getLayoutView()
                        .getRuntimeLayout(updatedLayout)
                        .getLayoutClient(layoutServer)
                        .committed(updatedLayout.getEpoch(), updatedLayout)
                        .get();
                if (result) {
                    log.debug("Layout Server: {} successfully patched with latest layout : {}",
                            layoutServer, updatedLayout);
                } else {
                    log.debug("Layout Server: {} patch with latest layout failed : {}", layoutServer, updatedLayout);
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
        failureDetectorWorker.shutdownNow();
        log.info("Fault Detection MonitoringService shutting down.");
    }

    public enum DetectorTask {
        /**
         * The task is completed successfully
         */
        COMPLETED,
        /**
         * The task is completed with an exception
         */
        NOT_COMPLETED,
        /**
         * Skipped task
         */
        SKIPPED;

        public static DetectorTask fromBool(boolean taskResult) {
            return taskResult ? COMPLETED : NOT_COMPLETED;
        }
    }
}
