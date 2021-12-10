package org.corfudb.infrastructure;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.result.Result;
import org.corfudb.infrastructure.management.ClusterAdvisor;
import org.corfudb.infrastructure.management.ClusterAdvisorFactory;
import org.corfudb.infrastructure.management.ClusterStateContext;
import org.corfudb.infrastructure.management.ClusterType;
import org.corfudb.infrastructure.management.FailureDetector;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.infrastructure.management.failuredetector.ClusterGraph;
import org.corfudb.infrastructure.management.failuredetector.EpochHandler;
import org.corfudb.infrastructure.management.failuredetector.FailureDetectorDataStore;
import org.corfudb.infrastructure.management.failuredetector.FailureDetectorException;
import org.corfudb.infrastructure.management.failuredetector.FailureDetectorHelper;
import org.corfudb.infrastructure.management.failuredetector.HealingAgent;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.FailureDetectorMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.FailureDetectorMetrics.FailureDetectorAction;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeRank;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.LambdaUtils;
import org.corfudb.util.concurrent.SingletonResource;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Remote Monitoring Service constitutes of failure and healing monitoring and handling.
 * This service is responsible for heartbeat and aggregating the cluster view. This is
 * updated in the shared context with the management server which serves the heartbeat responses.
 * The failure detector updates the unreachable nodes in the layout.
 * The healing detector heals nodes which were previously marked unresponsive but have now healed.
 * Created by zlokhandwala on 11/2/18.
 */
@Slf4j
public class RemoteMonitoringService implements ManagementService {

    private static final CompletableFuture<DetectorTask> DETECTOR_TASK_NOT_COMPLETED
            = CompletableFuture.completedFuture(DetectorTask.NOT_COMPLETED);

    private static final CompletableFuture<DetectorTask> DETECTOR_TASK_SKIPPED
            = CompletableFuture.completedFuture(DetectorTask.SKIPPED);

    /**
     * Detectors to be used to detect failures and healing.
     */
    @Getter
    private final FailureDetector failureDetector;

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

    private final HealingAgent healingAgent;

    private final FailureDetectorDataStore fdDataStore;

    /**
     * The management agent attempts to bootstrap a NOT_READY sequencer if the
     * sequencerNotReadyCounter counter exceeds this value.
     */
    private final int sequencerNotReadyThreshold = 3;

    /**
     * Number of workers for failure detector. Three workers used by default:
     * - failure/healing detection
     * - bootstrap sequencer
     * - merge segments
     */
    private final int detectionWorkersCount = 3;

    /**
     * This tuple maintains, in an epoch, how many heartbeats the primary sequencer has responded
     * in not bootstrapped (NOT_READY) state.
     */
    @Getter
    @Setter
    @AllArgsConstructor
    private static class SequencerNotReadyCounter {
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
                            @NonNull FailureDetector failureDetector,
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
                        .setNameFormat(serverContext.getThreadPrefix() + "RemoteMonitoringService")
                        .build());

        // Creating the detection worker thread pool.
        // This thread pool is utilized to dispatch detection tasks at regular intervals in the
        // detectorTaskScheduler.
        ThreadFactory fdThreadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(serverContext.getThreadPrefix() + "DetectionWorker-%d")
                .build();
        this.failureDetectorWorker = Executors.newFixedThreadPool(detectionWorkersCount, fdThreadFactory);

        fdDataStore = FailureDetectorDataStore.builder()
                .localEndpoint(serverContext.getLocalEndpoint())
                .dataStore(serverContext.getDataStore())
                .build();

        healingAgent = HealingAgent.builder()
                .advisor(advisor)
                .localEndpoint(serverContext.getLocalEndpoint())
                .runtimeSingleton(runtimeSingletonResource)
                .failureDetectorWorker(failureDetectorWorker)
                .dataStore(fdDataStore)
                .build();
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

        Runnable task = () -> {
            if (!failureDetectorFuture.isDone()) {
                return;
            }

            failureDetectorFuture = runDetectionTasks();
        };

        detectionTasksScheduler.scheduleAtFixedRate(
                () -> LambdaUtils.runSansThrow(task),
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
    private synchronized CompletableFuture<DetectorTask> runDetectionTasks() {

        return getCorfuRuntime()
                .invalidateLayout()
                .thenApply(serverContext::saveManagementLayout)
                //check if this node can handle failures (if the node is in the cluster)
                .thenCompose(ourLayout -> {
                    String localEndpoint = serverContext.getLocalEndpoint();
                    FailureDetectorHelper helper = new FailureDetectorHelper(ourLayout, localEndpoint);

                    return helper.handleReconfigurationsAsync();
                })
                .thenCompose(ourLayout -> {
                    //Get metrics from local monitoring service (local monitoring works in it's own thread)
                    return localMonitoringService.getMetrics()//Poll report asynchronously using failureDetectorWorker executor
                            .thenCompose(metrics -> pollReport(ourLayout, metrics))
                            //Update cluster view by latest cluster state given by the poll report. No need to be asynchronous
                            .thenApply(pollReport -> {
                                log.trace("Update cluster view: {}", pollReport.getClusterState());
                                clusterContext.refreshClusterView(ourLayout, pollReport);
                                return pollReport;
                            })
                            .thenApply(pollReport -> {
                                if (!pollReport.getClusterState().isReady()) {
                                    throw FailureDetectorException.notReady(pollReport.getClusterState());
                                }

                                return pollReport;
                            })
                            //Execute failure detector task using failureDetectorWorker executor
                            .thenCompose(pollReport -> runFailureDetectorTask(pollReport, ourLayout));
                })
                //Print exceptions to log
                .whenComplete((taskResult, ex) -> {
                    if (ex != null) {
                        log.error("Failure detection task finished with an error", ex);
                    }
                });
    }

    private CompletableFuture<PollReport> pollReport(Layout layout, SequencerMetrics sequencerMetrics) {
        return CompletableFuture.supplyAsync(() -> {
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
     *  - handle healed nodes.
     *  - Looking for link failures in the cluster, handle failure if found.
     *  - Restore redundancy and merge segments if present in the layout.
     *  - bootstrap sequencer if needed
     * </pre>
     *
     * @param pollReport cluster status
     * @return async detection task
     */
    private CompletableFuture<DetectorTask> runFailureDetectorTask(PollReport pollReport, Layout ourLayout) {

        EpochHandler epochHandler = EpochHandler.builder()
                .corfuRuntime(getCorfuRuntime())
                .serverContext(serverContext)
                .failureDetectorWorker(failureDetectorWorker)
                .build();

        // Corrects out of phase epoch issues if present in the report. This method
        // performs re-sealing of all nodes if required and catchup of a layout server to
        // the current state.
        CompletableFuture<Layout> wrongEpochTask = epochHandler.correctWrongEpochs(pollReport, ourLayout);

        return wrongEpochTask.thenApplyAsync(latestLayout -> {

            // This is just an optimization in case we receive a WrongEpochException
            // while one of the other management clients is trying to move to a new layout.
            // This check is merely trying to minimize the scenario in which we end up
            // filling the slot with an outdated layout.
            if (!pollReport.areAllResponsiveServersSealed()) {
                log.debug("All responsive servers have not been sealed yet. Skipping.");
                return DetectorTask.COMPLETED;
            }

            Result<DetectorTask, RuntimeException> failure = Result.of(() -> {

                Optional<Long> unfilledSlot = pollReport.getLayoutSlotUnFilled(latestLayout);
                // If the latest slot has not been filled, fill it with the previous known layout.
                if (unfilledSlot.isPresent()) {
                    log.info("Trying to fill an unfilled slot {}. PollReport: {}",
                            unfilledSlot.get(), pollReport);
                    detectFailure(latestLayout, Collections.emptySet(), pollReport).join();
                    return DetectorTask.COMPLETED;
                }

                return DetectorTask.NOT_COMPLETED;
            });

            if (!pollReport.getWrongEpochs().isEmpty()) {
                log.debug("Wait for next iteration. Poll report contains wrong epochs: {}",
                        pollReport.getWrongEpochs()
                );
                return DetectorTask.COMPLETED;
            }

            // If layout was updated by correcting wrong epochs,
            // we can't continue with failure detection,
            // as the cluster state has changed.
            if (!latestLayout.equals(ourLayout)) {
                log.warn("Layout was updated by correcting wrong epochs. " +
                        "Cancel current round of failure detection.");
                return DetectorTask.COMPLETED;
            }

            failure.ifError(err -> log.error("Can't fill slot. Poll report: {}", pollReport, err));

            if (failure.isValue() && failure.get() == DetectorTask.COMPLETED) {
                return DetectorTask.COMPLETED;
            }

            DetectorTask healing = healingAgent.detectHealing(pollReport, ourLayout).join();

            //If local node healed it causes change in the cluster state which means the layout is changed also.
            //If the cluster status is changed let failure detector detect the change on next iteration and
            //behave according to latest cluster state.
            if (healing == DetectorTask.COMPLETED) {
                return DetectorTask.COMPLETED;
            }

            // Analyze the poll report and trigger failure handler if needed.
            DetectorTask handleFailure = detectFailure(pollReport, ourLayout);

            //If a failure is detected (which means we have updated a layout)
            // then don't try to heal anything, wait for next iteration.
            if (handleFailure == DetectorTask.COMPLETED) {
                return DetectorTask.COMPLETED;
            }

            // Restores redundancy and merges multiple segments if present.
            healingAgent.restoreRedundancyAndMergeSegments(ourLayout);

            handleSequencer(ourLayout);

            return DetectorTask.COMPLETED;

        }, failureDetectorWorker);
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
    private DetectorTask detectFailure(PollReport pollReport, Layout layout) {
        log.trace("Handle failures for the report: {}", pollReport);

        try {
            ClusterState clusterState = pollReport.getClusterState();

            if (clusterState.size() != layout.getAllServers().size()) {
                throw FailureDetectorException.layoutMismatch(clusterState, layout);
            }

            Optional<NodeRank> maybeFailedNode = advisor.failedServer(clusterState);

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

                fdDataStore.saveFailureDetectorMetrics(history);

                Set<String> failedNodes = new HashSet<>();
                failedNodes.add(failedNode.getEndpoint());
                return detectFailure(layout, failedNodes, pollReport).get();
            }
        } catch (Exception e) {
            log.error("Exception invoking failure handler", e);
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

        ClusterState clusterState = clusterContext.getClusterView();
        Optional<NodeState> primarySequencer = clusterState.getNode(layout.getPrimarySequencer());
        if (primarySequencer.isPresent() && primarySequencer.get().getSequencerMetrics() == SequencerMetrics.READY) {
            log.trace("Primary sequencer is already ready at: {} in {}", primarySequencer.get(), clusterState);
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
        if (sequencerNotReadyCounter.getCounter() < sequencerNotReadyThreshold) {
            return DETECTOR_TASK_SKIPPED;
        }

        // Launch task to bootstrap the primary sequencer.
        log.trace("Attempting to bootstrap the primary sequencer. ClusterState {}", clusterState);
        // We do not care about the result of the trigger.
        // If it fails, we detect this again and retry in the next polling cycle.
        return getCorfuRuntime()
                .getLayoutManagementView()
                .asyncSequencerBootstrap(layout, failureDetectorWorker)
                .thenApply(DetectorTask::fromBool);
    }

    /**
     * Handle failures, sending message with detected failure to relevant management server.
     *
     * @param failedNodes list of failed nodes
     * @param pollReport  poll report
     */
    private CompletableFuture<DetectorTask> detectFailure(
            Layout layout, Set<String> failedNodes, PollReport pollReport) {

        ClusterGraph graph = advisor.getGraph(pollReport.getClusterState());

        log.info("Detected failed nodes in node responsiveness: Failed:{}, is slot unfilled: {}, clusterState:{}",
                failedNodes, pollReport.getLayoutSlotUnFilled(layout), graph.toJson()
        );

        return getCorfuRuntime()
                .getLayoutView()
                .getRuntimeLayout(layout)
                .getManagementClient(serverContext.getLocalEndpoint())
                .handleFailure(layout.getEpoch(), failedNodes)
                .thenApply(DetectorTask::fromBool);
    }

    @Override
    public void shutdown() {
        // Shutting the fault detector.
        detectionTasksScheduler.shutdownNow();
        failureDetectorWorker.shutdownNow();
        log.info("Fault detection service shutting down.");
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
