package org.corfudb.infrastructure;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.health.Component;
import org.corfudb.infrastructure.health.HealthMonitor;
import org.corfudb.infrastructure.health.Issue;
import org.corfudb.infrastructure.log.FileSystemAgent;
import org.corfudb.infrastructure.log.FileSystemAgent.PartitionAgent.PartitionAttribute;
import org.corfudb.infrastructure.management.ClusterAdvisor;
import org.corfudb.infrastructure.management.ClusterAdvisorFactory;
import org.corfudb.infrastructure.management.ClusterStateContext;
import org.corfudb.infrastructure.management.ClusterType;
import org.corfudb.infrastructure.management.FailureDetector;
import org.corfudb.infrastructure.management.FileSystemAdvisor;
import org.corfudb.infrastructure.management.PollReport;
import org.corfudb.infrastructure.management.failuredetector.EpochHandler;
import org.corfudb.infrastructure.management.failuredetector.FailureDetectorDataStore;
import org.corfudb.infrastructure.management.failuredetector.FailureDetectorException;
import org.corfudb.infrastructure.management.failuredetector.FailureDetectorHelper;
import org.corfudb.infrastructure.management.failuredetector.FailureDetectorService;
import org.corfudb.infrastructure.management.failuredetector.FailureDetectorService.SequencerBootstrapper;
import org.corfudb.infrastructure.management.failuredetector.FailuresAgent;
import org.corfudb.infrastructure.management.failuredetector.HealingAgent;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.BatchProcessorStats;
import org.corfudb.protocols.wireprotocol.failuredetector.FileSystemStats.PartitionAttributeStats;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.LambdaUtils;
import org.corfudb.util.concurrent.SingletonResource;

import java.time.Duration;
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

    private final FailureDetectorService fdService;

    /**
     * Number of workers for failure detector. Three workers used by default:
     * - failure/healing detection
     * - bootstrap sequencer
     * - merge segments
     */
    private final int detectionWorkersCount = 3;

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
        ClusterAdvisor advisor = ClusterAdvisorFactory.createForStrategy(
                ClusterType.COMPLETE_GRAPH
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

        FailureDetectorDataStore fdDataStore = FailureDetectorDataStore.builder()
                .localEndpoint(serverContext.getLocalEndpoint())
                .dataStore(serverContext.getDataStore())
                .build();

        FileSystemAdvisor fsAdvisor = new FileSystemAdvisor();

        EpochHandler epochHandler = EpochHandler.builder()
                .runtimeSingletonResource(runtimeSingletonResource)
                .serverContext(serverContext)
                .failureDetectorWorker(failureDetectorWorker)
                .build();

        HealingAgent healingAgent = HealingAgent.builder()
                .advisor(advisor)
                .fsAdvisor(fsAdvisor)
                .runtimeSingleton(runtimeSingletonResource)
                .failureDetectorWorker(failureDetectorWorker)
                .dataStore(fdDataStore)
                .build();

        FailuresAgent failuresAgent = FailuresAgent.builder()
                .fdDataStore(fdDataStore)
                .advisor(advisor)
                .fsAdvisor(fsAdvisor)
                .runtimeSingleton(runtimeSingletonResource)
                .build();

        SequencerBootstrapper sequencerBootstrapper = SequencerBootstrapper.builder()
                .runtimeSingletonResource(runtimeSingletonResource)
                .clusterContext(clusterContext)
                .failureDetectorWorker(failureDetectorWorker)
                .build();

        this.fdService = FailureDetectorService.builder()
                .epochHandler(epochHandler)
                .failuresAgent(failuresAgent)
                .healingAgent(healingAgent)
                .sequencerBootstrapper(sequencerBootstrapper)
                .failureDetectorWorker(failureDetectorWorker)
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
        HealthMonitor.resolveIssue(Issue.createInitIssue(Component.FAILURE_DETECTOR));
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
        String localEndpoint = serverContext.getLocalEndpoint();
        return getCorfuRuntime()
                .invalidateLayout()
                .thenApply(serverContext::saveManagementLayout)
                //check if this node can handle failures (if the node is in the cluster)
                .thenCompose(ourLayout -> {
                    FailureDetectorHelper helper = new FailureDetectorHelper(ourLayout, localEndpoint);

                    return helper.handleReconfigurationsAsync();
                })
                .thenCompose(ourLayout -> {
                    //Get metrics from local monitoring service (local monitoring works in it's own thread)
                    return localMonitoringService.getMetrics() //Poll report asynchronously using failureDetectorWorker executor
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
                            .thenCompose(pollReport -> fdService.runFailureDetectorTask(pollReport, ourLayout, localEndpoint));
                }).whenComplete((taskResult, ex) -> {
                    if (ex != null) {
                        log.error("Failure detection task finished with an error", ex);

                    }
                    if (ex != null || taskResult == DetectorTask.NOT_COMPLETED) {
                        log.trace("Reporting issue");
                        HealthMonitor.reportIssue(new Issue(Component.FAILURE_DETECTOR,
                                Issue.IssueId.FAILURE_DETECTOR_TASK_FAILED,
                                "Last failure detector task was not completed"));
                    } else {
                        log.trace("Resolving issue");
                        HealthMonitor.resolveIssue(new Issue(Component.FAILURE_DETECTOR,
                                Issue.IssueId.FAILURE_DETECTOR_TASK_FAILED,
                                "Last failure detector task succeeded"));
                    }
                });
    }

    private CompletableFuture<PollReport> pollReport(Layout layout, SequencerMetrics sequencerMetrics) {
        return CompletableFuture.supplyAsync(() -> {
            PartitionAttribute partition = FileSystemAgent.getPartitionAttribute();
            PartitionAttributeStats partitionAttributeStats = new PartitionAttributeStats(
                    partition.isReadOnly(),
                    partition.getAvailableSpace(),
                    partition.getTotalSpace()
            );

            BatchProcessorStats bpStats = new BatchProcessorStats(partition.getBatchProcessorStatus());
            FileSystemStats fsStats = new FileSystemStats(partitionAttributeStats, bpStats);

            CorfuRuntime corfuRuntime = getCorfuRuntime();

            return failureDetector.poll(layout, corfuRuntime, sequencerMetrics, fsStats);
        }, failureDetectorWorker);
    }

    @Override
    public void shutdown() {
        // Shutting the fault detector.
        detectionTasksScheduler.shutdownNow();
        failureDetectorWorker.shutdownNow();
        log.info("Fault detection service shutting down.");
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.FAILURE_DETECTOR));
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
