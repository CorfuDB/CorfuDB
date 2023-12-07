package org.corfudb.infrastructure;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.health.Component;
import org.corfudb.infrastructure.health.HealthMonitor;
import org.corfudb.infrastructure.health.Issue;
import org.corfudb.infrastructure.management.ClusterStateContext;
import org.corfudb.infrastructure.management.FailureDetector;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.concurrent.SingletonResource;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Instantiates and performs failure detection and handling asynchronously,
 * as well as the auto commit service.
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

    /**
     * Interval in checking presence of management layout to start management agent tasks.
     */
    private static final Duration CHECK_BOOTSTRAP_INTERVAL = Duration.ofSeconds(1);

    /**
     * Interval in retrying layout recovery when management server started.
     */
    private static final Duration RECOVERY_RETRY_INTERVAL = Duration.ofSeconds(1);

    /**
     * Locally collected server metrics polling interval.
     */
    private static final Duration METRICS_POLL_INTERVAL = Duration.ofMillis(500);

    /**
     * Interval in executing the failure detection policy.
     */
    private static final Duration POLICY_EXECUTE_INTERVAL = Duration.ofMillis(500);

    /**
     * Interval of executing the AutoCommitService.
     */
    private static final Duration AUTO_COMMIT_INTERVAL = Duration.ofSeconds(15);

    /**
     * Interval of executing the CompactionService.
     */
    private static final Duration TRIGGER_INTERVAL = Duration.ofSeconds(10);

    /**
     * Compactor flag
     */
    private static final String COMPACTOR_SCRIPT_PATH_KEY = "--compactor-script";

    /**
     * To dispatch initialization tasks for recovery and sequencer bootstrap.
     */
    private final Thread initializationTaskThread;

    private final SingletonResource<CorfuRuntime> runtimeSingletonResource;

    private volatile boolean shutdown = false;

    private boolean recovered = false;

    /**
     * LocalMonitoringService to poll local server metrics:
     * Sequencer ready/not ready state.
     */
    @Getter(AccessLevel.PROTECTED)
    private final LocalMonitoringService localMonitoringService;

    /**
     * RemoteMonitoringService to detect faults and generate a cluster connectivity graph.
     */
    @Getter
    private final RemoteMonitoringService remoteMonitoringService;

    /**
     * AutoCommitService to periodically commit the unwritten addresses.
     */
    @Getter
    private final AutoCommitService autoCommitService;

    /**
     * CompactorService to periodically orchestrate distributed compaction.
     */
    @Getter
    private final CompactorService compactorService;

    /**
     * Checks and restores if a layout is present in the local datastore to recover from.
     * Spawns an initialization task which recovers if required, and start monitoring services.
     *
     * @param runtimeSingletonResource Singleton resource to fetch runtime.
     * @param serverContext            Server Context.
     */
    ManagementAgent(@NonNull SingletonResource<CorfuRuntime> runtimeSingletonResource,
                    @NonNull ServerContext serverContext,
                    @NonNull ClusterStateContext clusterContext,
                    @NonNull FailureDetector failureDetector,
                    Layout managementLayout) {
        this.runtimeSingletonResource = runtimeSingletonResource;
        this.serverContext = serverContext;
        this.localMonitoringService = new LocalMonitoringService(serverContext, runtimeSingletonResource);

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
        log.info("ManagementAgent(): serverContext.getCurrentLayout() {}, managementLayout {}",
                serverContext.getCurrentLayout(), managementLayout);
        serverContext.saveManagementLayout(serverContext.getCurrentLayout());
        serverContext.saveManagementLayout(managementLayout);

        this.remoteMonitoringService = new RemoteMonitoringService(
                serverContext,
                runtimeSingletonResource,
                clusterContext,
                failureDetector,
                localMonitoringService
        );
        HealthMonitor.reportIssue(Issue.createInitIssue(Component.FAILURE_DETECTOR));

        this.autoCommitService = new AutoCommitService(serverContext, runtimeSingletonResource);

        final String compactorScript = COMPACTOR_SCRIPT_PATH_KEY;
        if (serverContext.getServerConfig(String.class, compactorScript) != null) {
            HealthMonitor.reportIssue(Issue.createInitIssue(Component.COMPACTOR));
        }

        this.compactorService = new CompactorService(serverContext, TRIGGER_INTERVAL,
                new InvokeCheckpointingJvm(serverContext), new DynamicTriggerPolicy());

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

    private void tryImmediateRecovery() {
        log.info("Attempting immediate recovery");
        for(int i = 0; i < 3; i++) {
            try {
                getCorfuRuntime().invalidateLayout();
                Layout layout = getCorfuRuntime().getLayoutView().getLayout();
                serverContext.saveManagementLayout(layout);
                boolean serverRecovered = getCorfuRuntime()
                        .getLayoutManagementView()
                        .attemptClusterRecovery(serverContext.copyManagementLayout());
                if (serverRecovered) {
                    getCorfuRuntime().invalidateLayout();
                    Layout updatedLayout = getCorfuRuntime().getLayoutView().getLayout();
                    log.info("Cluster recovery succeeded. Latest management layout: {}", updatedLayout);
                    serverContext.saveManagementLayout(updatedLayout);
                    return;
                }
                TimeUnit.MILLISECONDS.sleep(RECOVERY_RETRY_INTERVAL.toMillis());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(ie);
            }
            catch (RuntimeException re) {
                log.error("Hit exception when trying immediate recovery", re);
            }

        }
        log.warn("Failed to recover immediately after the restart. The recovery will be completed " +
                "by the failure detector.");
    }

    /**
     * Initialization task.
     * This task is blocked until the management server is bootstrapped and has a connected runtime.
     * Initiates the monitoring services which performs failure detection and healing detection tasks.
     */
    private void initializationTask() {
        log.info("Start initialization task");
        // Wait for management server to be bootstrapped.
        try {
            while (!shutdown && serverContext.getManagementLayout() == null) {
                log.warn("initializationTask: Management Server waiting to be bootstrapped");
                TimeUnit.MILLISECONDS.sleep(CHECK_BOOTSTRAP_INTERVAL.toMillis());
            }
        } catch (InterruptedException e) {
            // Due to shutdown, which interrupts this thread
            log.debug("initializationTask: Initialization task interrupted without " +
                    "management server bootstrapped");
            return;
        }

        if (!recovered) {
            // If the node is found with a management layout - it has been restarted previously.
            // Attempt to update the local management layout to the most recent cluster layout and
            // bump the epoch. If the consensus can not be reached for some reason,
            // the remote monitoring service of the current node will attempt to heal/rejoin
            // during one of the failure detection rounds.
            tryImmediateRecovery();
            recovered = true;
        }

        // Start management services that deals with failure & healing detection and auto commit.
        if (!shutdown) {
            localMonitoringService.start(METRICS_POLL_INTERVAL);
            remoteMonitoringService.start(POLICY_EXECUTE_INTERVAL);
            if (!serverContext.getServerConfig(Boolean.class, "--no-auto-commit")) {
                autoCommitService.start(AUTO_COMMIT_INTERVAL);
            } else {
                log.info("Auto commit service disabled.");
            }
            if (serverContext.getServerConfig(String.class, COMPACTOR_SCRIPT_PATH_KEY) != null) {
                compactorService.start(TRIGGER_INTERVAL);
            } else {
                log.info("Compaction Service disabled");
            }
        }
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
     * Shutdown the initializationTaskThread and monitoring services.
     */
    public void shutdown() {
        // Shutting the fault detector.
        shutdown = true;

        try {
            initializationTaskThread.interrupt();
            initializationTaskThread.join(ServerContext.SHUTDOWN_TIMER.toMillis());
        } catch (InterruptedException ie) {
            log.error("initializationTask interrupted : {}", ie);
            throw new UnrecoverableCorfuInterruptedError(ie);
        }

        remoteMonitoringService.shutdown();
        localMonitoringService.shutdown();
        autoCommitService.shutdown();
        compactorService.shutdown();

        log.info("Management Agent shutting down.");
    }

}
