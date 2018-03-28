package org.corfudb.infrastructure;

import java.util.HashMap;
import java.util.Map;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.NetworkMetrics;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.Layout;
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

    /**
     * Interval in checking presence of management layout to start management agent tasks.
     * In milliseconds.
     */
    @Getter
    private static final long CHECK_BOOTSTRAP_INTERVAL = 1000;
    /**
     * To dispatch initialization tasks for recovery and sequencer bootstrap.
     */
    @Getter
    private final Thread initializationTaskThread;

    private boolean recovered = false;

    private final SingletonResource<CorfuRuntime> runtimeSingletonResource;

    private volatile boolean shutdown = false;

    @Getter(AccessLevel.PROTECTED)
    private final LocalMetricsPollingService localMetricsPollingService;

    @Getter
    private final FaultDetectionService faultDetectionService;

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

        this.localMetricsPollingService = new LocalMetricsPollingService(serverContext,
                runtimeSingletonResource);
        this.faultDetectionService = new FaultDetectionService(serverContext,
                runtimeSingletonResource);

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
     * Initialization task.
     * Performs recovery if required.
     * Bootstraps the primary sequencer if the server has been bootstrapped.
     * Initiates the failure detection and healing detection tasks.
     */
    private void initializationTask() {

        try {
            while (!shutdown && serverContext.getManagementLayout() == null) {
                log.warn("Management Server waiting to be bootstrapped");
                Sleep.MILLISECONDS.sleepRecoverably(CHECK_BOOTSTRAP_INTERVAL);
            }

            // Recover if flag is false
            if (!recovered) {
                RecoveryHandler.retryUntilRecovery(serverContext, getCorfuRuntime());
            }

            this.localMetricsPollingService.runTask();
            this.faultDetectionService.runTask();

        } catch (InterruptedException e) {
            log.error("initializationTask: InitializationTask interrupted.");
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("initializationTask: Error in initializationTask.", e);
            throw new UnrecoverableCorfuError(e);
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
     * Combines the peer connectivity view from the failure and the healing detectors.
     * This map containing the view of the detectors is used to create the NodeView.
     * This NodeView is sent in the HeartbeatResponse message.
     *
     * @return Peer connectivity view map
     */
    NetworkMetrics getConnectivityView() {
        Map<String, Boolean> peerConnectivityDeltaMap = new HashMap<>();
        faultDetectionService.getResponsiveNodesPeerView()
                .forEach(s -> peerConnectivityDeltaMap.put(s, true));
        faultDetectionService.getUnresponsiveNodesPeerView()
                .forEach(s -> peerConnectivityDeltaMap.put(s, false));
        // If the management server is not bootstrapped, stamp with INVALID_EPOCH.
        long peerConnectivitySnapshotEpoch = serverContext.getManagementLayout() != null
                ? serverContext.getManagementLayout().getEpoch() : Layout.INVALID_EPOCH;
        return new NetworkMetrics(peerConnectivitySnapshotEpoch, peerConnectivityDeltaMap);
    }

    /**
     * Shutdown the detectorTaskScheduler, workers and initializationTaskThread.
     */
    public void shutdown() {
        // Shutting the fault detector.
        shutdown = true;

        faultDetectionService.shutdown();
        localMetricsPollingService.shutdown();

        try {
            initializationTaskThread.interrupt();
            initializationTaskThread.join(ServerContext.SHUTDOWN_TIMER.toMillis());
        } catch (InterruptedException ie) {
            log.error("initializationTask interrupted : {}", ie);
            Thread.currentThread().interrupt();
        }

        log.info("Management Agent shutting down.");
    }

}
