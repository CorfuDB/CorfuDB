package org.corfudb.infrastructure;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutView;

import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Instantiates and performs failure detection and handling asynchronously.
 * <p>
 * Failure Detector:
 * Executes detection policy (eg. PeriodicPollingPolicy).
 * It then checks for status of the nodes. If the result map is not empty,
 * there are failed nodes to be addressed. This then triggers the failure
 * handler which then responds to these failures based on a policy.
 * <p>
 * Created by zlokhandwala on 9/28/16.
 */
@Slf4j
public class ManagementServer extends AbstractServer {

    @Override
    public void reset() {
    }

    @Override
    public void reboot() {
    }

    /**
     * The options map.
     */
    private Map<String, Object> opts;
    private ServerContext serverContext;

    private CorfuRuntime corfuRuntime;
    /**
     * Policy to be used to detect failures.
     */
    private IFailureDetectorPolicy failureDetectorPolicy;
    /**
     * Used for initial bootstrap only.
     */
    private LayoutServer layoutServer;
    /**
     * Interval in executing the failure detection policy.
     * In milliseconds.
     */
    @Getter
    private long policyExecuteInterval = 1000;
    /**
     * Boolean flag denoting whether the server is shutdown or not.
     */
    @Getter
    private volatile boolean isShutdown = false;
    /**
     * To schedule failure detection.
     */
    @Getter
    private final ScheduledExecutorService failureDetectorService;

    public ManagementServer(ServerContext serverContext, LayoutServer layoutServer) {

        this.opts = serverContext.getServerConfig();
        this.serverContext = serverContext;
        this.layoutServer = layoutServer;

        this.failureDetectorPolicy = serverContext.getFailureDetectorPolicy();

        this.failureDetectorService = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("Fault-Detector-" + getLocalEndpoint())
                        .build());

        try {
            failureDetectorService.scheduleAtFixedRate(
                    this::taskScheduler,
                    0,
                    policyExecuteInterval,
                    TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException err) {
            log.error("Error scheduling failure detection task, {}", err);
        }
    }

    /**
     * Wait until we receive a message with the layout endpoint to bootstrap the management server.
     */
    private Layout waitForBootstrap() {
        Layout currentLayout = null;
        log.debug("Waiting for bootstrap");
        while (currentLayout == null) {
            try {
                Thread.sleep(1000);
                currentLayout = layoutServer.getCurrentLayout();
            } catch (InterruptedException ie) {
                log.warn("Thread interrupted : {}", ie);
            }
            if (isShutdown) {
                return null;
            }
        }
        log.info("Node bootstrapped. Layout received by management server.");
        return currentLayout;
    }

    /**
     * Returns a connected instance of the CorfuRuntime.
     *
     * @return A connected instance of runtime or null if isShutdown is true.
     */
    private synchronized CorfuRuntime getCorfuRuntime() {

        if (corfuRuntime == null) {

            // TODO: Check for persisted layout if saved by the management server.
            // If yes, connect runtime to that layout as it is the latest snapshot
            // of the layout servers we have.
            // If no persisted layout present, wait for bootstrap. (Rely on local layout server)

            Layout bootstrapLayout = waitForBootstrap();
            if (bootstrapLayout == null) {
                return null;
            }

            corfuRuntime = new CorfuRuntime();
            bootstrapLayout.getLayoutServers().forEach(ls -> corfuRuntime.addLayoutServer(ls));
            corfuRuntime.connect();
        }
        return corfuRuntime;
    }


    /**
     * Gets the address of this endpoint.
     *
     * @return localEndpoint address
     */
    private String getLocalEndpoint() {
        return this.opts.get("--address") + ":" + this.opts.get("<port>");
    }


    /**
     * This contains the complete failure detection and handling mechanism.
     * <p>
     * It first checks whether the current node is bootstrapped.
     * If not, it continues checking in intervals of 1 second.
     * If yes, it sets up the corfuRuntime and continues execution
     * of the policy.
     * <p>
     * It executes the policy which detects and reports failures.
     * Once detected, it triggers the trigger handler which takes care of
     * dispatching the appropriate handler.
     * <p>
     * Currently executing the periodicPollPolicy.
     * It executes the the polling at an interval of every 1 second.
     * After every poll it checks for any failures detected.
     */
    private void taskScheduler() {

        Layout currentLayout;
        LayoutView layoutView;
        CorfuRuntime corfuRuntime;

        corfuRuntime = getCorfuRuntime();
        if (corfuRuntime == null) return;

        corfuRuntime.invalidateLayout();

        // Fetch the latest layout view through the runtime.
        layoutView = corfuRuntime.getLayoutView();
        currentLayout = layoutView.getLayout();
        //TODO: Store this layout on disk for persistence.

        // Execute the failure detection policy once.
        failureDetectorPolicy.executePolicy(currentLayout, corfuRuntime);

        // Get the server status from the policy and check for failures.
        HashMap<String, Boolean> map = failureDetectorPolicy.getServerStatus();
        if (!map.isEmpty()) {
            // If map not empty, failures present. Trigger handler.
            log.info("Failures detected. Failed nodes : {}", map.keySet());
        } else {
            log.debug("No failures present.");
        }
    }

    /**
     * Management Server shutdown:
     * Shuts down the fault detector service.
     */
    public void shutdown() {
        // Shutting the fault detector.
        isShutdown = true;
        failureDetectorService.shutdownNow();

        // Shut down the Corfu Runtime.
        if (corfuRuntime != null) {
            corfuRuntime.shutdown();
        }

        try {
            failureDetectorService.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            log.debug("failureDetectorService awaitTermination interrupted : {}", ie);
        }
    }
}
