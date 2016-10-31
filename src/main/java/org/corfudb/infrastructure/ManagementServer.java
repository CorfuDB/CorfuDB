package org.corfudb.infrastructure;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutView;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

/**
 * Instantiates and performs failure detection adn handling asynchronously.
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

    private IFailureDetectorPolicy failureDetectorPolicy;
    private CorfuRuntime corfuRuntime;

    /**
     * Flag to notify management server of shutdown
     */
    @Getter
    private boolean isShutdown = false;
    /**
     * Stores this current node's address
     */
    @Getter
    private String localEndpoint;
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
     * To schedule failure detection.
     */
    private final ExecutorService schedulerService;


    public ManagementServer(ServerContext serverContext, LayoutServer layoutServer) {

        this.opts = serverContext.getServerConfig();
        this.serverContext = serverContext;
        this.layoutServer = layoutServer;

        this.failureDetectorPolicy = serverContext.getFailureDetectorPolicy();
        this.localEndpoint = getLocalEndpoint();

        this.schedulerService = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("Fault-Detector-" + this.localEndpoint)
                        .build());

        try {
            schedulerService.submit(this::taskScheduler);
        } catch (RejectedExecutionException err) {
            log.error("Error scheduling failure detection task");
            err.printStackTrace();
        }
    }

    /**
     * Wait until we receive a message with the layout endpoint to bootstrap the management server.
     */
    private Layout waitForBootstrap() {
        Layout currentLayout = null;
        while (currentLayout == null) {
            try {
                Thread.sleep(1000);
                currentLayout = layoutServer.getCurrentLayout();
            } catch (InterruptedException err) {
                log.warn("Thread interrupted.");
                if (isShutdown) {
                    return null;
                }
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
            if (bootstrapLayout == null || isShutdown) {
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

        do {
            corfuRuntime = getCorfuRuntime();
            if (isShutdown) return;
        } while (corfuRuntime == null);


        while (true) {
            try {
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
                    // Failure present. Triggering the handler.
                    triggerHandlerDispatcher(currentLayout, map.keySet());
                }

                Thread.sleep(policyExecuteInterval);

            } catch (InterruptedException err) {
                log.debug("Sleep interrupted.");
                if (isShutdown) {
                    return;
                }
            }
        }
    }

    /**
     * Shuts down the fault detector service.
     */
    public void shutdown() {
        isShutdown = true;

        // Shut down the Corfu Runtime.
        if (corfuRuntime != null) {
            corfuRuntime.isShutdown = true;
            if (corfuRuntime.layout != null) {
                corfuRuntime.layout.cancel(true);
            }
        }

        // Shutting the fault detector.
        schedulerService.shutdownNow();
        while (!schedulerService.isShutdown()) {
            schedulerService.shutdownNow();
        }
    }

    /**
     * Triggers the handler dispatcher.
     *
     * @param layout        Current layout with failed nodes.
     * @param failedServers Set of server nodes to be removed from the layout.
     */
    private void triggerHandlerDispatcher(Layout layout, Set<String> failedServers) {
        //TODO: Code to trigger and handle the failures.
    }
}
