package org.corfudb.infrastructure;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
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

    /**
     * The options map.
     */
    Map<String, Object> opts;

    private ServerContext serverContext;

    private IFailureDetectorPolicy failureDetectorPolicy;
    private LayoutView layoutView = null;
    private CorfuRuntime corfuRuntime = null;

    // Stores this node's address
    private String localEndpoint;

    // Used for initial bootstrap only.
    private LayoutServer layoutServer;

    // Bootstrap Layout
    private Layout bootstrapLayout = null;

    // Flag to be toggled once bootstrapped.
    private static boolean isBootstrapped = false;

    /**
     * A scheduler, which is used to schedule failure detection.
     */
    private final ExecutorService schedulerService =
            Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("Config-Mgr-%d")
                            .build());

    public ManagementServer(ServerContext serverContext, IFailureDetectorPolicy failureDetectorPolicy,
                            LayoutServer layoutServer) {

        this.opts = serverContext.getServerConfig();
        this.serverContext = serverContext;
        this.layoutServer = layoutServer;

        this.failureDetectorPolicy = failureDetectorPolicy;
        this.localEndpoint = getLocalEndpoint();

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
    private void waitForBootstrap() {
        Layout currentLayout = layoutServer.getCurrentLayout();
        while (currentLayout == null) {
            try {
                Thread.sleep(1000);
                currentLayout = layoutServer.getCurrentLayout();
            } catch (InterruptedException err) {
                log.warn("Thread interrupted.");
            }
        }
        bootstrapLayout = currentLayout;
        isBootstrapped = true;
        log.info("Node bootstrapped. Layout received by management server.");
    }

    /**
     * Returns a connected instance of the CorfuRuntime.
     *
     * @return
     */
    private synchronized CorfuRuntime getCorfuRuntime() {

        // If not bootstrapped we cannot return the CorfuRuntime so we wait.
        if (!isBootstrapped) {
            waitForBootstrap();
        }
        if (corfuRuntime == null) {
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

        CorfuRuntime corfuRuntime = getCorfuRuntime();


        while (true) {

            corfuRuntime.invalidateLayout();

            try {
                // Fetch the latest layout view through the runtime.
                layoutView = corfuRuntime.getLayoutView();
                currentLayout = layoutView.getCurrentLayout();
                //TODO: Store this layout on disk for persistence.

                // Execute the failure detection policy once.
                failureDetectorPolicy.executePolicy(currentLayout, corfuRuntime);

                // Get the server status from the policy and check for failures.
                HashMap<String, Boolean> map = failureDetectorPolicy.getServerStatus();
                if (!map.isEmpty()) {
                    // Failure found. Triggering the handler.
                    triggerHandlerDispatcher(currentLayout, map.keySet());
                }

                Thread.sleep(1000);

            } catch (InterruptedException err) {
                log.debug("Sleep interrupted.");
            }
        }
    }

    /**
     * Triggers the handler dispatcher.
     *
     * @param layout  Current layout with failed nodes.
     * @param servers Set of server nodes to be removed from the layout.
     */
    private void triggerHandlerDispatcher(Layout layout, Set<String> servers) {
        FailureHandlerDispatcher failureHandlerDispatcher = new FailureHandlerDispatcher();
        failureHandlerDispatcher.dispatchHandler(corfuRuntime, layout, servers);
    }
}
