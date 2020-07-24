package org.corfudb.infrastructure;


import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.MetricsServer;
import org.corfudb.common.metrics.servers.PrometheusMetricsServer;
import org.corfudb.common.util.LoggerUtil;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.Utils;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.corfudb.util.NetworkUtils.getAddressFromInterfaceName;


/**
 * This is the new Corfu server single-process executable.
 *
 * <p>The command line options are documented in the USAGE variable.
 *
 * <p>Created by mwei on 11/30/15.
 */

@Slf4j
public class CorfuServer {

    // Active Corfu Server.
    private static volatile CorfuServerNode activeServer;

    // Flag if set to true - causes the Corfu Server to shutdown.
    private static volatile boolean shutdownServer = false;
    // If set to true - triggers a reset of the server by wiping off all the data.
    private static volatile boolean cleanupServer = false;
    // Error code required to detect an ungraceful shutdown.
    private static final int EXIT_ERROR_CODE = 100;

    /**
     * Main program entry point.
     *
     * @param args command line argument strings
     */
    public static void main(String[] args) {
        try {
            CorfuServerCmdLine cmdLine = new CorfuServerCmdLine(args);
            Map<String, Object> opts = cmdLine.getOpts();
            cmdLine.printStartupMsg();

            String logLevel = ((String) opts.get("--log-level")).toUpperCase();
            LoggerUtil.configureLogger(logLevel);

            log.debug("Started with arguments: {}", opts);

            setupNetwork(opts);
            createServiceDirectory(opts);
            validateMetadataRetention(opts);
            startCorfuServer(opts);
        } catch (Throwable err) {
            log.error("Exit. Unrecoverable error", err);
            throw err;
        }
    }

    /**
     * Manages the lifecycle of the Corfu Server.
     * @param opts cmd line params
     */
    private static void startCorfuServer(Map<String, Object> opts) {

        while (!shutdownServer) {
            ServerContext serverContext = new ServerContext(opts);
            try {
                setupMetrics(opts);
                CorfuServerNode node = new CorfuServerNode(serverContext);
                activeServer = node;
                setupShutdownHook(node);
                node.startAndListen();
            } catch (Throwable th) {
                log.error("CorfuServer: Server exiting due to unrecoverable error: ", th);
                System.exit(EXIT_ERROR_CODE);
            }

            File logPath = new File(serverContext.getServerConfig(String.class, "--log-path"));

            if (cleanupServer && !serverContext.getServerConfig(Boolean.class, "--memory")) {
                Utils.clearDataFiles(logPath);
                cleanupServer = false;
            }

            if (!shutdownServer) {
                log.info("main: Server restarting.");
            }
        }

        log.info("main: Server exiting due to shutdown");
    }

    /**
     * Cleanly shuts down the server and restarts.
     *
     * @param resetData Resets and clears all data if True.
     */
    static void restartServer(boolean resetData) {
        if (resetData) {
            cleanupServer = true;
        }

        log.info("RestartServer: Shutting down corfu server");
        activeServer.close();
        log.info("RestartServer: Starting corfu server");
    }

    /**
     * Attempt to cleanly shutdown all the servers.
     */
    private static void cleanShutdown(CorfuServerNode corfuServer) {
        log.info("CleanShutdown: Starting Cleanup.");
        shutdownServer = true;
        try {
            corfuServer.close();
        } catch (Throwable th) {
            log.error("cleanShutdown: failed during shutdown", th);
        }
        LoggerUtil.stopLogger();
    }

    /**
     * Create the service directory if it does not exist.
     *
     * @param opts Server options map.
     */
    private static void createServiceDirectory(Map<String, Object> opts) {
        if ((Boolean) opts.get("--memory")) {
            return;
        }

        Path serviceDir = Paths.get((String) opts.get("--log-path"));

        if (!serviceDir.toFile().isDirectory()) {
            log.error("Service directory {} does not point to a directory. Aborting.",
                    serviceDir);
            throw new UnrecoverableCorfuError("Service directory must be a directory!");
        }

        Path corfuServiceDirPath = serviceDir.toAbsolutePath().resolve("corfu");
        File corfuServiceDir = corfuServiceDirPath.toFile();
        // Update the new path with the dedicated child service directory.
        opts.put("--log-path", corfuServiceDirPath.toString());
        if (!corfuServiceDir.exists() && corfuServiceDir.mkdirs()) {
            log.info("Created new service directory at {}.", corfuServiceDir);
        }
    }

    private static void setupNetwork(Map<String, Object> opts) {
        // Bind to all interfaces only if no address or interface specified by the user.
        // Fetch the address if given a network interface.
        if (opts.get("--network-interface") != null) {
            opts.put("--address", getAddressFromInterfaceName((String) opts.get("--network-interface")));
            opts.put("--bind-to-all-interfaces", false);
        } else if (opts.get("--address") == null) {
            // Default the address to localhost and set the bind to all interfaces flag to true,
            // if the address and interface is not specified.
            opts.put("--bind-to-all-interfaces", true);
            opts.put("--address", "localhost");
        } else {
            // Address is specified by the user.
            opts.put("--bind-to-all-interfaces", false);
        }
    }

    /**
     *  Register shutdown handler
     * @param node corfu server node
     */
    private static void setupShutdownHook(CorfuServerNode node) {
        Thread shutdownThread = new Thread(() -> cleanShutdown(node));
        shutdownThread.setName("ShutdownThread");
        Runtime.getRuntime().addShutdownHook(shutdownThread);
    }

    private static void validateMetadataRetention(Map<String, Object> opts) {
        // Check the specified number of datastore files to retain
        if (Integer.parseInt((String) opts.get("--metadata-retention")) < 1) {
            throw new IllegalArgumentException("Max number of metadata files to retain must be greater than 0.");
        }
    }

    /**
     * Generate metrics server config and start server.
     *
     * @param opts Command line parameters.
     */
    private static void setupMetrics(Map<String, Object> opts) {
        PrometheusMetricsServer.Config config = PrometheusMetricsServer.Config.parse(opts);
        MetricsServer server = new PrometheusMetricsServer(config, ServerContext.getMetrics());
        server.start();
    }
}
