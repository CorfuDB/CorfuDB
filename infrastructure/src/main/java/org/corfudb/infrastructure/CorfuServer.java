package org.corfudb.infrastructure;

import static org.corfudb.util.NetworkUtils.getAddressFromInterfaceName;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.joran.spi.JoranException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.corfudb.common.metrics.MetricsServer;
import org.corfudb.common.metrics.servers.PrometheusMetricsServer;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;


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
            startServer(args);
        } catch (Throwable err) {
            log.error("Exit. Unrecoverable error", err);
            throw err;
        }
    }

    private static void startServer(String[] args) {
        // Parse the options given, using docopt.
        Map<String, Object> opts = new Docopt(CorfuServerCmdLine.USAGE)
                .withVersion(GitRepositoryState.getRepositoryState().describe)
                .parse(args);
        // Print a nice welcome message.
        printStartupMsg(opts);
        configureLogger(opts);

        log.debug("Started with arguments: {}", opts);

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

        createServiceDirectory(opts);

        // Check the specified number of datastore files to retain
        if (Integer.parseInt((String) opts.get("--metadata-retention")) < 1) {
            throw new IllegalArgumentException("Max number of metadata files to retain must be greater than 0.");
        }

        // Manages the lifecycle of the Corfu Server.
        while (!shutdownServer) {
            final ServerContext serverContext = new ServerContext(opts);
            try {
                setupMetrics(opts);
                CorfuServerNode node = new CorfuServerNode(serverContext);
                activeServer = node;

                // Register shutdown handler
                Thread shutdownThread = new Thread(() -> cleanShutdown(node));
                shutdownThread.setName("ShutdownThread");
                Runtime.getRuntime().addShutdownHook(shutdownThread);

                activeServer.startAndListen();
            } catch (Throwable th) {
                log.error("CorfuServer: Server exiting due to unrecoverable error: ", th);
                System.exit(EXIT_ERROR_CODE);
            }

            if (cleanupServer) {
                clearDataFiles(serverContext);
                cleanupServer = false;
            }

            if (!shutdownServer) {
                log.info("main: Server restarting.");
            }
        }

        log.info("main: Server exiting due to shutdown");
    }

    /**
     * Setup logback logger
     * - pick the correct logging level before outputting error messages
     * - add serverEndpoint information
     *
     * @param opts command line parameters
     * @throws JoranException logback exception
     */
    private static void configureLogger(Map<String, Object> opts) {
        final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        final Level level = Level.toLevel(((String) opts.get("--log-level")).toUpperCase());
        root.setLevel(level);
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

    /**
     * Clear all data files to reset the server.
     *
     * @param serverContext Server context.
     */
    private static void clearDataFiles(ServerContext serverContext) {
        log.warn("main: cleanup requested, DELETE server data files");
        if (!serverContext.getServerConfig(Boolean.class, "--memory")) {
            File serviceDir = new File(serverContext.getServerConfig(String.class, "--log-path"));
            try {
                FileUtils.cleanDirectory(serviceDir);
            } catch (IOException ioe) {
                throw new UnrecoverableCorfuError(ioe);
            }
        }
        log.warn("main: cleanup completed, expect clean startup");
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

        // Flush the async appender before exiting to prevent the loss of logs
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.stop();
    }

    /**
     * Print the corfu logo.
     */
    private static void printLogo() {
        println("▄████████  ▄██████▄     ▄████████    ▄████████ ███    █▄");
        println("███    ███ ███    ███   ███    ███   ███    ██████    ███");
        println("███    █▀  ███    ███   ███    ███   ███    █▀ ███    ███");
        println("███        ███    ███  ▄███▄▄▄▄██▀  ▄███▄▄▄    ███    ███");
        println("███        ███    ███ ▀▀███▀▀▀▀▀   ▀▀███▀▀▀    ███    ███");
        println("███    █▄  ███    ███ ▀███████████   ███       ███    ███");
        println("███    ███ ███    ███   ███    ███   ███       ███    ███");
        println("████████▀   ▀██████▀    ███    ███   ███       ████████▀ ");
        println("                        ███    ███");
    }

    /**
     * Print an object to the console, followed by a newline.
     * Call this method instead of calling System.out.println().
     *
     * @param line The object to print.
     */
    @SuppressWarnings("checkstyle:printLine")
    private static void println(Object line) {
        System.out.println(line);
        log.info(line.toString());
    }

    /**
     * Print the welcome message, logo and the arguments.
     *
     * @param opts Arguments.
     */
    private static void printStartupMsg(Map<String, Object> opts) {
        printLogo();
        println("Welcome to CORFU SERVER");
        println("Version (" + GitRepositoryState.getRepositoryState().commitIdAbbrev + ")");

        final int port = Integer.parseInt((String) opts.get("<port>"));
        final String dataLocation = (Boolean) opts.get("--memory") ? "MEMORY mode" :
                opts.get("--log-path").toString();

        println("Serving on port " + port);
        println("Data location: " + dataLocation);
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
