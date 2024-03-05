package org.corfudb.infrastructure;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.joran.spi.JoranException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.corfudb.common.util.URLUtils.NetworkInterfaceVersion;
import org.corfudb.infrastructure.health.HealthMonitor;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuInterClusterReplicationServer;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.corfudb.common.metrics.micrometer.MeterRegistryProvider.MeterRegistryInitializer.initServerMetrics;
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
    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    private static volatile CountDownLatch resetLatch;
    // Active Corfu Server.
    private static volatile CorfuServerNode activeServer;

    // Flag if set to true - causes the Corfu Server to shutdown.
    private static volatile boolean shutdownServer = false;
    // If set to true - triggers a reset of the server by wiping off all the data.
    private static volatile boolean cleanupServer = false;
    // Error code required to detect an ungraceful shutdown.
    private static final int EXIT_ERROR_CODE = 100;

    private static final String DEFAULT_METRICS_LOGGER_NAME = "org.corfudb.metricsdata";

    private static final Duration DEFAULT_METRICS_LOGGING_INTERVAL = Duration.ofMinutes(1);

    // Declaring strings to avoid code duplication code analysis error
    private static final String ADDRESS_PARAM = "--address";
    private static final String NETWORK_INTERFACE_VERSION_PARAM = "--network-interface-version";

    /**
     * Main program entry point.
     *
     * @param args command line argument strings
     */
    public static void main(String[] args) throws Exception {
        try {
            // Parse the options given, using docopt.
            Map<String, Object> opts = new Docopt(CorfuServerCmdLine.USAGE)
                    .withVersion(GitRepositoryState.getRepositoryState().describe)
                    .parse(args);

            // Note: this is a temporal solution for license reuse.

            // We currently identify the Log Replication Server by the use of a specific
            // flag used for the specification of the LR plugin configuration.
            // In the future, log replication will not run as a separate process
            // but it will be an aggregated functionality of Corfu's Server so this will
            // be removed.
            if (opts.containsKey("--plugin") && opts.get("--plugin") != null) {
                CorfuInterClusterReplicationServer.main(args);
            } else {
                startServer(opts);
            }
        } catch (Throwable err) {
            log.error("Exit. Unrecoverable error", err);
            throw err;
        }
    }

    public static void configureMetrics(Map<String, Object> opts, String localEndpoint) {
        if ((boolean) opts.get("--metrics")) {
            try {
                LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
                Optional<Logger> maybeLogger = Optional.ofNullable(context.exists(DEFAULT_METRICS_LOGGER_NAME));
                maybeLogger.ifPresent(logger ->
                        initServerMetrics(logger, DEFAULT_METRICS_LOGGING_INTERVAL, localEndpoint)
                );
            } catch (IllegalStateException ise) {
                log.warn("Registry has been previously initialized. Skipping.");
            }

        }
    }

    public static void configureHealthMonitor(Map<String, Object> opts) {
        if (opts.get(CorfuServerCmdLine.HEALTH_PORT_PARAM) != null) {
            log.info("Starting health monitor");
            HealthMonitor.init();
        }
    }

    private static void startServer(Map<String, Object> opts) throws InterruptedException {

        // Print a nice welcome message.
        printStartupMsg(opts);
        configureLogger(opts);

        log.debug("Started with arguments: {}", opts);

        // Bind to all interfaces only if no address or interface specified by the user.
        // Fetch the address if given a network interface.
        configureNetwork(opts);

        log.info("Configured Corfu Server address: {}", opts.get(ADDRESS_PARAM));
        createServiceDirectory(opts);
        checkMetaDataRetention(opts);
        registerShutdownHandler();

        // Manages the lifecycle of the Corfu Server.
        while (!shutdownServer) {
            resetLatch = new CountDownLatch(1);
            ServerContext serverContext;
            try {
                serverContext = new ServerContext(opts);
                configureMetrics(opts, serverContext.getLocalEndpoint());
                configureHealthMonitor(opts);
                activeServer = new CorfuServerNode(serverContext);
            } catch (DataCorruptionException ex) {
                log.error("Failed starting server", ex);
                TimeUnit.SECONDS.sleep(TIMEOUT.getSeconds());
                continue;
            }

            try {
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
                log.info("main: Waiting until restart is complete.");
                resetLatch.await();
                log.info("main: Server restarted.");
            }
        }

        log.info("main: Server exiting due to shutdown");
    }

    private static void registerShutdownHandler() {
        // Register shutdown handler
        Thread shutdownThread = new Thread(CorfuServer::cleanShutdown);
        shutdownThread.setName("ShutdownThread");
        Runtime.getRuntime().addShutdownHook(shutdownThread);
    }

    private static void checkMetaDataRetention(Map<String, Object> opts) {
        // Check the specified number of datastore files to retain
        if (Integer.parseInt((String) opts.get("--metadata-retention")) < 1) {
            throw new IllegalArgumentException("Max number of metadata files to retain must be greater than 0.");
        }
    }

    private static void configureNetwork(Map<String, Object> opts) {
        if (opts.get("--network-interface") != null) {
            opts.put(
                    ADDRESS_PARAM,
                    getAddressFromInterfaceName(
                            (String) opts.get("--network-interface"),
                            (opts.get(NETWORK_INTERFACE_VERSION_PARAM) != null) ?
                                    NetworkInterfaceVersion.valueOf(((String) opts.get(NETWORK_INTERFACE_VERSION_PARAM)).toUpperCase()):
                                    NetworkInterfaceVersion.IPV6 // Default is IPV6
                    )
            );
            opts.put("--bind-to-all-interfaces", false);
        } else if (opts.get(ADDRESS_PARAM) == null) {
            // If the address and interface is not specified,
            // pick an address from eth0 interface and set the bind to all interfaces flag to true.
            opts.put("--bind-to-all-interfaces", true);
            opts.put(ADDRESS_PARAM,
                    getAddressFromInterfaceName(
                            "eth0",
                            (opts.get(NETWORK_INTERFACE_VERSION_PARAM) != null) ?
                                    NetworkInterfaceVersion.valueOf(((String) opts.get(NETWORK_INTERFACE_VERSION_PARAM)).toUpperCase()):
                                    NetworkInterfaceVersion.IPV6 // Default is IPV6
                    )
            );
        } else {
            // Address is specified by the user.
            opts.put("--bind-to-all-interfaces", false);
        }
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
    private static void createServiceDirectory(Map<String, Object> opts) throws InterruptedException {
        if ((Boolean) opts.get("--memory")) {
            return;
        }

        Optional<File> maybeServiceDir = Optional.ofNullable(opts.get("--log-path"))
                .map(Object::toString)
                .map(File::new);

        if (!maybeServiceDir.isPresent()) {
            throw new IllegalArgumentException("--log-path parameter is not set");
        }

        File serviceDir = maybeServiceDir.get();

        if (!serviceDir.isDirectory()) {
            log.error("Service directory {} does not point to a directory. Aborting.",
                    serviceDir);
            throw new UnrecoverableCorfuError("Service directory must be a directory!");
        }

        String corfuServiceDirPath = serviceDir.getAbsolutePath()
                + File.separator
                + "corfu";
        // Update the new path with the dedicated child service directory.
        opts.put("--log-path", corfuServiceDirPath);

        while (true) {
            File corfuServiceDir = new File(corfuServiceDirPath);

            if (corfuServiceDir.exists()) {
                log.info("Service directory at {} is present.", corfuServiceDir);
                break;
            }

            if (corfuServiceDir.mkdirs()) {
                log.info("Created new service directory at {}.", corfuServiceDir);
                break;
            }

            log.error("New service directory at {} couldn't be created.", corfuServiceDir);
            TimeUnit.SECONDS.sleep(TIMEOUT.getSeconds());
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
        resetLatch.countDown();
        log.info("RestartServer: Starting corfu server");
    }

    /**
     * Attempt to cleanly shutdown all the servers.
     */
    private static void cleanShutdown() {
        log.info("CleanShutdown: Starting Cleanup.");
        shutdownServer = true;
        try {
            CorfuServerNode current = activeServer;
            if (current != null) {
                current.close();
            }
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
}