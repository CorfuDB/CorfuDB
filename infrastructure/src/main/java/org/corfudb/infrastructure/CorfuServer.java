package org.corfudb.infrastructure;


import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.core.joran.spi.JoranException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.corfudb.common.metrics.MetricsServer;
import org.corfudb.common.metrics.servers.PrometheusMetricsServer;
import org.corfudb.infrastructure.configuration.ServerConfiguration;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.GitRepositoryState;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;


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
    public static void main(String[] args) throws ParseException {
        try {
            startServer(args);
        } catch (Throwable err) {
            log.error("Exit. Unrecoverable error", err);
            throw err;
        }
    }

    private static void startServer(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("l", "log-path", true,
                "Directory where the server outputs its binary logs and metadata");
        options.addOption("m", "memory", false,
                "Start the LogUnit in-memory mode (non-persistent)");
        options.addOption("s", "single", false,
                "Deploy a single-node configuration");
        options.addOption("a", "address", true,
                "address for the server to bind to (default: localhost");
        options.addOption("p", "port", true,
                "port for the server to bind to (default: 9000");
        options.addOption("q", "network-interface", true,
                "Name of the network interface to use");
        options.addOption("c", "cache-heap-ratio", true,
                "The ratio of Xmx to use for the LogUnit cache");
        options.addOption("k", "sequencer-cache-size", true,
                "Size of the sequencer's conflict window");
        options.addOption("d", "log-level", true,
                "Sets the server's logging level (i.e ALL,ERROR,WARN,INFO,DEBUG,TRACE,OFF)");
        options.addOption("e", "enable-tls", false, "Enable TLS");
        options.addOption("b", "enable-tls-mutual-auth", false,
                "Enable TLS mutual authentication");
        options.addOption("u", "keystore", true, "Path to the key store");
        options.addOption("f", "keystore-password-file", true,
                "Path to the file containing the key store password");
        options.addOption("r", "truststore", true, "Path to the trust store");
        options.addOption("w", "truststore-password-file", true,
                "Path to the file containing the trust store password");
        options.addOption("lsq", "log-size-quota-percentage", true,
                "The max size as percentage of underlying file-store size If this limit is exceeded " +
                        "write requests will be rejected");
        options.addOption("mp", "metrics-port", true, "Metrics provider server port ");


        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        ServerConfiguration conf = ServerConfiguration.getServerConfigFromCommandLineArg(cmd);


        // Print a nice welcome message.
        printStartupMsg(conf);
        configureLogger(conf.getLogLevel());

        log.info("Started with arguments: {}", conf);

        createServiceDirectory(conf);

        // Register shutdown handler
        Thread shutdownThread = new Thread(CorfuServer::cleanShutdown);
        shutdownThread.setName("ShutdownThread");
        Runtime.getRuntime().addShutdownHook(shutdownThread);

        // Manages the lifecycle of the Corfu Server.
        while (!shutdownServer) {
            final ServerContext serverContext = new ServerContext(conf);
            try {
                setupMetrics(conf);
                activeServer = new CorfuServerNode(serverContext);
                activeServer.startAndListen();
            } catch (Throwable th) {
                log.error("CorfuServer: Server exiting due to unrecoverable error: ", th);
                System.exit(EXIT_ERROR_CODE);
            }

            if (cleanupServer) {
                clearDataFiles(conf);
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
     * @throws JoranException logback exception
     */
    private static void configureLogger(Level logLevel) {
        final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(logLevel);
    }

    /**
     * Create the service directory if it does not exist.
     *
     * @param conf server configurations
     */
    private static void createServiceDirectory(ServerConfiguration conf) {
        if (conf.isInMemoryMode()) {
            return;
        }

        String corfuServiceDirPath = conf.getServerDir();
        File corfuServiceDir = new File(corfuServiceDirPath);
        // Update the new path with the dedicated child service directory.
        if (!corfuServiceDir.exists() && corfuServiceDir.mkdirs()) {
            log.info("Created new service directory at {}.", corfuServiceDir);
        }
    }

    /**
     * Clear all data files to reset the server.
     *
     * @param conf server configurations
     */
    private static void clearDataFiles(ServerConfiguration conf) {
        log.warn("main: cleanup requested, DELETE server data files");
        if (!conf.isInMemoryMode()) {
            File serviceDir = new File(conf.getServerDir());
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
    private static void cleanShutdown() {
        log.info("CleanShutdown: Starting Cleanup.");
        shutdownServer = true;
        activeServer.close();
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
     */
    private static void printStartupMsg(ServerConfiguration conf) {
        printLogo();
        println("Welcome to CORFU SERVER");
        println("Version (" + GitRepositoryState.getRepositoryState().commitIdAbbrev + ")");
        println("Serving on " + conf.getLocalServerEndpoint());
        println("Data location: " + (conf.isInMemoryMode() ? "MEMORY mode" : conf.getServerDir()));
    }

    /**
     * Generate metrics server config and start server.
     *
     */
    private static void setupMetrics(ServerConfiguration conf) {
        if (conf.getMetricsProviderAddress() != null) {
            MetricsServer server = new PrometheusMetricsServer(conf.getMetricsProviderAddress(),
                    ServerContext.getMetrics());
            server.start();
        }
    }
}
