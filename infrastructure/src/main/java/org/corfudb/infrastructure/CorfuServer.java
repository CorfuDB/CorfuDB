package org.corfudb.infrastructure;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.joran.spi.JoranException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.corfudb.common.config.ConfigParamsHelper;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
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
import java.util.concurrent.TimeUnit;

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
    /**
     * This string defines the command line arguments,
     * in the docopt DSL (see http://docopt.org) for the executable.
     * It also serves as the documentation for the executable.
     *
     * <p>Unfortunately, Java doesn't support multi-line string literals,
     * so you must concatenate strings and terminate with newlines.
     *
     * <p>Note that the java implementation of docopt has a strange requirement
     * that each option must be preceded with a space.
     */
    private static final String USAGE =
            "Corfu Server, the server for the Corfu Infrastructure.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tcorfu_server (-l <path>|-m) [-nsNA] [-a <address>|-q <interface-name>] "
                    + "[--network-interface-version=<interface-version>] "
                    + "[--max-replication-data-message-size=<msg-size>] "
                    + "[-c <ratio>] [-d <level>] [-p <seconds>] "
                    + "[--lrCacheSize=<cache-num-entries>]"
                    + "[--plugin=<plugin-config-file-path>]"
                    + "[--base-server-threads=<base_server_threads>] "
                    + "[--log-size-quota-percentage=<max_log_size_percentage>]"
                    + "[--reserved-space-bytes=<reserved_space_bytes>]"
                    + "[--logunit-threads=<logunit_threads>] [--management-server-threads=<management_server_threads>]"
                    + "[-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] "
                    + "[-b] [-g -o <username_file> -j <password_file>] "
                    + "[-k <seqcache>] [-T <threads>] [-B <size>] [-i <channel-implementation>] "
                    + "[-H <seconds>] [-I <cluster-id>] [-x <ciphers>] [-z <tls-protocols>]] "
                    + "[--disable-cert-expiry-check-file=<file_path>]"
                    + "[--metrics]"
                    + "[--health-port=<health_port>]"
                    + "[--snapshot-batch=<batch-size>] [--lock-lease=<lease-duration>]"
                    + "[--max-snapshot-entries-applied=<max-snapshot-entries-applied>]"
                    + "[-P <prefix>] [-R <retention>] <port>"
                    + "[--compaction-trigger-freq-ms=<compaction_trigger_freq_ms>]"
                    + "[--compactor-script=<compactor_script_path>]"
                    + "[--compactor-config=<compactor_config_path>]"
                    + "[--run-compactor-as-root]\n"
                    + "\n"
                    + "Options:\n"
                    + " -l <path>, --log-path=<path>                                             "
                    + "              Set the path to the storage file for the log unit.\n        "
                    + " -s, --single                                                             "
                    + "              Deploy a single-node configuration.\n"
                    + " -I <cluster-id>, --cluster-id=<cluster-id>"
                    + "              For a single node configuration the cluster id to use in UUID,"
                    + "              base64 format, or auto to randomly generate [default: auto].\n"
                    + " -T <threads>, --Threads=<threads>                                        "
                    + "              Number of corfu server worker threads, or 0 to use 2x the "
                    + "              number of available processors [default: 4].\n"
                    + " -P <prefix> --Prefix=<prefix>"
                    + "              The prefix to use for threads (useful for debugging multiple"
                    + "              servers) [default: ]."
                    + "                                                                          "
                    + "              The server will be bootstrapped with a simple one-unit layout."
                    + "\n -a <address>, --address=<address>                                      "
                    + "                IP address for the server router to bind to and to "
                    + "advertise to external clients.\n"
                    + " -q <interface-name>, --network-interface=<interface-name>                "
                    + "              The name of the network interface.\n"
                    + " --network-interface-version=<interface-version>                "
                    + "              The version of the network interface, IPv4 or IPv6(default).\n"
                    + " -i <channel-implementation>, --implementation <channel-implementation>   "
                    + "              The type of channel to use (auto, nio, epoll, kqueue)"
                    + "[default: nio].\n"
                    + " -m, --memory                                                             "
                    + "              Run the unit in-memory (non-persistent).\n"
                    + "              Data will be lost when the server exits!\n"
                    + " -c <ratio>, --cache-heap-ratio=<ratio>                                   "
                    + "              The ratio of jvm max heap size we will use for the the "
                    + "in-memory cache to serve requests from -\n"
                    + "                                                                          "
                    + "              (e.g. ratio = 0.5 means the cache size will be 0.5 * jvm max "
                    + "heap size\n"
                    + "                                                                          "
                    + "              If there is no log, then this will be the size of the log unit"
                    + "\n                                                                        "
                    + "                evicted entries will be auto-trimmed. [default: 0.5].\n"
                    + " -H <seconds>, --HandshakeTimeout=<seconds>                               "
                    + "              Handshake timeout in seconds [default: 10].\n               "
                    + "                                                                          "
                    + "              from the log. [default: -1].\n                              "
                    + "                                                                          "
                    + " -k <seqcache>, --sequencer-cache-size=<seqcache>                         "
                    + "               The size of the sequencer's cache. [default: 250000].\n    "
                    + " -B <size> --batch-size=<size>                                            "
                    + "              The read/write batch size used for data transfer operations [default: 100].\n"
                    + " -R <retention>, --metadata-retention=<retention>                         "
                    + "              Maximum number of system reconfigurations (i.e. layouts)    "
                    + "retained for debugging purposes [default: 1000].\n"
                    + " -p <seconds>, --compact=<seconds>                                        "
                    + "              The rate the log unit should compact entries (find the,\n"
                    + "                                                                          "
                    + "              contiguous tail) in seconds [default: 60].\n"
                    + " -d <level>, --log-level=<level>                                          "
                    + "              Set the logging level, valid levels are: \n"
                    + "                                                                          "
                    + "              ALL,ERROR,WARN,INFO,DEBUG,TRACE,OFF [default: INFO].\n"
                    + " -N, --no-sync                                                            "
                    + "              Disable syncing writes to secondary storage.\n"
                    + " -A, --no-auto-commit                                                     "
                    + "              Disable auto log commit.\n"
                    + " -e, --enable-tls                                                         "
                    + "              Enable TLS.\n"
                    + " -u <keystore>, --keystore=<keystore>                                     "
                    + "              Path to the key store.\n"
                    + " -f <keystore_password_file>, "
                    + "--keystore-password-file=<keystore_password_file>         Path to the file "
                    + "containing the key store password.\n"
                    + " -b, --enable-tls-mutual-auth                                             "
                    + "              Enable TLS mutual authentication.\n"
                    + " -r <truststore>, --truststore=<truststore>                               "
                    + "              Path to the trust store.\n"
                    + " -w <truststore_password_file>, "
                    + "--truststore-password-file=<truststore_password_file>   Path to the file "
                    + "containing the trust store password.\n"
                    + " -g, --enable-sasl-plain-text-auth                                        "
                    + "              Enable SASL Plain Text Authentication.\n"
                    + " -o <username_file>, --sasl-plain-text-username-file=<username_file>      "
                    + "              Path to the file containing the username for SASL Plain Text "
                    + "Authentication.\n"
                    + " -j <password_file>, --sasl-plain-text-password-file=<password_file>      "
                    + "              Path to the file containing the password for SASL Plain Text "
                    + "Authentication.\n"
                    + " -x <ciphers>, --tls-ciphers=<ciphers>                                    "
                    + "              Comma separated list of TLS ciphers to use.\n"
                    + "                                                                          "
                    + "              [default: " + ConfigParamsHelper.getTlsCiphersCSV() + "].\n"
                    + " -z <tls-protocols>, --tls-protocols=<tls-protocols>                      "
                    + "              Comma separated list of TLS protocols to use.\n"
                    + "                                                                          "
                    + "              [default: TLSv1.1,TLSv1.2].\n"
                    + " --disable-cert-expiry-check-file=<file_path>                             "
                    + "              Path to Disable Cert Expiry Check File. If this file is     "
                    + "              present, the certificate expiry checks are disabled.\n      "
                    + " --base-server-threads=<base_server_threads>                              "
                    + "              Number of threads dedicated for the base server [default: 1].\n"
                    + " --log-size-quota-percentage=<max_log_size_percentage>                    "
                    + "              The max size as percentage of underlying file-store size.\n "
                    + "              If this limit is exceeded "
                    + "              write requests will be rejected [default: 100.0].\n         "
                    + " --reserved-space-bytes=<reserved_space_bytes>                              "
                    + "              The reserved space that does not belong to                  "
                    + "              corfu's use [default: 0].\n                               "
                    + "                                                                          "
                    + " --compaction-trigger-freq-ms=<compaction_trigger_freq_ms>                "
                    + "               Frequency at which data will be trimmed & checkpointed\n   "
                    + " --compactor-script=<compactor_script_path>                               "
                    + "               Path to spawn the external corfu store compactor script\n  "
                    + "                                                                          "
                    + " --compactor-config=<compactor_config_path>                               "
                    + "               Path containing the external corfu store compactor config\n"
                    + "                                                                          "
                    + " --run-compactor-as-root                                               "
                    + "               To execute the compactor runner as a root user           \n"
                    + "                                                                          "
                    + " --management-server-threads=<management_server_threads>                  "
                    + "              Number of threads dedicated for the management server [default: 4].\n"
                    + "                                                                          "
                    + " --logunit-threads=<logunit_threads>                  "
                    + "              Number of threads dedicated for the logunit server [default: 4].\n"
                    + " --metrics                                                                "
                    + "              Enable metrics provider.\n                                  "
                    + " --health-port=<health_port>                                              "
                    + "              Enable health api and bind to this port.\n                  "
                    + " --snapshot-batch=<batch-size>                                            "
                    + "              Snapshot (Full) Sync batch size (number of entries)\n       "
                    + " --lrCacheSize=<cache-num-entries>"
                    + "              LR's cache max number of entries.\n                              "
                    + " --max-replication-data-message-size=<msg-size>                                       "
                    + "              The max size of replication data message in bytes.\n   "
                    + " --lock-lease=<lease-duration>                                            "
                    + "              Lock lease duration in seconds\n                            "
                    + " --max-snapshot-entries-applied=<max-snapshot-entries-applied>            "
                    + "              Max number of entries applied in a snapshot transaction.  50 by default."
                    + "              For special tables only\n.                                  "
                    + " -h, --help                                                               "
                    + "              Show this screen\n"
                    + " --version                                                                "
                    + "              Show version\n";

    private static final Duration TIMEOUT = Duration.ofSeconds(3);

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
            Map<String, Object> opts = new Docopt(USAGE)
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
                Optional.ofNullable(context.exists(DEFAULT_METRICS_LOGGER_NAME))
                        .ifPresent(logger -> MeterRegistryProvider.MeterRegistryInitializer
                                .initServerMetrics(logger, DEFAULT_METRICS_LOGGING_INTERVAL, localEndpoint));
            } catch (IllegalStateException ise) {
                log.warn("Registry has been previously initialized. Skipping.");
            }

        }
    }

    public static void configureHealthMonitor(Map<String, Object> opts) {
        if (opts.get("--health-port") != null) {
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
            ServerContext serverContext;
            try {
                serverContext = new ServerContext(opts);
            } catch (DataCorruptionException ex) {
                log.error("Failed creating server context", ex);
                TimeUnit.SECONDS.sleep(TIMEOUT.getSeconds());
                continue;
            }

            try {
                configureMetrics(opts, serverContext.getLocalEndpoint());
                configureHealthMonitor(opts);
                activeServer = new CorfuServerNode(serverContext);
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