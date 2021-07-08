package org.corfudb.infrastructure.logreplication.infrastructure;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.joran.spi.JoranException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.configuration.ServerConfiguration;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.CorfuReplicationClusterManagerAdapter;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * This class represents the Corfu Replication Server. This Server will be running on both ends
 * of the cross-cluster replication.
 *
 * A discovery mechanism will enable a cluster as Source (Sender) and another as Sink (Receiver).
 */
@Slf4j
public class CorfuInterClusterReplicationServer implements Runnable {

    /**
     * This string defines the command line arguments,
     * in the docopt DSL (see http://docopt.org) for the executable.
     * It also serves as the documentation for the executable.
     *
     * <p>Note that the java implementation of docopt has a strange requirement
     * that each option must be preceded with a space.
     */
    //TODO(NEIL): consolidate if possible
    private static final String USAGE =
            "Corfu Log Replication Server, the server for replication across clusters.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tcorfu_replication_server (-l <path>|-m) [-nsN] [-a <address>|-q <interface-name>] "
                    + "[--snapshot-batch=<batch-size>] "
                    + "[--max-replication-data-message-size=<msg-size>] "
                    + "[--lock-lease=<lease-duration>]"
                    + "[-c <ratio>] [-d <level>] [-p <seconds>] "
                    + "[--plugin=<plugin-config-file-path>]"
                    + "[--base-server-threads=<base_server_threads>] "
                    + "[--log-size-quota-percentage=<max_log_size_percentage>]"
                    + "[--logunit-threads=<logunit_threads>] [--management-server-threads=<management_server_threads>]"
                    + "[-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] "
                    + "[-b] [-g -o <username_file> -j <password_file>] "
                    + "[-k <seqcache>] [-T <threads>] [-B <size>] [-i <channel-implementation>] "
                    + "[-H <seconds>] [-I <cluster-id>] [-x <ciphers>] [-z <tls-protocols>]] "
                    + "[--metrics]"
                    + "[-P <prefix>] [-R <retention>] <port>\n"
                    + "\tcorfu_replication_server (--config-file=<config-file-path>)\n"
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
                    + " -n, --no-verify                                                          "
                    + "              Disable checksum computation and verification.\n"
                    + " -N, --no-sync                                                            "
                    + "              Disable syncing writes to secondary storage.\n"
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
                    + "              [default: TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256].\n"
                    + " -z <tls-protocols>, --tls-protocols=<tls-protocols>                      "
                    + "              Comma separated list of TLS protocols to use.\n"
                    + "                                                                          "
                    + "              [default: TLSv1.1,TLSv1.2].\n"
                    + " --base-server-threads=<base_server_threads>                              "
                    + "              Number of threads dedicated for the base server [default: 1].\n"
                    + " --log-size-quota-percentage=<max_log_size_percentage>                    "
                    + "              The max size as percentage of underlying file-store size.\n "
                    + "                                                                          "
                    + "              If this limit is exceeded write requests will be rejected [default: 100.0].\n"
                    + " --management-server-threads=<management_server_threads>                  "
                    + "              Number of threads dedicated for the management server [default: 4].\n"
                    + " --logunit-threads=<logunit_threads>                                      "
                    + "              Number of threads dedicated for the logunit server [default: 4].\n"
                    + " --metrics                                                                "
                    + "              Enable metrics provider.\n"
                    + " --snapshot-batch=<batch-size>                                            "
                    + "              Snapshot (Full) Sync batch size (number of entries)\n"
                    + " --max-replication-data-message-size=<msg-size>                           "
                    + "              The max size of replication data message in bytes.\n"
                    + " --lock-lease=<lease-duration>                                            "
                    + "              Lock lease duration in seconds\n"
                    + " --config-file=<config-file-path>                                         "
                    + "              Location of configuration options file. Will override command line options.\n"
                    + " -h, --help                                                               "
                    + "              Show this screen\n"
                    + " --version                                                                "
                    + "              Show version\n";

    // Active Corfu Server Node.
    private volatile CorfuInterClusterReplicationServerNode activeServer;

    // Flag if set to true - causes the Corfu Server to shutdown.
    private volatile boolean shutdownServer = false;

    // If set to true - triggers a reset of the server by wiping off all the data.
    private volatile boolean cleanupServer = false;

    // Error code required to detect an ungraceful shutdown.
    private static final int EXIT_ERROR_CODE = 100;

    private static final Duration DEFAULT_METRICS_LOGGING_INTERVAL_DURATION = Duration.ofMinutes(1);

    private static final String DEFAULT_METRICS_LOGGER_NAME = "LogReplicationMetrics";

    // Getter for testing
    @Getter
    private CorfuReplicationClusterManagerAdapter clusterManagerAdapter;

    @Getter
    private CorfuReplicationDiscoveryService replicationDiscoveryService;

    private final String[] args;

    public static void main(String[] args) {
        CorfuInterClusterReplicationServer corfuReplicationServer = new CorfuInterClusterReplicationServer(args);
        corfuReplicationServer.run();
    }

    public CorfuInterClusterReplicationServer(String[] inputs) {
        this.args = inputs;
    }

    @Override
    public void run() {
        try {
            this.startServer();
        } catch (Throwable err) {
            log.error("Exit. Unrecoverable error", err);
            throw err;
        }
    }

    private void startServer() {
        // Parse the options given, using docopt.
        Map<String, Object> opts = new Docopt(USAGE)
                .withVersion(GitRepositoryState.getRepositoryState().describe)
                .parse(args);

        ServerConfiguration conf;
        if (opts.containsKey("--config-file") && opts.get("--config-file") != null) {
            conf = ServerConfiguration.getServerConfigFromFile((String) opts.get("--config-file"));
        } else {
            conf = ServerConfiguration.getServerConfigFromMap(opts);
        }

        printStartupMsg(conf);
        configureLogger(conf);

        log.info("Started with arguments: {}", conf);

        ServerContext serverContext = getServerContext(conf);

        configureMetrics(conf, serverContext.getLocalEndpoint());

        // Register shutdown handler
        Thread shutdownThread = new Thread(this::cleanShutdown);
        shutdownThread.setName("ShutdownThread");
        Runtime.getRuntime().addShutdownHook(shutdownThread);

        // Manages the lifecycle of the Corfu Log Replication Server.
        while (!shutdownServer) {
            try {
                CompletableFuture<CorfuInterClusterReplicationServerNode> discoveryServiceCallback = startDiscoveryService(serverContext);

                log.info("Wait for Discovery Service to provide a view of the topology...");

                // Block until the replication context is provided by the Discovery service
                activeServer = discoveryServiceCallback.get();

                log.info("Discovery Service completed. Start Log Replication Service...");

                activeServer.startAndListen();
            } catch (Throwable th) {
                log.error("CorfuServer: Server exiting due to unrecoverable error: ", th);
                System.exit(EXIT_ERROR_CODE);
            }

            if (cleanupServer) {
                cleanupServer = false;
            }

            if (!shutdownServer) {
                log.info("main: Server restarting.");
            }
        }

        log.info("main: Server exiting due to shutdown");
        flushAsyncLogAppender();
    }

    private ServerContext getServerContext(ServerConfiguration conf) {
        return new ServerContext(conf);
    }

    /**
     * Start Corfu Log Replication Discovery Service
     *
     * @param serverContext server context (server information)
     * @return completable future for discovered topology
     */
    private CompletableFuture<CorfuInterClusterReplicationServerNode> startDiscoveryService(ServerContext serverContext) {

        log.info("Start Discovery Service.");
        CompletableFuture<CorfuInterClusterReplicationServerNode> discoveryServiceCallback = new CompletableFuture<>();

        this.clusterManagerAdapter = buildClusterManagerAdapter(serverContext.getPluginConfigFilePath());

        // Start LogReplicationDiscovery Service, responsible for
        // acquiring lock, retrieving Site Manager Info and processing this info
        // so this node is initialized as Source (sender) or Sink (receiver)
        replicationDiscoveryService = new CorfuReplicationDiscoveryService(serverContext,
                clusterManagerAdapter, discoveryServiceCallback);

        Thread replicationDiscoveryThread = new Thread(replicationDiscoveryService, "discovery-service");
        replicationDiscoveryThread.start();

        return discoveryServiceCallback;
    }

    /**
     * Setup logback logger
     * - pick the correct logging level before outputting error messages
     * - add serverEndpoint information
     *
     * @param conf Server Configuration Options
     * @throws JoranException logback exception
     */
    private static void configureLogger(ServerConfiguration conf) {
        final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        final Level level = conf.getLogLevel();
        root.setLevel(level);
    }

    public static void configureMetrics(ServerConfiguration conf, String localEndpoint) {
        if (conf.isMetricsEnabled()) {
            try {
                LoggerContext context =  (LoggerContext) LoggerFactory.getILoggerFactory();
                Optional.ofNullable(context.exists(DEFAULT_METRICS_LOGGER_NAME))
                        .ifPresent(logger -> MeterRegistryProvider.MeterRegistryInitializer.init(logger,
                                DEFAULT_METRICS_LOGGING_INTERVAL_DURATION, localEndpoint));
            }
            catch (IllegalStateException ise) {
                log.warn("Registry has been previously initialized. Skipping.");
            }
        }
    }

    /**
     * Attempt to cleanly shutdown all the servers.
     */
    public void cleanShutdown() {
        log.info("CleanShutdown: Starting Cleanup.");
        shutdownServer = true;
        if (activeServer != null) {
            activeServer.close();
        }
        if (replicationDiscoveryService != null) {
            replicationDiscoveryService.shutdown();
        }
        flushAsyncLogAppender();
    }

    /**
     * Flush the logs of the async log appender. Useful call to make during clean shutdown
     * to ensure that all the logs are indeed flushed out to disk for debugging.
     */
    public static void flushAsyncLogAppender() {
        // Flush the async appender before exiting to prevent the loss of logs
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.stop();
    }

    /**
     * Print the corfu logo.
     */
    private static void printLogo() {
        println(" _                  ____            _ _           _   _             \n" +
                    " | |    ___   __ _  |  _ \\ ___ _ __ | (_) ___ __ _| |_(_) ___  _ __  \n" +
                    " | |   / _ \\ / _` | | |_) / _ \\ '_ \\| | |/ __/ _` | __| |/ _ \\| '_ \\ \n" +
                    " | |__| (_) | (_| | |  _ <  __/ |_) | | | (_| (_| | |_| | (_) | | | |\n" +
                    " |_____\\___/ \\__, | |_| \\_\\___| .__/|_|_|\\___\\__,_|\\__|_|\\___/|_| |_|\n" +
                    "             |___/            |_|                                   ");
    }

    /**
     * Print an object to the console.
     *
     * @param line The object to print.
     */
    @SuppressWarnings("checkstyle:printLine")
    private static void println(Object line) {
        System.out.println(line.toString());
        log.info(line.toString());
    }

    /**
     * Print the welcome message, logo and the arguments for Log Replication Server
     *
     * @param conf Server Configuration.
     */
    private static void printStartupMsg(ServerConfiguration conf) {
            printLogo();
            println("");
            println("------------------------------------");
            println("Initializing LOG REPLICATION SERVER");
            println("Version (" + GitRepositoryState.getRepositoryState().commitIdAbbrev + ")");
            final int port = conf.getServerPort();
            println("Serving on port " + port);
            println("------------------------------------");
            println("");
    }

    /**
     * Retrieve Cluster Manager Adapter, i.e., the adapter to external provider of the topology.
     *
     * @return cluster manager adapter instance
     */
    private CorfuReplicationClusterManagerAdapter buildClusterManagerAdapter(String pluginConfigFilePath) {

        LogReplicationPluginConfig config = new LogReplicationPluginConfig(pluginConfigFilePath);
        File jar = new File(config.getTopologyManagerAdapterJARPath());

        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class adapter = Class.forName(config.getTopologyManagerAdapterName(), true, child);
            return (CorfuReplicationClusterManagerAdapter) adapter.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            log.error("Fatal error: Failed to create serverAdapter", e);
            throw new UnrecoverableCorfuError(e);
        }
    }
}
