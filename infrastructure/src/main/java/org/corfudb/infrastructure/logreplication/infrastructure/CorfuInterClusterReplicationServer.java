package org.corfudb.infrastructure.logreplication.infrastructure;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.core.joran.spi.JoranException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.config.ConfigParamNames;
import org.corfudb.common.config.ConfigParamsHelper;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.common.util.URLUtils.NetworkInterfaceVersion;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationUpgradeManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.corfudb.common.util.URLUtils.getHostFromEndpointURL;
import static org.corfudb.common.util.URLUtils.getVersionFormattedEndpointURL;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.util.NetworkUtils.getAddressFromInterfaceName;

/**
 * This class represents the Corfu Replication Server. This Server will be running on both ends
 * of the cross-cluster replication.
 * <p>
 * A discovery mechanism will enable a cluster as Source (Sender) and another as Sink (Receiver).
 */
@Slf4j
public class CorfuInterClusterReplicationServer implements Runnable {

    private static final int SYSTEM_EXIT_ERROR_CODE = -3;

    private static final long DEFAULT_UPGRADE_CHECK_DELAY_MS = 5000;

    /**
     * This string defines the command line arguments,
     * in the docopt DSL (see http://docopt.org) for the executable.
     * It also serves as the documentation for the executable.
     *
     * <p>Note that the java implementation of docopt has a strange requirement
     * that each option must be preceded with a space.
     */
    private static final String USAGE =
            "Corfu Log Replication Server, the server for replication across clusters.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tlog_replication_server (-l <path>|-m) [-nsN] [-a <address>|-q <interface-name>] "
                    + "[--network-interface-version=<interface-version>] "
                    + "[--snapshot-batch=<batch-size>] "
                    + "[--max-replication-data-message-size=<msg-size>] "
                    + "[--max-replication-write-size=<max-replication-write-size>] "
                    + "[--lock-lease=<lease-duration>]"
                    + "[--max-snapshot-entries-applied=<max-snapshot-entries-applied>]"
                    + "[-c <ratio>] [-d <level>] [-p <seconds>] "
                    + "[--lrCacheSize=<cache-num-entries>]"
                    + "[--plugin=<plugin-config-file-path>]"
                    + "[--base-server-threads=<base_server_threads>] "
                    + "[--log-size-quota-percentage=<max_log_size_percentage>]"
                    + "[--logunit-threads=<logunit_threads>] [--management-server-threads=<management_server_threads>]"
                    + "[-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] "
                    + "[-b] [-g -o <username_file> -j <password_file>] "
                    + "[-k <seqcache>] [-T <threads>] [-B <size>] [-i <channel-implementation>] "
                    + "[-H <seconds>] [-I <cluster-id>] [-x <ciphers>] [-z <tls-protocols>]] "
                    + "[--disable-cert-expiry-check-file=<file_path>]"
                    + "[--metrics]"
                    + "[-P <prefix>] [-R <retention>] <port> <corfu-server-port>\n"
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
                    + "                                                                          "
                    + " --management-server-threads=<management_server_threads>                  "
                    + "              Number of threads dedicated for the management server [default: 4].\n"
                    + "                                                                          "
                    + " --logunit-threads=<logunit_threads>                  "
                    + "              Number of threads dedicated for the logunit server [default: 4].\n"
                    + " --metrics                                                                "
                    + "              Enable metrics provider.\n                                  "
                    + " --snapshot-batch=<batch-size>                                            "
                    + "              Snapshot (Full) Sync batch size.\n                          "
                    + "              The max number of messages per batch)\n                     "
                    + " --lrCacheSize=<cache-num-entries>"
                    + "              Cache max number of entries.\n                              "
                    + " --max-replication-data-message-size=<msg-size>                           "
                    + "              Max size of replication data message in bytes. \n   "
                    + " --max-replication-write-size=<max-replication-write-size>"
                    + "              Max size written by the SINK in a single corfu transaction.  Integer.MAX_VALUE by default\n "
                    + "                                                                          "
                    + " --lock-lease=<lease-duration>                                            "
                    + "              Lock lease duration in seconds\n                            "
                    + " --max-snapshot-entries-applied=<max-snapshot-entries-applied>            "
                    + "              Max number of entries applied in a snapshot transaction.  50 by default."
                    + "              For special tables only\n.                                  "
                    + " -h, --help                                                               "
                    + "              Show this screen\n"
                    + " --version                                                                "
                    + "              Show version\n";

    // Error code required to detect an ungraceful shutdown.
    private static final int EXIT_ERROR_CODE = 100;

    private static final Duration DEFAULT_METRICS_LOGGING_INTERVAL_DURATION = Duration.ofMinutes(1);

    private static final String DEFAULT_METRICS_LOGGER_NAME = "LogReplicationMetrics";

    // Declaring strings to avoid code duplication code analysis error
    private static final String ADDRESS_PARAM = "--address";
    private static final String NETWORK_INTERFACE_VERSION_PARAM = "--network-interface-version";

    @Getter
    private CorfuReplicationDiscoveryService replicationDiscoveryService;

    private final String[] args;

    public static void main(String[] args) {
        CorfuInterClusterReplicationServer corfuReplicationServer =
            new CorfuInterClusterReplicationServer(args);
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

        printStartupMsg(opts);
        configureLogger(opts);

        log.info("Started with arguments: {}", opts);

        ServerContext serverContext = getServerContext(opts);

        configureMetrics(opts, serverContext.getLocalEndpoint());

        // Register shutdown handler
        Thread shutdownThread = new Thread(this::cleanShutdown);
        shutdownThread.setName("ShutdownThread");
        Runtime.getRuntime().addShutdownHook(shutdownThread);

        try {
            startDiscoveryService(serverContext);
        } catch (Throwable th) {
            log.error("Corfu Replication Service exiting due to unrecoverable error: ", th);
            System.exit(EXIT_ERROR_CODE);
        }
        flushAsyncLogAppender();
    }

    private ServerContext getServerContext(Map<String, Object> opts) {
        // Bind to all interfaces only if no address or interface specified by the user.
        // Fetch the address if given a network interface.
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
        log.info("Configured Corfu Replication Server address: {}", opts.get(ADDRESS_PARAM));
        return new ServerContext(opts);
    }

    /**
     * Start Corfu Log Replication Discovery Service after the local node version matches the cluster version, i.e.,
     * any ongoing rolling upgrade completes.
     *
     * @param serverContext server context (server information)
     */
    private void startDiscoveryService(ServerContext serverContext) {

        CorfuRuntime runtime = getRuntime(serverContext);
        CorfuStore corfuStore = new CorfuStore(runtime);

        LogReplicationUpgradeManager upgradeManager =
                new LogReplicationUpgradeManager(corfuStore, serverContext.getPluginConfigFilePath());

        // Check if an upgrade is in progress.  If it is, wait for it to complete
        log.info("Wait for any ongoing rolling upgrade to complete.");

        while(isUpgradeInProgress(upgradeManager, corfuStore)) {
            // Wait before checking again.
            sleep();
        }

        // Upgrade has completed.  Start the discovery Service
        log.info("Start Discovery Service.");

        // Start LogReplicationDiscovery Service, responsible for
        // acquiring lock, retrieving Site Manager Info and processing this info
        // so this node is initialized as Source (sender) or Sink (receiver)
        replicationDiscoveryService = new CorfuReplicationDiscoveryService(serverContext, runtime);
        replicationDiscoveryService.start();
    }

    private CorfuRuntime getRuntime(ServerContext serverContext) {

        String localCorfuEndpoint = getCorfuEndpoint(getHostFromEndpointURL(serverContext.getLocalEndpoint()),
            serverContext.getCorfuServerConnectionPort());

        return CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder()
            .trustStore((String) serverContext.getServerConfig().get(ConfigParamNames.TRUST_STORE))
            .tsPasswordFile((String) serverContext.getServerConfig().get(ConfigParamNames.TRUST_STORE_PASS_FILE))
            .keyStore((String) serverContext.getServerConfig().get(ConfigParamNames.KEY_STORE))
            .ksPasswordFile((String) serverContext.getServerConfig().get(ConfigParamNames.KEY_STORE_PASS_FILE))
            .tlsEnabled((Boolean) serverContext.getServerConfig().get("--enable-tls"))
            .systemDownHandler(() -> System.exit(SYSTEM_EXIT_ERROR_CODE))
            .maxCacheEntries(serverContext.getLogReplicationCacheMaxSize()/2)
            .maxWriteSize(serverContext.getMaxWriteSize())
            .build())
            .parseConfigurationString(localCorfuEndpoint).connect();
    }

    private String getCorfuEndpoint(String localHostAddress, int port) {
        return getVersionFormattedEndpointURL(localHostAddress, port);
    }

    private boolean isUpgradeInProgress(LogReplicationUpgradeManager upgradeManager, CorfuStore corfuStore) {
        boolean isUpgraded = true;
        try (TxnContext txnContext = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            isUpgraded = upgradeManager.getLrRollingUpgradeHandler().isLRUpgradeInProgress(txnContext);
            txnContext.commit();
        } catch (TransactionAbortedException e) {
            // TODO V2: This exception needs to be caught to handle concurrent writes to the Replication Event table
            //  from multiple nodes.  The retry should be done in LRRollingUpgradeHandler where the event is
            //  written.  It is not currently possible because the transaction is committed outside
            //  LRRollingUpgradeHandler.  When the new wrapper for isLRUpgradeInProgress() without TxnContext is
            //  available, this try-catch block should be moved there.
            log.warn("TX Abort when writing to the Replication Event Table.  The table was already updated by another" +
                    " node.", e);
        }
        return isUpgraded;
    }

    private void sleep() {
        try {
            TimeUnit.MILLISECONDS.sleep(DEFAULT_UPGRADE_CHECK_DELAY_MS);
        } catch (InterruptedException e) {
            log.debug("Sleep Interrupted", e);
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

    public static void configureMetrics(Map<String, Object> opts, String localEndpoint) {
        if ((boolean) opts.get("--metrics")) {
            try {
                LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
                Optional.ofNullable(context.exists(DEFAULT_METRICS_LOGGER_NAME))
                    .ifPresent(logger -> MeterRegistryProvider.MeterRegistryInitializer.initServerMetrics(logger,
                        DEFAULT_METRICS_LOGGING_INTERVAL_DURATION, localEndpoint));
            } catch (IllegalStateException ise) {
                log.warn("Registry has been previously initialized. Skipping.");
            }
        }
    }

    /**
     * Attempt to cleanly shutdown all the servers.
     */
    public void cleanShutdown() {
        log.info("CleanShutdown: Starting Cleanup.");

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
     * @param opts Arguments.
     */
    private static void printStartupMsg(Map<String, Object> opts) {
        printLogo();
        println("");
        println("------------------------------------");
        println("Initializing LOG REPLICATION SERVER");
        println("Version (" + GitRepositoryState.getRepositoryState().commitIdAbbrev + ")");
        final int port = Integer.parseInt((String) opts.get("<port>"));
        println("Serving on port " + port);
        println("------------------------------------");
        println("");
    }
}
