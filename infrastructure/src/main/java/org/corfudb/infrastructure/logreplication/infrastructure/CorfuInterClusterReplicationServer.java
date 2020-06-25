package org.corfudb.infrastructure.logreplication.infrastructure;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.core.joran.spi.JoranException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.CorfuReplicationSiteManagerAdapter;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationStreamNameTableManager;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.util.NetworkUtils.getAddressFromInterfaceName;

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
    private static final String USAGE =
            "Log Replication Server, the server for the Log Replication.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tlog_replication_server [-a <address>|-q <interface-name>] "
                    + "[-c <ratio>] [-d <level>] [-p <seconds>] "
                    + "[--base-server-threads=<base_server_threads>] "
                    + "[-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] "
                    + "[-b] [-g -o <username_file> -j <password_file>] "
                    + "[-i <channel-implementation>] "
                    + "[-z <tls-protocols>]] [-H <seconds>] "
                    + "[--plugin=<plugin-config-file-path>] "
                    + "[-T <threads>] [-B <size>] "
                    + "[--metrics] [--metrics-port <metrics_port>] "
                    + "[-P <prefix>] [-R <retention>] [--agent] <port> \n"
                    + "\n"
                    + "Options:\n"
                    + " -s, --single                                                             "
                    + "              Deploy a single-node Log Replication.\n"
                    + " -T <threads>, --Threads=<threads>                                        "
                    + "              Number of corfu server worker threads, or 0 to use 2x the "
                    + "              number of available processors [default: 0].\n"
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
                    + "                                                                          "
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
                    + " --plugin=<plugin-config-file-path>                                       "
                    + "             Path to Plugin Config Path.\n                                "
                    + " -H <seconds>, --HandshakeTimeout=<seconds>                               "
                    + "              Handshake timeout in seconds [default: 10].\n               "
                    + "                                                                          "
                    + "              from the log. [default: -1].\n    "
                    + " -B <size> --batch-size=<size>                                            "
                    + "              The read/write batch size used for data transfer operations [default: 100].\n"
                    + "                                                                          "
                    + " -k <seqcache>, --sequencer-cache-size=<seqcache>                         "
                    + "               The size of the sequencer's cache. [default: 250000].\n    "
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
                    + "              Number of threads dedicated for the base server.\n          "
                    + " --log-size-quota-percentage=<max_log_size_percentage>                    "
                    + "              The max size as percentage of underlying file-store size.\n "
                    + "              If this limit is exceeded "
                    + "              write requests will be rejected [default: 100.0].\n         "
                    + "                                                                          "
                    + " --layout-server-threads=<layout_server_threads>                          "
                    + "              Number of threads dedicated for the layout server.\n        "
                    + "                                                                          "
                    + " --management-server-threads=<management_server_threads>                  "
                    + "              Number of threads dedicated for the management server.\n"
                    + "                                                                          "
                    + " --logunit-threads=<logunit_threads>                  "
                    + "              Number of threads dedicated for the logunit server.\n"
                    + "                                                                          "
                    + " --agent      Run with byteman agent to enable runtime code injection.\n  "
                    + " --metrics                                                                "
                    + "              Enable metrics provider.\n                                  "
                    + " --metrics-port=<metrics_port>                                            "
                    + "              Metrics provider server port [default: 9999].\n             "
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

    @Getter
    private CorfuReplicationSiteManagerAdapter siteManagerAdapter;

    @Getter
    CorfuReplicationDiscoveryService replicationDiscoveryService;

    String[] args;

    private static boolean test;

    public static void main(String[] args) {
        // For testing purposes, inspect for test flag and remove.
        // This flag is required to avoid forcefully shutting down the server
        // in the event of an InterruptedException which can be part of a test workflow, once the logic
        // is executed (executorService.shutdownNow()).
        args = checkIfTest(args);

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
            if(!test) {
                log.error("Exit. Unrecoverable error", err);
                throw err;
            }
        }
    }

    /**
     * Set a flag if this server is run from a testing environment,
     * so we can avoid triggering SYSTEM_EXIT when the test ends
     * which will cause an error to be thrown.
     */
    private static String[] checkIfTest(String[] args) {
        List<String> argsList = new ArrayList<>();
        argsList.addAll(Arrays.asList(args));
        test = argsList.contains("test");
        argsList.remove("test");
        return argsList.toArray(new String[0]);
    }

    private void startServer() {
        // Parse the options given, using docopt.
        Map<String, Object> opts = new Docopt(USAGE)
                .withVersion(GitRepositoryState.getRepositoryState().describe)
                .parse(args);

        printStartupMsg(opts);
        configureLogger(opts);

        log.info("Started with arguments: {}", opts);
        String pluginConfigFilePath = opts.get("--plugin") != null ? (String) opts.get("--plugin") :
                ServerContext.PLUGIN_CONFIG_FILE_PATH;

        this.siteManagerAdapter = constructSiteManagerAdapter(pluginConfigFilePath);

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

        // Register shutdown handler
        Thread shutdownThread = new Thread(new CleanupRunnable(this));
        shutdownThread.setName("ShutdownThread");
        Runtime.getRuntime().addShutdownHook(shutdownThread);

        // Manages the lifecycle of the Corfu Server.
        while (!shutdownServer) {
            final ServerContext serverContext = new ServerContext(opts);
            try {
                String corfuPort = serverContext.getLocalEndpoint()
                    .equals("localhost:9020") ? ":9001" : ":9000";

                LogReplicationStreamNameTableManager replicationStreamNameTableManager =
                    new LogReplicationStreamNameTableManager(corfuPort, pluginConfigFilePath);

                // TODO pankti: Check if version does not match.  If if does not, create an event for site discovery to
                // do a snapshot sync.
                boolean upgraded = replicationStreamNameTableManager
                    .isUpgraded();

                // Initialize the LogReplicationConfig with site ids and tables to replicate
                Set<String> streamsToReplicate =
                    replicationStreamNameTableManager.getStreamsToReplicate();

                // Start LogReplicationDiscovery Service, responsible for
                // acquiring lock, retrieving Site Manager Info and processing this info
                // so this node is initialized as Source (sender) or Sink (receiver)
                activeServer = new CorfuInterClusterReplicationServerNode(serverContext,  new LogReplicationConfig(streamsToReplicate));

                replicationDiscoveryService = new CorfuReplicationDiscoveryService(serverContext, activeServer, siteManagerAdapter);

                Thread replicationDiscoveryThread = new Thread(replicationDiscoveryService);
                replicationDiscoveryThread.start();

                // Start Corfu Replication Server Node
                activeServer.startAndListen();

                if (upgraded) {
                    replicationDiscoveryService.putEvent(
                        new DiscoveryServiceEvent(DiscoveryServiceEvent.DiscoveryServiceEventType.UPGRADE));
                }
            } catch (Throwable th) {
                if (!test) {
                    log.error("CorfuServer: Server exiting due to unrecoverable error: ", th);
                    System.exit(EXIT_ERROR_CODE);
                }
            }

            if (cleanupServer) {
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
    private void configureLogger(Map<String, Object> opts) {
        final Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        final Level level = Level.toLevel(((String) opts.get("--log-level")).toUpperCase());
        root.setLevel(level);
    }

    /**
     * Cleanly shuts down the server and restarts.
     *
     * @param resetData Resets and clears all data if True.
     */
    void restartServer(boolean resetData) {

        if (resetData) {
            cleanupServer = true;
        }

        log.info("RestartServer: Shutting down Log Replication server");
        activeServer.close();
        log.info("RestartServer: Starting Log Replication server");
    }

    /**
     * Attempt to cleanly shutdown all the servers.
     */
    public void cleanShutdown() {
        log.info("CleanShutdown: Starting Cleanup.");
        shutdownServer = true;
        activeServer.close();
        replicationDiscoveryService.shutdown();
        siteManagerAdapter.shutdown();
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
        // log.info(line.toString());
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
            println("VERSION (" + GitRepositoryState.getRepositoryState().commitIdAbbrev + ")");
            final int port = Integer.parseInt((String) opts.get("<port>"));
            println("Serving on port " + port);
            println("------------------------------------");
            println("");
    }

    private CorfuReplicationSiteManagerAdapter constructSiteManagerAdapter(String pluginConfigFilePath) {

        LogReplicationPluginConfig config = new LogReplicationPluginConfig(pluginConfigFilePath);
        File jar = new File(config.getTopologyManagerAdapterJARPath());

        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class adapter = Class.forName(config.getTopologyManagerAdapterName(), true, child);
            return (CorfuReplicationSiteManagerAdapter) adapter.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            log.error("Fatal error: Failed to create serverAdapter", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    static class CleanupRunnable implements Runnable {
            CorfuInterClusterReplicationServer server;
            CleanupRunnable(CorfuInterClusterReplicationServer server) {
                this.server = server;
            }
            @Override
            public void run() {
                server.cleanShutdown();
            }
    }
}
