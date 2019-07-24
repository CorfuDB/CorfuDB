package org.corfudb.infrastructure;

import static org.corfudb.util.NetworkUtils.getAddressFromInterfaceName;
import static org.fusesource.jansi.Ansi.Color.BLUE;
import static org.fusesource.jansi.Ansi.Color.MAGENTA;
import static org.fusesource.jansi.Ansi.Color.RED;
import static org.fusesource.jansi.Ansi.Color.WHITE;
import static org.fusesource.jansi.Ansi.ansi;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.core.joran.spi.JoranException;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.FileUtils;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.Version;
import org.docopt.Docopt;
import org.fusesource.jansi.AnsiConsole;
import org.slf4j.LoggerFactory;


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
                    + "\tcorfu_server (-l <path>|-m) [-nsN] [-a <address>|-q <interface-name>] "
                    + "[-t <token>] [-c <ratio>] [-d <level>] [-p <seconds>] "
                    + "[--layout-server-threads=<layout_server_threads>] [--base-server-threads=<base_server_threads>] "
                    + "[--log-size-quota-percentage=<max_log_size_percentage>]"
                    + "[--logunit-threads=<logunit_threads>] [--management-server-threads=<management_server_threads>]"
                    + "[-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] "
                    + "[-b] [-g -o <username_file> -j <password_file>] "
                    + "[-k <seqcache>] [-T <threads>] [-B <size>] [-i <channel-implementation>] "
                    + "[-H <seconds>] [-I <cluster-id>] [-x <ciphers>] [-z <tls-protocols>]] "
                    + "[-P <prefix>] [-R <retention>] [--agent] <port>\n"
                    + "\n"
                    + "Options:\n"
                    + " -l <path>, --log-path=<path>                                             "
                    + "              Set the path to the storage file for the log unit.\n"
                    + " -s, --single                                                             "
                    + "              Deploy a single-node configuration.\n"
                    + " -I <cluster-id>, --cluster-id=<cluster-id>"
                    + "              For a single node configuration the cluster id to use in UUID,"
                    + "              base64 format, or auto to randomly generate [default: auto].\n"
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
                    + " -H <seconds>, --HandshakeTimeout=<seconds>                               "
                    + "              Handshake timeout in seconds [default: 10].\n               "
                    + " -t <token>, --initial-token=<token>                                      "
                    + "              The first token the sequencer will issue, or -1 to recover\n"
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
                    + "              Number of threads dedicated for the base server.\n          "
                    + " --log-size-quota-percentage=<max_log_size_percentage>                         "
                    + "              The max size as percentage of underlying filestore size.\n  "
                    + "              If this limit is exceeded "
                    + "              write requests will be rejected [default: 100].\n"
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
                    + " -h, --help                                                               "
                    + "              Show this screen\n"
                    + " --version                                                                "
                    + "              Show version\n";

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

        // Parse the options given, using docopt.
        Map<String, Object> opts = new Docopt(USAGE)
                .withVersion(GitRepositoryState.getRepositoryState().describe)
                .parse(args);
        // Print a nice welcome message.
        AnsiConsole.systemInstall();
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

        // Register shutdown handler
        Thread shutdownThread = new Thread(CorfuServer::cleanShutdown);
        shutdownThread.setName("ShutdownThread");
        Runtime.getRuntime().addShutdownHook(shutdownThread);

        // Manages the lifecycle of the Corfu Server.
        while (!shutdownServer) {
            final ServerContext serverContext = new ServerContext(opts);
            try {
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

        File serviceDir = new File((String) opts.get("--log-path"));

        if (!serviceDir.isDirectory()) {
            log.error("Service directory {} does not point to a directory. Aborting.",
                    serviceDir);
            throw new UnrecoverableCorfuError("Service directory must be a directory!");
        }

        String corfuServiceDirPath = serviceDir.getAbsolutePath()
                + File.separator
                + "corfu";
        File corfuServiceDir = new File(corfuServiceDirPath);
        // Update the new path with the dedicated child service directory.
        opts.put("--log-path", corfuServiceDirPath);
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
    private static void cleanShutdown() {
        log.info("CleanShutdown: Starting Cleanup.");
        shutdownServer = true;
        activeServer.close();
    }

    /**
     * Print the corfu logo.
     */
    private static void printLogo() {
        println(ansi().fg(WHITE).toString());
        println("▄████████  ▄██████▄     ▄████████    ▄████████ ███    █▄");
        println("███    ███ ███    ███   ███    ███   ███    ██████    ███");
        println("███    █▀  ███    ███   ███    ███   ███    █▀ ███    ███");
        println("███        ███    ███  ▄███▄▄▄▄██▀  ▄███▄▄▄    ███    ███");
        println("███        ███    ███ ▀▀███▀▀▀▀▀   ▀▀███▀▀▀    ███    ███");
        println("███    █▄  ███    ███ ▀███████████   ███       ███    ███");
        println("███    ███ ███    ███   ███    ███   ███       ███    ███");
        println("████████▀   ▀██████▀    ███    ███   ███       ████████▀ ");
        println("                        ███    ███");
        println(ansi().reset().toString());
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
        int port = Integer.parseInt((String) opts.get("<port>"));
        println(ansi().a("Welcome to ").fg(RED).a("CORFU ").fg(MAGENTA).a("SERVER").reset());
        println(ansi().a("Version ").a(Version.getVersionString()).a(" (").fg(BLUE)
                .a(GitRepositoryState.getRepositoryState().commitIdAbbrev).reset().a(")"));
        println(ansi().a("Serving on port ").fg(WHITE).a(port).reset());
        println(ansi().a("Service directory: ").fg(WHITE).a(
                (Boolean) opts.get("--memory") ? "MEMORY mode" :
                        opts.get("--log-path")).reset());
    }
}
