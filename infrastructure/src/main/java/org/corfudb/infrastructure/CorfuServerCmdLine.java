package org.corfudb.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;

import java.util.Map;

/**
 * Command line helper class
 */
@Slf4j
public class CorfuServerCmdLine {

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
    public static final String USAGE =
            "Corfu Server, the server for the Corfu Infrastructure.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tcorfu_server (-l <path>|-m) [-nsNA] [-a <address>|-q <interface-name>] "
                    + "[-c <ratio>] [-d <level>] [-p <seconds>] "
                    + "[--layout-server-threads=<layout_server_threads>] [--base-server-threads=<base_server_threads>] "
                    + "[--log-size-quota-percentage=<max_log_size_percentage>]"
                    + "[--logunit-threads=<logunit_threads>] [--management-server-threads=<management_server_threads>]"
                    + "[-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] "
                    + "[-b] [-g -o <username_file> -j <password_file>] "
                    + "[-k <seqcache>] [-T <threads>] [-B <size>] [-i <channel-implementation>] "
                    + "[-H <seconds>] [-I <cluster-id>] [-x <ciphers>] [-z <tls-protocols>]] "
                    + "[--metrics] [--metrics-port <metrics_port>]"
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

    @Getter
    private final Map<String, Object> opts;

    public CorfuServerCmdLine(String[] args) {
        this.opts = buildOpts(args);
    }

    /**
     * Print the corfu logo.
     */
    private void printLogo() {
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
    private static void println(String line) {
        System.out.println(line);
        log.info(line);
    }

    /**
     * Print the welcome message, logo and the arguments.
     */
    public void printStartupMsg() {
        printLogo();
        println("Welcome to CORFU SERVER");
        println("Version (" + GitRepositoryState.getRepositoryState().commitIdAbbrev + ")");

        int port = Integer.parseInt((String) opts.get("<port>"));
        String dataLocation = (Boolean) opts.get("--memory") ? "MEMORY mode" :
                opts.get("--log-path").toString();

        println("Serving on port " + port);
        println("Data location: " + dataLocation);
    }

    /**
     * Parse the options given, using docopt.
     *
     * @param args command line arguments
     * @return Docopt object
     */
    private Map<String, Object> buildOpts(String[] args) {
        return new Docopt(USAGE)
                .withVersion(GitRepositoryState.getRepositoryState().describe)
                .parse(args);
    }
}
