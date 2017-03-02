package org.corfudb.infrastructure;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageDecoder;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageEncoder;
import org.corfudb.security.sasl.plaintext.PlainTextSaslNettyServer;
import org.corfudb.security.tls.TlsUtils;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.Version;
import org.docopt.Docopt;
import org.fusesource.jansi.AnsiConsole;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadFactory;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.net.ssl.SSLEngine;

import static org.fusesource.jansi.Ansi.Color.*;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * This is the new Corfu server single-process executable.
 * <p>
 * The command line options are documented in the USAGE variable.
 * <p>
 * Created by mwei on 11/30/15.
 */

@Slf4j
public class CorfuServer {
    @Getter
    private static SequencerServer sequencerServer;

    @Getter
    private static LayoutServer layoutServer;

    @Getter
    private static LogUnitServer logUnitServer;

    @Getter
    private static ManagementServer managementServer;

    private static NettyServerRouter router;

    private static ServerContext serverContext;

    public static boolean serverRunning = false;

    private static SslContext sslContext;

    private static String[] enabledTlsProtocols;

    private static String[] enabledTlsCipherSuites;

    /**
     * This string defines the command line arguments,
     * in the docopt DSL (see http://docopt.org) for the executable.
     * It also serves as the documentation for the executable.
     * <p>
     * Unfortunately, Java doesn't support multi-line string literals,
     * so you must concatenate strings and terminate with newlines.
     * <p>
     * Note that the java implementation of docopt has a strange requirement
     * that each option must be preceded with a space.
     */
    private static final String USAGE =
            "Corfu Server, the server for the Corfu Infrastructure.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tcorfu_server (-l <path>|-m) [-nsQ] [-a <address>] [-t <token>] [-c <size>] [-k seconds] [-d <level>] [-p <seconds>] [-M <address>:<port>] [-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] [-b] [-g -o <username_file> -j <password_file>] [-x <ciphers>] [-z <tls-protocols>]] <port>\n"
                    + "\n"
                    + "Options:\n"
                    + " -l <path>, --log-path=<path>                                                           Set the path to the storage file for the log unit.\n"
                    + " -s, --single                                                                           Deploy a single-node configuration.\n"
                    + "                                                                                        The server will be bootstrapped with a simple one-unit layout.\n"
                    + " -a <address>, --address=<address>                                                      IP address to advertise to external clients [default: localhost].\n"
                    + " -m, --memory                                                                           Run the unit in-memory (non-persistent).\n"
                    + "                                                                                        Data will be lost when the server exits!\n"
                    + " -c <size>, --max-cache=<size>                                                          The size of the in-memory cache to serve requests from -\n"
                    + "                                                                                        If there is no log, then this is the max size of the log unit\n"
                    + "                                                                                        evicted entries will be auto-trimmed. [default: 1000000000].\n"
                    + " -t <token>, --initial-token=<token>                                                    The first token the sequencer will issue, or -1 to recover\n"
                    + "                                                                                        from the log. [default: -1].\n"
                    + " -p <seconds>, --compact=<seconds>                                                      The rate the log unit should compact entries (find the,\n"
                    + "                                                                                        contiguous tail) in seconds [default: 60].\n"
                    + " -d <level>, --log-level=<level>                                                        Set the logging level, valid levels are: \n"
                    + "                                                                                        ERROR,WARN,INFO,DEBUG,TRACE [default: INFO].\n"
                    + " -Q, --quickcheck-test-mode                                                             Run in QuickCheck test mode\n"
                    + " -M <address>:<port>, --management-server=<address>:<port>                              Layout endpoint to seed Management Server\n"
                    + " -n, --no-verify                                                                        Disable checksum computation and verification.\n"
                    + " -e, --enable-tls                                                                       Enable TLS.\n"
                    + " -u <keystore>, --keystore=<keystore>                                                   Path to the key store.\n"
                    + " -f <keystore_password_file>, --keystore-password-file=<keystore_password_file>         Path to the file containing the key store password.\n"
                    + " -b, --enable-tls-mutual-auth                                                           Enable TLS mutual authentication.\n"
                    + " -r <truststore>, --truststore=<truststore>                                             Path to the trust store.\n"
                    + " -w <truststore_password_file>, --truststore-password-file=<truststore_password_file>   Path to the file containing the trust store password.\n"
                    + " -g, --enable-sasl-plain-text-auth                                                      Enable SASL Plain Text Authentication.\n"
                    + " -o <username_file>, --sasl-plain-text-username-file=<username_file>                    Path to the file containing the username for SASL Plain Text Authentication.\n"
                    + " -j <password_file>, --sasl-plain-text-password-file=<password_file>                    Path to the file containing the password for SASL Plain Text Authentication.\n"
                    + " -x <ciphers>, --tls-ciphers=<ciphers>                                                  Comma separated list of TLS ciphers to use.\n"
                    + "                                                                                        [default: TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256].\n"
                    + " -z <tls-protocols>, --tls-protocols=<tls-protocols>                                    Comma separated list of TLS protocols to use.\n"
                    + "                                                                                        [default: TLSv1.1,TLSv1.2].\n"
                    + " -h, --help                                                                             Show this screen\n"
                    + " --version                                                                              Show version\n";

    public static void printLogo() {
        System.out.println(ansi().fg(WHITE).a("▄████████  ▄██████▄     ▄████████    ▄████████ ███    █▄").reset());
        System.out.println(ansi().fg(WHITE).a("███    ███ ███    ███   ███    ███   ███    ███ ███    ███").reset());
        System.out.println(ansi().fg(WHITE).a("███    █▀  ███    ███   ███    ███   ███    █▀  ███    ███").reset());
        System.out.println(ansi().fg(WHITE).a("███        ███    ███  ▄███▄▄▄▄██▀  ▄███▄▄▄     ███    ███").reset());
        System.out.println(ansi().fg(WHITE).a("███        ███    ███ ▀▀███▀▀▀▀▀   ▀▀███▀▀▀     ███    ███").reset());
        System.out.println(ansi().fg(WHITE).a("███    █▄  ███    ███ ▀███████████   ███        ███    ███").reset());
        System.out.println(ansi().fg(WHITE).a("███    ███ ███    ███   ███    ███   ███        ███    ███").reset());
        System.out.println(ansi().fg(WHITE).a("████████▀   ▀██████▀    ███    ███   ███        ████████▀").reset());
        System.out.println(ansi().fg(WHITE).a("                        ███    ███").reset());
    }

    public static void main(String[] args) {
        serverRunning = true;

        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE).withVersion(GitRepositoryState.getRepositoryState().describe).parse(args);

        int port = Integer.parseInt((String) opts.get("<port>"));
        // Print a nice welcome message.
        AnsiConsole.systemInstall();
        printLogo();
        System.out.println(ansi().a("Welcome to ").fg(RED).a("CORFU ").fg(MAGENTA).a("SERVER").reset());
        System.out.println(ansi().a("Version ").a(Version.getVersionString()).a(" (").fg(BLUE)
                .a(GitRepositoryState.getRepositoryState().commitIdAbbrev).reset().a(")"));
        System.out.println(ansi().a("Serving on port ").fg(WHITE).a(port).reset());
        System.out.println(ansi().a("Service directory: ").fg(WHITE).a(
                (Boolean) opts.get("--memory") ? "MEMORY mode" :
                        opts.get("--log-path")).reset());

        // Pick the correct logging level before outputting error messages.
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        switch ((String) opts.get("--log-level")) {
            case "ERROR":
                root.setLevel(Level.ERROR);
                break;
            case "WARN":
                root.setLevel(Level.WARN);
                break;
            case "INFO":
                root.setLevel(Level.INFO);
                break;
            case "DEBUG":
                root.setLevel(Level.DEBUG);
                break;
            case "TRACE":
                root.setLevel(Level.TRACE);
                break;
            default:
                root.setLevel(Level.INFO);
                log.warn("Level {} not recognized, defaulting to level INFO", opts.get("--log-level"));
        }

        log.debug("Started with arguments: " + opts);

        // Create the service directory if it does not exist.
        if (!(Boolean) opts.get("--memory")) {
            File serviceDir = new File((String) opts.get("--log-path"));

            if (!serviceDir.exists()) {
                if (serviceDir.mkdirs()) {
                    log.info("Created new service directory at {}.", serviceDir);
                }
            } else if (!serviceDir.isDirectory()) {
                log.error("Service directory {} does not point to a directory. Aborting.", serviceDir);
                throw new RuntimeException("Service directory must be a directory!");
            }
        }

        // Now, we start the Netty router, and have it route to the correct port.
        router = new NettyServerRouter(opts);

        // Create a common Server Context for all servers to access.
        serverContext = new ServerContext(opts, router);

        // Add each role to the router.
        addSequencer();
        addLayoutServer();
        addLogUnit();
        addManagementServer();
        router.baseServer.setOptionsMap(opts);

        // Setup SSL if needed
        Boolean tlsEnabled = (Boolean) opts.get("--enable-tls");
        Boolean tlsMutualAuthEnabled = (Boolean) opts.get("--enable-tls-mutual-auth");
        if (tlsEnabled) {
            // Get the TLS cipher suites to enable
            String ciphs = (String) opts.get("--tls-ciphers");
            if (ciphs != null) {
                List<String> ciphers = Pattern.compile(",")
                    .splitAsStream(ciphs)
                    .map(String::trim)
                    .collect(Collectors.toList());
                enabledTlsCipherSuites = ciphers.toArray(new String[ciphers.size()]);
            }

            // Get the TLS protocols to enable
            String protos = (String) opts.get("--tls-protocols");
            if (protos != null) {
                List<String> protocols = Pattern.compile(",")
                    .splitAsStream(protos)
                    .map(String::trim)
                    .collect(Collectors.toList());
                enabledTlsProtocols = protocols.toArray(new String[protocols.size()]);
            }

            try {
                sslContext =
                        TlsUtils.enableTls(TlsUtils.SslContextType.SERVER_CONTEXT,
                                (String) opts.get("--keystore"), e -> {
                                    log.error("Could not load keys from the key store.");
                                    System.exit(1);
                                },
                                (String) opts.get("--keystore-password-file"), e -> {
                                    log.error("Could not read the key store password file.");
                                    System.exit(1);
                                },
                                (String) opts.get("--truststore"), e -> {
                                    log.error("Could not load keys from the trust store.");
                                    System.exit(1);
                                },
                                (String) opts.get("--truststore-password-file"), e -> {
                                    log.error("Could not read the trust store password file.");
                                    System.exit(1);
                                });
            } catch (Exception ex) {
                log.error("Could not build the SSL context");
                System.exit(1);
            }
        }

        Boolean saslPlainTextAuth = (Boolean) opts.get("--enable-sasl-plain-text-auth");

        // Create the event loops responsible for servicing inbound messages.
        EventLoopGroup bossGroup;
        EventLoopGroup workerGroup;
        EventExecutorGroup ee;

        bossGroup = new NioEventLoopGroup(1, new ThreadFactory() {
            final AtomicInteger threadNum = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("accept-" + threadNum.getAndIncrement());
                return t;
            }
        });

        workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2, new ThreadFactory() {
            final AtomicInteger threadNum = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("io-" + threadNum.getAndIncrement());
                return t;
            }
        });

        ee = new DefaultEventExecutorGroup(Runtime.getRuntime().availableProcessors() * 2, new ThreadFactory() {

            final AtomicInteger threadNum = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("event-" + threadNum.getAndIncrement());
                return t;
            }
        });


        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .option(ChannelOption.SO_BACKLOG, 100)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.SO_REUSEADDR, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(io.netty.channel.socket.SocketChannel ch) throws Exception {
                            if (tlsEnabled) {
                                SSLEngine engine = sslContext.newEngine(ch.alloc());
                                engine.setEnabledCipherSuites(enabledTlsCipherSuites);
                                engine.setEnabledProtocols(enabledTlsProtocols);
                                if (tlsMutualAuthEnabled) {
                                    engine.setNeedClientAuth(true);
                                }
                                ch.pipeline().addLast("ssl", new SslHandler(engine));
                            }
                            ch.pipeline().addLast(new LengthFieldPrepender(4));
                            ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                            if (saslPlainTextAuth) {
                                ch.pipeline().addLast("sasl/plain-text", new PlainTextSaslNettyServer());
                            }
                            ch.pipeline().addLast(ee, new NettyCorfuMessageDecoder());
                            ch.pipeline().addLast(ee, new NettyCorfuMessageEncoder());
                            ch.pipeline().addLast(ee, router);
                        }
                    });
            ChannelFuture f = b.bind(port).sync();
            while (true) {
                try {
                    f.channel().closeFuture().sync();
                } catch (InterruptedException ie) {
                }
            }

        } catch (InterruptedException ie) {

        } catch (Exception ex) {
            log.error("Corfu server shut down unexpectedly due to exception", ex);
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }

    }

    public static void addSequencer() {
        sequencerServer = new SequencerServer(serverContext);
        router.addServer(sequencerServer);
    }

    public static void addLayoutServer() {
        layoutServer = new LayoutServer(serverContext);
        router.addServer(layoutServer);
    }

    public static void addLogUnit() {
        logUnitServer = new LogUnitServer(serverContext);
        router.addServer(logUnitServer);
    }

    public static void addManagementServer() {
        managementServer = new ManagementServer(serverContext);
        router.addServer(managementServer);
    }
}
