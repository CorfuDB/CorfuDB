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
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageDecoder;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageEncoder;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.Version;
import org.docopt.Docopt;
import org.fusesource.jansi.AnsiConsole;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

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
                    + "\tcorfu_server (-l <path>|-m) [-fs] [-a <address>] [-t <token>] [-c <size>] [-k seconds] [-d <level>] [-p <seconds>] <port>\n"
                    + "\n"
                    + "Options:\n"
                    + " -l <path>, --log-path=<path>            Set the path to the storage file for the log unit.\n"
                    + " -s, --single                            Deploy a single-node configuration.\n"
                    + "                                         The server will be bootstrapped with a simple one-unit layout.\n"
                    + " -f, --sync                              Flush all writes to disk before acknowledging.\n"
                    + " -a <address>, --address=<address>       IP address to advertise to external clients [default: localhost].\n"
                    + " -m, --memory                            Run the unit in-memory (non-persistent).\n"
                    + "                                         Data will be lost when the server exits!\n"
                    + " -c <size>, --max-cache=<size>           The size of the in-memory cache to serve requests from -\n"
                    + "                                         If there is no log, then this is the max size of the log unit\n"
                    + "                                         evicted entries will be auto-trimmed. [default: 1000000000].\n"
                    + " -t <token>, --initial-token=<token>     The first token the sequencer will issue, or -1 to recover\n"
                    + "                                         from the log. [default: -1].\n"
                    + " -k <seconds>, --checkpoint=<seconds>    The rate the sequencer should checkpoint its state to disk,\n"
                    + "                                         in seconds [default: 60].\n"
                    + " -p <seconds>, --compact=<seconds>       The rate the log unit should compact entries (find the,\n"
                    + "                                         contiguous tail) in seconds [default: 60].\n"
                    + " -d <level>, --log-level=<level>         Set the logging level, valid levels are: \n"
                    + "                                         ERROR,WARN,INFO,DEBUG,TRACE [default: INFO].\n"
                    + " -h, --help  Show this screen\n"
                    + " --version  Show version\n";

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
        NettyServerRouter router = new NettyServerRouter(opts);

        // Add each role to the router.
        router.addServer(new SequencerServer(opts));
        router.addServer(new LayoutServer(opts, router));
        router.addServer(new LogUnitServer(opts));
        router.baseServer.setOptionsMap(opts);

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
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(io.netty.channel.socket.SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new LengthFieldPrepender(4));
                            ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
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
}
