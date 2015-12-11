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
import org.corfudb.runtime.wireprotocol.NettyCorfuMessageDecoder;
import org.corfudb.runtime.wireprotocol.NettyCorfuMessageEncoder;
import org.corfudb.util.GitRepositoryState;
import org.docopt.Docopt;
import org.fusesource.jansi.AnsiConsole;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static org.fusesource.jansi.Ansi.ansi;
import static org.fusesource.jansi.Ansi.Color.*;

/**
 * This is the new Corfu server single-process executable.
 *
 * The command line options are documented in the USAGE variable.
 *
 * Created by mwei on 11/30/15.
 */

@Slf4j
public class CorfuServer {

    /**
     * This string defines the command line arguments,
     * in the docopt DSL (see http://docopt.org) for the executable.
     * It also serves as the documentation for the executable.
     *
     * Unfortunately, Java doesn't support multi-line string literals,
     * so you must concatenate strings and terminate with newlines.
     *
     * Note that the java implementation of docopt has a strange requirement
     * that each option must be preceded with a space.
     */
    private static final String USAGE =
            "Corfu Server, the server for the Corfu Infrastructure.\n"
            + "\n"
            + "Usage:\n"
            + "\tcorfu_server (-l <path>|-m) [-s] [-a <address>] [-t <token>] [-d <level>] <port>\n"
            + "\n"
            + "Options:\n"
            + " -l <path>, --log-path=<path>         Set the path to the storage file for the log unit.\n"
            + " -s, --single                         Deploy a single-node configuration.\n"
            + " -a <address>, --address=<address>    IP address to advertise to external clients [default: localhost].\n"
            + "                                      The server will be bootstrapped with a simple one-unit layout.\n"
            + " -m, --memory                         Run the unit in-memory (non-persistent).\n"
            + "                                      Data will be lost when the server exits!\n"
            + " -t <token>, --initial-token=<token>  The first token the sequencer will issue, or -1 to recover\n"
            + "                                      from the log. [default: -1].\n"
            + " -d <level>, --log-level=<level>      Set the logging level, valid levels are: \n"
            + "                                      ERROR,WARN,INFO,DEBUG,TRACE [default: INFO].\n"
            + " -h, --help  Show this screen\n"
            + " --version  Show version\n";

    public static void main(String[] args) {

        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE).withVersion(GitRepositoryState.getRepositoryState().describe).parse(args);

        int port = Integer.parseInt((String) opts.get("<port>"));
        // Print a nice welcome message.
        AnsiConsole.systemInstall();
        System.out.println(ansi().a("Welcome to ").fg(RED).a("CORFU ").fg(MAGENTA).a("SERVER").reset());
        System.out.println(ansi().a("You are running version ").fg(BLUE)
                .a(GitRepositoryState.getRepositoryState().describe).reset());
        System.out.println(ansi().a("Serving on port ").fg(WHITE).a(port).reset());

        // Pick the correct logging level before outputting error messages.
        Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
        switch ((String)opts.get("--log-level"))
        {
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

        // Now, we start the Netty router, and have it route to the correct port.
        NettyServerRouter router = new NettyServerRouter();

        // Add each role to the router.
        router.addServer(new SequencerServer(opts));
        router.addServer(new LayoutServer(opts));
        router.addServer(new LogUnitServer(opts));

        // Create the event loops responsible for servicing inbound messages.
        EventLoopGroup bossGroup;
        EventLoopGroup workerGroup;
        EventExecutorGroup ee;

        bossGroup = new NioEventLoopGroup(1, new ThreadFactory() {
            final AtomicInteger threadNum = new AtomicInteger(0);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("accept-" +threadNum.getAndIncrement());
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
                    .option(ChannelOption.TCP_NODELAY, true)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
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
            while (true)
            {
                try {
                    f.channel().closeFuture().sync();
                } catch (InterruptedException ie)
                {}
            }

        }
        catch (InterruptedException ie)
        {

        }
        catch (Exception ex)
        {
            log.error("Corfu server shut down unexpectedly due to exception", ex);
        }
        finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }

    }
}
