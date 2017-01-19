package org.corfudb.infrastructure;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.codahale.metrics.Counter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.github.benmanes.caffeine.cache.Cache;
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
import io.netty.util.BooleanSupplier;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageDecoder;
import org.corfudb.protocols.wireprotocol.NettyCorfuMessageEncoder;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.Version;
import org.docopt.Docopt;
import org.fusesource.jansi.AnsiConsole;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
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

    /**
     * Metrics: meter (counter), histogram
     */
    static private final String mp = "corfu.server.";
    static private final String mpBase = mp + "base.";
    static private final String mpLU = mp + "logunit.";
    static private final String mpSeq = mp + "sequencer.";
    static private final String mpLayout = mp + "layout.";
    static private final String mpTrigger = "filter-trigger"; // internal use only

    public static final MetricRegistry metrics = new MetricRegistry();
    static final Counter counterFilterTrigger = metrics.counter(mpTrigger); // Internal use, never reported
    static final Timer timerPing = metrics.timer(mpBase + "ping");
    static final Timer timerVersionRequest = metrics.timer(mpBase + "version-request");
    static final Timer timerLogWrite = metrics.timer(mpLU + "write");
    static final Timer timerLogCommit = metrics.timer(mpLU + "commit");
    static final Timer timerLogRead = metrics.timer(mpLU + "read");
    static final Timer timerLogGcInterval = metrics.timer(mpLU + "gc-interval");
    static final Timer timerLogForceGc = metrics.timer(mpLU + "force-gc");
    static final Timer timerLogFillHole = metrics.timer(mpLU + "fill-hole");
    static final Timer timerLogTrim = metrics.timer(mpLU + "trim");
    static final Timer timerSeqReq = metrics.timer(mpSeq + "token-req");
    static final Counter counterTokenSum = metrics.counter(mpSeq + "token-sum");
    static final Counter counterToken0 = metrics.counter(mpSeq + "token-0");
    static final Timer timerLayoutReq = metrics.timer(mpLayout + "request");
    static final Timer timerLayoutBootstrap = metrics.timer(mpLayout + "bootstrap");
    static final Timer timerLayoutSetEpoch = metrics.timer(mpLayout + "set-epoch");
    static final Timer timerLayoutPrepare = metrics.timer(mpLayout + "prepare");
    static final Timer timerLayoutPropose = metrics.timer(mpLayout + "propose");
    static final Timer timerLayoutCommitted = metrics.timer(mpLayout + "committed");
    static final MetricSet metricsJVMGC = new GarbageCollectorMetricSet();
    static final MetricSet metricsJVMMem = new MemoryUsageGaugeSet();
    static final MetricSet metricsJVMThread = new ThreadStatesGaugeSet();

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
                    + "\tcorfu_server (-l <path>|-m) [-nsQ] [-a <address>] [-t <token>] [-c <size>] [-k seconds] [-d <level>] [-p <seconds>] [-M <address>:<port>] <port>\n"
                    + "\n"
                    + "Options:\n"
                    + " -l <path>, --log-path=<path>            Set the path to the storage file for the log unit.\n"
                    + " -s, --single                            Deploy a single-node configuration.\n"
                    + "                                         The server will be bootstrapped with a simple one-unit layout.\n"
                    + " -a <address>, --address=<address>       IP address to advertise to external clients [default: localhost].\n"
                    + " -m, --memory                            Run the unit in-memory (non-persistent).\n"
                    + "                                         Data will be lost when the server exits!\n"
                    + " -c <size>, --max-cache=<size>           The size of the in-memory cache to serve requests from -\n"
                    + "                                         If there is no log, then this is the max size of the log unit\n"
                    + "                                         evicted entries will be auto-trimmed. [default: 1000000000].\n"
                    + " -t <token>, --initial-token=<token>     The first token the sequencer will issue, or -1 to recover\n"
                    + "                                         from the log. [default: -1].\n"
                    + " -p <seconds>, --compact=<seconds>       The rate the log unit should compact entries (find the,\n"
                    + "                                         contiguous tail) in seconds [default: 60].\n"
                    + " -d <level>, --log-level=<level>         Set the logging level, valid levels are: \n"
                    + "                                         ERROR,WARN,INFO,DEBUG,TRACE [default: INFO].\n"
                    + " -Q, --quickcheck-test-mode              Run in QuickCheck test mode\n"
                    + " -M <address>:<port>, --management-server=<address>:<port>     Layout endpoint to seed Management Server\n"
                    + " -n, --no-verify                         Disable checksum computation and verification.\n"
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

        // Create the event loops responsible for servicing inbound messages.
        EventLoopGroup bossGroup;
        EventLoopGroup workerGroup;
        EventExecutorGroup ee;

        // Metrics reporting setup
        metrics.register("jvm.gc", metricsJVMGC);
        metrics.register("jvm.memory", metricsJVMMem);
        metrics.register("jvm.thread", metricsJVMThread);
        metricsCachesSetup();
        metricsReportingSetup();

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

    private static void metricsCachesSetup() {
        addCacheGauges(mpLU + "cache.", getLogUnitServer().getDataCache());
        addCacheGauges(mp + "datastore.cache.", serverContext.getDataStore().getCache());
        addCacheGauges(mpSeq + "conflict.cache.", sequencerServer.getConflictToGlobalTailCache());
    }

    private static void addCacheGauges(String name, Cache cache) {
        metrics.register(name + "cache-size", (Gauge<Long>) () -> cache.estimatedSize());
        metrics.register(name + "evictions", (Gauge<Long>) () -> cache.stats().evictionCount());
        metrics.register(name + "hit-rate", (Gauge<Double>) () -> cache.stats().hitRate());
        metrics.register(name + "hits", (Gauge<Long>) () -> cache.stats().hitCount());
        metrics.register(name + "misses", (Gauge<Long>) () -> cache.stats().missCount());
    }

    static Properties metricsProperties = new Properties();
    static boolean metricsReportingEnabled = false;

    /**
     * Load a metrics properties file.
     * The expected properties in this properties file are:
     *
     * enabled: Boolean for whether CSV output will be generated.
     *          For each reporting interval, this function will be
     *          called to re-parse the properties file and to
     *          re-evaluate the value of 'enabled'.  Changes to
     *          any other property in this file will be ignored.
     *
     * directory: String for the path to the CSV output subdirectory
     *
     * interval: Long for the reporting interval for CSV output
     */
    private static void loadPropertiesFile() {
        String propPath;

        if ((propPath = System.getenv("METRICS_PROPERTIES")) != null) {
            try {
                metricsProperties.load(new FileInputStream(propPath));
                metricsReportingEnabled = Boolean.valueOf((String) metricsProperties.get("enabled"));
            } catch (Exception e) {
                log.error("Error processing METRICS_PROPERTIES {}: {}", propPath, e.toString());
            }
        }
    }

    private static void metricsReportingSetup() {
        loadPropertiesFile();
        String outPath = (String) metricsProperties.get("directory");
        if (outPath != null && !outPath.isEmpty()) {
            Long interval = Long.valueOf((String) metricsProperties.get("interval"));
            File statDir = new File(outPath);
            statDir.mkdirs();
            MetricFilter f = new MetricFilter() {
                @Override
                public boolean matches(String name, Metric metric) {
                    if (name.equals(mpTrigger)) {
                        loadPropertiesFile();
                        return false;
                    }
                    return metricsReportingEnabled;
                }
            };
            final CsvReporter reporter1 = CsvReporter.forRegistry(metrics)
                    .formatFor(Locale.US)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .filter(f)
                    .build(statDir);
            reporter1.start(interval, TimeUnit.SECONDS);
        }
    }
}
