package org.corfudb.runtime;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import java.lang.Thread.UncaughtExceptionHandler;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.Singular;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.comm.ChannelImplementation;
import org.corfudb.protocols.wireprotocol.VersionInfo;
import org.corfudb.recovery.FastObjectLoader;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LogUnitClient;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.exceptions.RequestTimeoutException;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.exceptions.unrecoverable.RuntimeShutdownError;
import org.corfudb.runtime.exceptions.unrecoverable.SystemUnavailableError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutManagementView;
import org.corfudb.runtime.view.LayoutView;
import org.corfudb.runtime.view.ManagementView;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.SequencerView;
import org.corfudb.runtime.view.StreamsView;

import org.corfudb.util.CFUtils;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.Sleep;
import org.corfudb.util.UuidUtils;
import org.corfudb.util.Version;

/**
 * Created by mwei on 12/9/15.
 */
@Slf4j
@Accessors(chain = true)
public class CorfuRuntime {

    /** A class which holds parameters and settings for the {@link CorfuRuntime}.
     *
     */
    @Builder
    @Data
    public static class CorfuRuntimeParameters {

        // region Object Layer Parameters
        /** True, if undo logging is disabled. */
        @Default boolean undoDisabled = false;

        /** True, if optimistic undo logging is disabled. */
        @Default boolean optimisticUndoDisabled = false;

        /**
         * Use fast loader to restore objects on connection.
         *
         * <p>If using this utility, you need to be sure that no one
         * is accessing objects until the tables are loaded
         * (i.e. when connect returns)
         */
        @Default boolean useFastLoader = false;

        /** Set the bulk read size. */
        @Default int bulkReadSize = 10;

        /**
         * How much time the Fast Loader has to get the maps up to date.
         *
         * <p>Once the timeout is reached, the Fast Loader gives up. Every map that is
         * not up to date will be loaded through normal path.
         *
         */
        @Default Duration fastLoaderTimeout = Duration.ofMinutes(30);
        // endregion

        // region Address Space Parameters
        /** Number of times to attempt to read before hole filling. */
        @Default int holeFillRetry = 10;

        /** Whether or not to disable the cache. */
        @Default boolean cacheDisabled = false;

        /** The maximum size of the cache, in bytes. */
        @Default long numCacheEntries = 5000;

        /** Sets expireAfterAccess and expireAfterWrite in seconds. */
        @Default long cacheExpiryTime = Long.MAX_VALUE;
        // endregion

        // region Handshake Parameters
        /** Sets handshake timeout in seconds. */
        @Default int handshakeTimeout = 10;
        // endregion

        // region Stream Parameters
        /** Whether or not to disable backpointers. */
        @Default boolean backpointersDisabled = false;

        /** Whether or not hole filling should be disabled. */
        @Default boolean holeFillingDisabled = false;

        /** Number of times to retry on an
         * {@link org.corfudb.runtime.exceptions.OverwriteException} before giving up. */
        @Default int writeRetry = 5;

        /** The number of times to retry on a retriable
         * {@link org.corfudb.runtime.exceptions.TrimmedException} during a transaction.*/
        @Default int trimRetry = 2;
        // endregion

        //region        Security parameters
        /** True, if TLS is enabled. */
        @Default boolean tlsEnabled = false;

        /** A path to the key store. */
        String keyStore;

        /** A file containing the password for the key store. */
        String ksPasswordFile;

        /** A path to the trust store. */
        String trustStore;

        /** A path containing the password for the trust store. */
        String tsPasswordFile;

        /** True, if SASL plain text authentication is enabled. */
        @Default boolean saslPlainTextEnabled = false;

        /** A path containing the username file for SASL. */
        String usernameFile;

        /** A path containing the password file for SASL. */
        String passwordFile;
        //endregion

        //region Connection parameters
        /**
         * {@link Duration} before requests timeout.
         */
        @Default Duration requestTimeout = Duration.ofSeconds(5);

        /**
         * {@link Duration} before connections timeout.
         */
        @Default Duration connectionTimeout = Duration.ofMillis(500);

        /**
         * {@link Duration} before reconnecting to a disconnected node.
         */
        @Default Duration connectionRetryRate = Duration.ofSeconds(1);

        /**
         * The {@link UUID} for this client. Randomly generated by default.
         */
        @Default UUID clientId = UUID.randomUUID();

        /** The {@link UUID} for the cluster this client is connecting to, or
         * {@code null} if the client should adopt the {@link UUID} of the first
         *  server it connects to.
         */
        @Default UUID clusterId = null;

        /** The type of socket which {@link NettyClientRouter}s should use. By default,
         *  an NIO based implementation is used.
         */
        @Default
        ChannelImplementation socketType = ChannelImplementation.NIO;

        /** The initial list of layout servers. */
        @Singular List<NodeLocator> layoutServers;

        /** If true, automatically connects the runtime at construction time, so that
         *  an explicit call to {@link this#connect()} is not needed.
         *
         */
        @Default boolean autoConnect = false;

        /** Netty channel options, if provided. If no options are set, we default to
         *  the defaults in {@link this#DEFAULT_CHANNEL_OPTIONS}.
         */
        @Singular Map<ChannelOption, Object> customNettyChannelOptions;

        /** Default channel options, used if there are no options in the
         * {@link this#customNettyChannelOptions} field.
         */
        static final Map<ChannelOption, Object> DEFAULT_CHANNEL_OPTIONS =
                ImmutableMap.<ChannelOption, Object>builder()
                .put(ChannelOption.TCP_NODELAY, true)
                .put(ChannelOption.SO_KEEPALIVE, true)
                .put(ChannelOption.SO_REUSEADDR, true)
                .build();

        /** Get the netty channel options to be used by the netty client implementation.
         *
         * @return  A map containing options which should be applied to each netty channel.
         */
        public Map<ChannelOption, Object> getNettyChannelOptions() {
            return customNettyChannelOptions.size() == 0
                ? DEFAULT_CHANNEL_OPTIONS : customNettyChannelOptions;
        }
        //endregion

        //region Threading Parameters
        /** The {@link EventLoopGroup} which {@link NettyClientRouter}s will use.
         *  If not specified, the runtime will generate this using the {@link ChannelImplementation}
         *  specified in {@code socketType} and the {@link ThreadFactory} specified in
         *  {@code nettyThreadFactory}.
         */
        EventLoopGroup nettyEventLoop;

        /** A string which will be used to set the
         * {@link com.google.common.util.concurrent.ThreadFactoryBuilder#nameFormat} for the
         * {@code nettyThreadFactory}. By default, this is set to "netty-%d".
         * If you provide your own {@code nettyEventLoop}, this field is ignored.
         */
        @Default String nettyEventLoopThreadFormat = "netty-%d";

        /** The number of threads that should be available in the {@link NettyClientRouter}'s
         *  event pool. 0 means that we will use 2x the number of processors reported in the
         *  system. If you provide your own {@code nettyEventLoop}, this field is ignored.
         */
        @Default int nettyEventLoopThreads = 0;

        /** True, if the {@code NettyEventLoop} should be shutdown when the runtime is
         *  shutdown. False otherwise.
         */
        @Default boolean shutdownNettyEventLoop = true;

        /** A thread pool for executing I/O related tasks. These threads may block,
         * so ensure there are sufficient threads in the pool
         * ({@link Executors#newCachedThreadPool()} is recommended).
         *
         * <p>If not specified, the runtime will generate an executor based on a
         * {@link Executors#newCachedThreadPool()}.
         */
        ExecutorService ioExecutor;

        /** A string which will be used to set the
         * {@link com.google.common.util.concurrent.ThreadFactoryBuilder#nameFormat} for the
         * {@code ioExecutor}. By default, this is set to "io-%d".
         * If you provide your own {@code ioExecutor}, this field is ignored.
         */
        @Default String ioExecutorThreadFormat = "io-%d";

        /** Whether or not to shutdown the ioExecutor when the runtime is shutdown. */
        @Default boolean shutdownIoExecutor = true;

        /** A prefix which will be applied to all thread names we generate.
         *  If supplied, all threads generated by {@link ThreadFactory}s generated by
         *  the runtime will be prefixed with this string. Useful if multiple runtimes
         *  are in use in a system. Does not apply to {@link Executor}s or {@link EventLoopGroup}s
         *  which are supplied by the user.
         */
        @Default String threadPrefix = "";

        /** Get the thread name, with the prefix if set.
         *
         * @param threadFormat  The format to prefix.
         * @return              A prefixed thread format.
         */
        public @Nonnull String getPrefixedThreadFormat(@Nonnull String threadFormat) {
            if (threadPrefix.equals("")) {
                return threadFormat;
            } else {
                return threadPrefix + "-" + threadFormat;
            }
        }

        /** A {@link UncaughtExceptionHandler} which handles threads that have an uncaught
         *  exception. Used on all {@link ThreadFactory}s the runtime creates, but if you
         *  generate your own thread factory, this field is ignored. If this field is not set,
         *  the runtime's default handler runs, which logs an error level message.
         */
        UncaughtExceptionHandler uncaughtExceptionHandler;
        //endregion
    }

    /**
     * The parameters used to configure this {@link CorfuRuntime}.
     */
    @Getter
    private final CorfuRuntimeParameters parameters;

    // Methods for getting views
    // region Views
    /**
     * The {@link EventLoopGroup} provided to netty routers.
     */
    @Getter
    private final EventLoopGroup nettyEventLoop;

    /**
     * The {@link ExecutorService} to execute IO-bound tasks on.
     */
    @Getter
    private final ExecutorService ioExecutor;

    /**
     * A view of the layout service in the Corfu server instance.
     */
    @Getter(lazy = true)
    private final LayoutView layoutView = new LayoutView(this);
    /**
     * A view of the sequencer server in the Corfu server instance.
     */
    @Getter(lazy = true)
    private final SequencerView sequencerView = new SequencerView(this);
    /**
     * A view of the address space in the Corfu server instance.
     */
    @Getter(lazy = true)
    private final AddressSpaceView addressSpaceView = new AddressSpaceView(this);
    /**
     * A view of streamsView in the Corfu server instance.
     */
    @Getter(lazy = true)
    private final StreamsView streamsView = new StreamsView(this);

    /**
     * Views of objects in the Corfu server instance.
     */
    @Getter(lazy = true)
    private final ObjectsView objectsView = new ObjectsView(this);
    /**
     * A view of the Layout Manager to manage reconfigurations of the Corfu Cluster.
     */
    @Getter(lazy = true)
    private final LayoutManagementView layoutManagementView = new LayoutManagementView(this);
    /**
     * A view of the Management Service.
     */
    @Getter(lazy = true)
    private final ManagementView managementView = new ManagementView(this);

    //endregion Views

    /**
     * A list of known layout servers.
     */
    private List<String> layoutServers;

    /**
     * A map of routers, representing nodes.
     */
    public Map<String, IClientRouter> nodeRouters;

    /**
     * A completable future containing a layout, when completed.
     */
    public volatile CompletableFuture<Layout> layout;

    /**
     * A stamped lock which ensures only a single layout update occurs at a time.
     * The thread which updates the layout acquires the write lock. All other threads
     * wait using a read lock.
     */
    private final StampedLock layoutLock = new StampedLock();

    /** The {@link UUID} of the cluster we are currently connected to, or null, if
     *  there is no cluster yet.
     */
    @Getter
    public volatile UUID clusterId;

    /**
     * Notifies that the runtime is no longer used
     * and async retries to fetch the layout can be stopped.
     */
    @Getter
    private volatile boolean isShutdown = false;

    /**
     * Metrics: meter (counter), histogram.
     */
    private static final String mp = "corfu.runtime.";
    @Getter
    private static final String mpASV = mp + "as-view.";
    @Getter
    private static final String mpLUC = mp + "log-unit-client.";
    @Getter
    private static final String mpCR = mp + "client-router.";
    @Getter
    private static final String mpObj = mp + "object.";
    @Getter
    private static MetricRegistry defaultMetrics = new MetricRegistry();
    @Getter
    private MetricRegistry metrics = new MetricRegistry();

    public CorfuRuntime setMetrics(@NonNull MetricRegistry metrics) {
        this.metrics = metrics;
        return this;
    }

    /**
     * These two handlers are provided to give some control on what happen when system is down.
     *
     * <p>For applications that want to have specific behaviour when a the system appears
     * unavailable, they can register their own handler for both before the rpc request
     * and upon network exception.
     *
     * <p>An example of how to use these handlers implementing timeout is given in
     * test/src/test/java/org/corfudb/runtime/CorfuRuntimeTest.java
     *
     */
    public Runnable beforeRpcHandler = () -> { };
    public Runnable systemDownHandler = () -> { };


    public CorfuRuntime registerSystemDownHandler(Runnable handler) {
        systemDownHandler = handler;
        return this;
    }

    public CorfuRuntime registerBeforeRpcHandler(Runnable handler) {
        beforeRpcHandler = handler;
        return this;
    }


    /**
     * When set, overrides the default getRouterFunction. Used by the testing
     * framework to ensure the default routers used are for testing.
     */
    public static BiFunction<CorfuRuntime, String, IClientRouter> overrideGetRouterFunction = null;

    /**
     * A function to handle getting routers. Used by test framework to inject
     * a test router. Can also be used to provide alternative logic for obtaining
     * a router.
     */
    @Getter
    @Setter
    public Function<String, IClientRouter> getRouterFunction = overrideGetRouterFunction != null
            ? (address) -> overrideGetRouterFunction.apply(this, address) : (address) ->
                nodeRouters.compute(address, (k, r) -> {
                    final NettyClientRouter router = (NettyClientRouter) r;
                    if (router != null && router.getConnected()) {
                        // Return an existing router if we already have one and it is connected.
                        return router;
                    } else {
                        NodeLocator node = NodeLocator.parseString(address);
                        // Generate a new router, start it and add it to the table.
                        NettyClientRouter newRouter = new NettyClientRouter(node,
                                getNettyEventLoop(),
                                getParameters());
                        log.debug("Connecting to new router {}", node);
                        try {
                            newRouter.addClient(new LayoutClient())
                                .addClient(new SequencerClient())
                                .addClient(new LogUnitClient().setMetricRegistry(metrics != null
                                    ? metrics : CorfuRuntime.getDefaultMetrics()))
                                .addClient(new ManagementClient())
                                .start();
                            if (layout.isDone()) {
                                newRouter.setEpoch(CFUtils.getUninterruptibly(layout).getEpoch());
                            }
                        } catch (Exception e) {
                            log.warn("Error connecting to router", e);
                            throw e;
                        }
                        return newRouter;
                    }
                });

    /** Factory method for generating new {@link CorfuRuntime}s given a set of
     *  {@link CorfuRuntimeParameters} to configure the runtime with.
     *
     * @param parameters    A {@link CorfuRuntimeParameters} to use.
     * @return              A new {@link CorfuRuntime}.
     */
    public static CorfuRuntime fromParameters(@Nonnull CorfuRuntimeParameters parameters) {
        return new CorfuRuntime(parameters);
    }

    /** Construct a new {@link CorfuRuntime} given a {@link CorfuRuntimeParameters} instance.
     *
     * @param parameters    {@link CorfuRuntimeParameters} to configure the runtime with.
     */
    private CorfuRuntime(@Nonnull CorfuRuntimeParameters parameters) {
        // Set the local parameters field
        this.parameters = parameters;

        // Populate the initial set of layout servers
        layoutServers = parameters.getLayoutServers().stream()
                                .map(NodeLocator::toString)
                                .collect(Collectors.toList());

        // Generate the map of routers
        nodeRouters = new ConcurrentHashMap<>();

        // Set the initial cluster Id
        clusterId = parameters.getClusterId();

        // Generate or set the NettyEventLoop
        nettyEventLoop = parameters.nettyEventLoop == null ? getNewEventLoopGroup()
                                                            : parameters.nettyEventLoop;

        // Generate or set the ioExecutor
        ioExecutor = parameters.ioExecutor == null ? getNewIoExecutor()
                                                    : parameters.ioExecutor;

        // Generate a new layout future. When the layout is invalidated for the
        // first time, this future will be completed.
        layout = new CompletableFuture<>();

        // If automatic connection is requested, we will asynchronously fetch
        // and update the layout.
        if (parameters.autoConnect) {
            ioExecutor.submit(this::invalidateLayout);
        }

        synchronized (metrics) {
            if (metrics.getNames().isEmpty()) {
                MetricsUtils.metricsReportingSetup(metrics);
            }
        }
        log.info("Corfu runtime version {} initialized.", getVersionString());
    }

    /**
     * Shutdown the runtime, stop asynchronous tasks and free resources.
     *
     * <p>Once the runtime is shutdown, requests will fail with an exception and the
     * runtime cannot be reused. {@link this#isShutdown()} will return true to reflect
     * that the runtime is shutdown.
     *
     * <p>This method is synchronized to prevent multiple callers from initiating
     * a shutdown, and is therefore thread-safe (the runtime will only be shutdown once).
     */
    public synchronized void shutdown() {
        // If we are already shutdown, throw a shutdown error.
        if (isShutdown) {
            return;
        }

        log.info("shutdown: Corfu runtime shutting down");
        // Set the shutdown flag to true. This stops any active layout acquisition
        // from retrying.
        isShutdown = true;

        // Generate a new layout which completes exceptionally. This stops any thread
        // which is trying to get a layout.
        layout = new CompletableFuture<>();
        layout.completeExceptionally(new RuntimeShutdownError());

        // Stop each active router. This stops network RPC.
        nodeRouters.forEach((name, router) -> router.stop());

        // Shutdown our executors, if requested.
        if (parameters.shutdownIoExecutor) {
            ioExecutor.shutdown();
        }

        // Shutdown the event loop, if requested.
        if (parameters.shutdownNettyEventLoop) {
            nettyEventLoop.shutdownGracefully().syncUninterruptibly();
        }


        log.info("shutdown: Corfu runtime successfully shutdown");
    }

    /** Connect to the Corfu cluster.
     *
     * <p>When the method returns, the Corfu cluster is connected and the {@link CorfuRuntime}
     * can access Corfu cluster services.
     *
     * @return  This {@link CorfuRuntime}, to support chaining calls.
     */
    public CorfuRuntime connect() {
        if (!layout.isDone()) {
            invalidateLayout();
        }
        return this;
    }

    /** Asynchronously connect to the Corfu cluster.
     *
     *  <p>When the connection to the Corfu cluster is completed, the supplied future
     *  will be completed and the runtime can access Corfu cluster services.
     *
     *  @return A future which is completed when the Corfu cluster is connected.
     */
    @SuppressWarnings("unchecked")
    public Future<Void> connectAsync() {
        if (!layout.isDone()) {
            // Submit a request to invalidate the layout on an IO thread.
            ioExecutor.submit(this::invalidateLayout);
            // This cast is needed because the caller should not expect a layout in the
            // return (the return is void).
            return (Future) layout;
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    /**
     * Invalidate and update the current layout.
     *
     * <p>This function updates the current layout by connecting to each known
     * layout servers and checking if a new layout is available. If a new layout
     * is available, {@link this#layout} will be completed with the new layout.
     *
     * <p>While a retrieval is in progress, {@link this#layout} will be replaced
     * with a incomplete future. This forces clients which retrieve the future
     * after {@link this#layout} has been invalidated to wait for the layout to be
     * updated.
     *
     * <p>This function is thread-safe. Multiple threads may call this method
     * simultaneously, {@link this#layoutLock} ensures that only a single thread
     * retrieves the layout. This thread obtains a write lock while it calls the
     * internal method {@link this#fetchLayout()} which does the actual work of getting
     * a new layout. All other threads obtain a read lock, which blocks until the writer
     * thread is complete. Once the writer completes, the readers unblock and exit
     * without updating the layout, since the writer thread has completed their
     * writer for them.
     */
    public void invalidateLayout() {
        // First, we attempt to grab the layout lock
        long ts = layoutLock.tryWriteLock();
        try {
            if (ts == 0) {
                // If we enter this function and a layout retrieval is in progress,
                // (i.e. the lock is taken and ts is 0)
                // We wait for the lock to be released and the layout to be updated
                // by taking a read lock. When the call to readLock unblocks,
                // the layout has been updated by the writer thread.
                // The read lock will be released by the try-finally block.
                ts = layoutLock.readLock();
            } else {
                // Otherwise, we have the lock and can be guaranteed we are that we will be the
                // only thread updating the layout.
                log.info("invalidateLayout: Attempting layout update...");

                // First, invalidate the layout by installing a new future. All clients
                // which were working on the previous layout will eventually encounter
                // a WrongEpochException if there really was a layout issue.
                //
                // If this is the first time we are run, the layout will not be done, so
                // we don't generate a new future (since clients waiting may have used that
                // layout.
                if (layout.isDone()) {
                    layout = new CompletableFuture<>();
                }

                // Fetch the layout.
                // When the task is complete, the layout future will be completed with the layout.
                fetchLayout();
            }
        } finally {
            // Release the lock.
            layoutLock.unlock(ts);
        }
    }



    /**
     * Get a UUID for a named stream.
     *
     * @param string The name of the stream.
     * @return The ID of the stream.
     */
    @SuppressWarnings("checkstyle:abbreviation")
    public static UUID getStreamID(String string) {
        return UUID.nameUUIDFromBytes(string.getBytes());
    }

    public static UUID getCheckpointStreamIdFromId(UUID streamId) {
        return getStreamID(streamId.toString() + StreamsView.CHECKPOINT_SUFFIX);
    }

    public static UUID getCheckpointStreamIdFromName(String streamName) {
        return getCheckpointStreamIdFromId(CorfuRuntime.getStreamID(streamName));
    }

    /**
     * Get corfu runtime version.
     **/
    public static String getVersionString() {
        if (Version.getVersionString().contains("SNAPSHOT")
                || Version.getVersionString().contains("source")) {
            return Version.getVersionString() + "("
                + GitRepositoryState.getRepositoryState().commitIdAbbrev + ")";
        }
        return Version.getVersionString();
    }

    /**
     * If enabled, successful transactions will be written to a special transaction stream
     * (i.e. TRANSACTION_STREAM_ID)
     *
     * @param enable indicates if transaction logging is enabled
     * @return corfu runtime object
     */
    public CorfuRuntime setTransactionLogging(boolean enable) {
        this.getObjectsView().setTransactionLogging(enable);
        return this;
    }

    /**
     * Parse a configuration string and get a CorfuRuntime.
     *
     * @param configurationString The configuration string to parse.
     * @return A CorfuRuntime Configured based on the configuration string.
     */
    public CorfuRuntime parseConfigurationString(String configurationString) {
        // Parse comma sep. list.
        layoutServers = Pattern.compile(",")
            .splitAsStream(configurationString)
            .map(String::trim)
            .collect(Collectors.toList());
        return this;
    }

    /**
     * Get a router, given the address.
     *
     * @param address The address of the router to get.
     * @return The router.
     */
    public IClientRouter getRouter(String address) {
        return getRouterFunction.apply(address);
    }


    /** Function which is called whenever the runtime encounters an uncaught thread.
     *
     * @param thread        The thread which terminated.
     * @param throwable     The throwable which caused the thread to terminate.
     */
    private void handleUncaughtThread(@Nonnull Thread thread, @Nonnull Throwable throwable) {
        if (parameters.getUncaughtExceptionHandler() != null) {
            parameters.getUncaughtExceptionHandler().uncaughtException(thread, throwable);
        } else {
            log.error("handleUncaughtThread: {} terminated with throwable of type {}",
                    thread.getName(),
                    throwable.getClass().getSimpleName(),
                    throwable);
        }
    }

    /** Get a new {@link EventLoopGroup} for scheduling threads for Netty. The
     *  {@link EventLoopGroup} is typically passed to a router.
     *
     * @return  An {@link EventLoopGroup}.
     */
    private EventLoopGroup getNewEventLoopGroup() {
        // Calculate the number of threads which should be available in the thread pool.
        int numThreads = parameters.nettyEventLoopThreads == 0
                ? Runtime.getRuntime().availableProcessors() * 2 :
                parameters.nettyEventLoopThreads;
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(parameters.getPrefixedThreadFormat(
                                    parameters.nettyEventLoopThreadFormat))
                .setUncaughtExceptionHandler(this::handleUncaughtThread)
                .build();
        return parameters.socketType.getGenerator().generate(numThreads, factory);
    }

    /** Get a new {@link ExecutorService} for scheduling IO-bound tasks.
     *
     * @return  An {@link ExecutorService}.
     */
    private ExecutorService getNewIoExecutor() {
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(parameters.getPrefixedThreadFormat(
                        parameters.ioExecutorThreadFormat))
                .setUncaughtExceptionHandler(this::handleUncaughtThread)
                .build();
        return Executors.newCachedThreadPool(factory);
    }

    /** Fetch a new layout, completing the layout in {@link this#layout}.
     *
     *  <p>The {@link this#layoutLock} ensures that there is only a single invocation of this
     *  method at a time. When the method is executed, we fulfill the layout, or complete it
     *  exceptionally if we could not retrieve the layout.
     *
     *  <p>This method tries to fetch a layout from each server in {@link this#layoutServers}
     *  in random order. If all layout servers are unavailable, then we sleep for the duration
     *  specified in {@link CorfuRuntimeParameters#connectionRetryRate} before retrying.
     *
     *  <p>The fetching thread can be stopped by setting {@link this#isShutdown} to true. This
     *  will cause the fetching thread to terminate on the next retry with a
     *  {@link UnrecoverableCorfuError}.
     */
    private void fetchLayout() {
        if (layout.isDone()) {
            log.warn("fetchLayout: Requested new layout but future already done!");
            return;
        }

        while (!isShutdown) {
            // Every pass, we want to check using the current list of known
            // layout servers in random order. We make sure to connect to
            // all layout servers on the list in each pass.
            List<String> serversToCheck = new ArrayList<>(layoutServers);
            Collections.shuffle(serversToCheck);

            for (String server : serversToCheck) {
                try {
                    // Get the layout from the server we chose.
                    final Layout newLayout = getLayoutFromServer(server);

                    // Update the epoch of all the active routers
                    newLayout.getAllServers().forEach((name) -> {
                        IClientRouter router = getRouter(name);
                        log.debug("fetchLayout: Updating the epoch for {} from {} to {}",
                                name,
                                router.getEpoch(),
                                newLayout.getEpoch());
                        router.setEpoch(newLayout.getEpoch());
                    });

                    // And remove routers which we're no longer using
                    Sets.difference(nodeRouters.keySet(), newLayout.getAllServers())
                            .immutableCopy()
                            .forEach(e -> {
                                nodeRouters.get(e).stop();
                                nodeRouters.remove(e);
                            });

                    // Use the new layout's list of servers
                    layoutServers = newLayout.getLayoutServers();

                    // Update the future, releasing any clients waiting on it and finishing
                    // our task of updating the layout.
                    layout.complete(newLayout);

                    // Check the version of each layout server and log (informational)
                    logServerVersions();

                    // If fast loading was requested, load maps before restarting.
                    if (parameters.isUseFastLoader()
                                && getObjectsView().getObjectCache().isEmpty()) {
                        FastObjectLoader fastLoader = new FastObjectLoader(this)
                                .setBatchReadSize(parameters.getBulkReadSize())
                                .setTimeoutInMinutesForLoading(
                                    (int) parameters.fastLoaderTimeout.toMinutes());
                        fastLoader.loadMaps();
                    }
                    log.info("fetchLayout: Successfully installed new layout from {} in epoch {}",
                            server, newLayout.getEpoch());
                    return;
                } catch (RequestTimeoutException | NetworkException | WrongEpochException
                            | WrongClusterException | TimeoutException | NoBootstrapException e) {
                    // We had a problem connecting to this layout server, so we retry using the
                    // next server in the list.
                    log.info("fetchLayout: {} connecting to {}",
                            e.getClass().getSimpleName(), server, e);
                }
            }
            // Run the system down handler.
            try {
                systemDownHandler.run();
            } catch (SystemUnavailableError e) {
                layout.completeExceptionally(e);
                log.error("invalidateLayout: Terminating due to system unavailable request", e);
                return;
            }
            log.warn("fetchLayout: Couldn't connect to any layout servers, retrying in {}",
                    parameters.connectionRetryRate);
            // Sleep and retry
            Sleep.sleepUninterruptibly(parameters.connectionRetryRate);
        }

        log.warn("fetchLayout: Terminated due to shutdown, cancelling future.");
        layout.completeExceptionally(new RuntimeShutdownError());
        throw new RuntimeShutdownError();
    }

    /** Get a layout from a layout server.
     *
     * @param server                The server to fetch a {@link Layout} from.
     * @return                      The {@link Layout} retrieved from the server.
     * @throws TimeoutException     If the server timed out servicing the request.
     * @throws NetworkException     If there was a network issue communicating to the server.
     * @throws WrongEpochException  If the remote server was in the wrong epoch.
     */
    private Layout getLayoutFromServer(@Nonnull String server)
            throws TimeoutException {
        log.info("getLayoutFromServer: Trying {}", server);
        IClientRouter router = getRouter(server);
        // Try to get a layout.
        Layout layout =
                CFUtils.getUninterruptibly(router.getClient(LayoutClient.class).getLayout(),
                        TimeoutException.class, NetworkException.class, WrongEpochException.class);

        // If the layout we got has a smaller epoch than the router,
        // we discard it.
        if (layout.getEpoch() < router.getEpoch()) {
            log.warn("fetchLayout: Received a layout with epoch {} from server "
                    + "{}:{} smaller than router epoch {}, discarded.",
                    layout.getEpoch(), router.getHost(), router.getPort(), router.getEpoch());
            throw new WrongEpochException(router.getEpoch());
        }

        checkClusterId(layout);
        layout.setRuntime(this);

        return layout;
    }

    /** Check if the cluster Id of the layout matches the client cluster Id.
     *  If the client cluster Id is null, we update the client cluster Id.
     *
     *  <p>If both the layout cluster Id and the client cluster Id is null, the check is skipped.
     *  This can only occur in the case of legacy cluster without a cluster Id.
     *
     *  @param  layout  The layout to check.
     *  @throws WrongClusterException If the layout belongs to the wrong cluster.
     */
    private void checkClusterId(@Nonnull Layout layout) {
        // We haven't adopted a clusterId yet.
        if (clusterId == null) {
            clusterId = layout.getClusterId();
            log.info("Connected to new cluster {}", clusterId == null ? "(legacy)" :
                    UuidUtils.asBase64(clusterId));
        } else if (!clusterId.equals(layout.getClusterId())) {
            // We connected but got a cluster id we didn't expect.
            throw new WrongClusterException(clusterId, layout.getClusterId());
        }
    }

    /** Log the version information for each server on the console.
     *  Does not cause a failure in case of a version mismatch.
     */
    @SuppressWarnings("unchecked")
    private void logServerVersions() {
        // Get a map of servers to futures with version information.
        Map<String, CompletableFuture<VersionInfo>> futures =
                CFUtils.getUninterruptibly(layout).getAllServers().stream()
                .collect(Collectors.toMap(
                    // Key = server string
                    s -> s,
                    // Value = VersionInfo future
                    s -> getRouter(s).getClient(BaseClient.class).getVersionInfo()));

        // Print out each version information.
        futures.forEach((server, versionFuture) -> {
            try {
                VersionInfo version = CFUtils.getUninterruptibly(versionFuture,
                        TimeoutException.class,
                        NetworkException.class);
                if (version.getVersion() == null) {
                    log.error("logServerVersions[{}]: Unexpected version, "
                            + "server is too old to return version information", server);
                } else if (!version.getVersion().equals(getVersionString())) {
                    log.error("logServerVersions[{}]: expected version {}, "
                            + "but server version is {}",
                            server, getVersionString(), version.getVersion());
                } else {
                    log.info("logServerVersions[{}]: client version {}, server version is {}",
                            server, getVersionString(), version.getVersion());
                }
            } catch (TimeoutException | NetworkException e) {
                log.warn("logServerVersions[{}]: Attempted to get server version "
                        + "but hit an exception", server, e);
            }
        });
    }


    // Below are deprecated methods which should no longer be
    // used and may be deprecated in the future.

    // region Deprecated Constructors
    /**
     * Constructor for CorfuRuntime.
     * @deprecated Use {@link CorfuRuntime#fromParameters(CorfuRuntimeParameters)}
     **/
    @Deprecated
    public CorfuRuntime() {
        this(CorfuRuntimeParameters.builder().build());
    }

    /**
     * Parse a configuration string and get a CorfuRuntime.
     *
     * @param configurationString The configuration string to parse.
     * @deprecated Use {@link CorfuRuntime#fromParameters(CorfuRuntimeParameters)}
     */
    @Deprecated
    public CorfuRuntime(String configurationString) {
        this(CorfuRuntimeParameters.builder().build());
        this.parseConfigurationString(configurationString);
    }
    // endregion

    // region Deprecated Setters

    /**
     * Enable TLS.
     * @deprecated  Deprecated, set using {@link CorfuRuntimeParameters} instead.
     **/
    @Deprecated
    public CorfuRuntime enableTls(String keyStore, String ksPasswordFile, String trustStore,
            String tsPasswordFile) {
        log.warn("enableTls: Deprecated, please set parameters instead");
        parameters.keyStore = keyStore;
        parameters.ksPasswordFile = ksPasswordFile;
        parameters.trustStore = trustStore;
        parameters.tsPasswordFile = tsPasswordFile;
        parameters.tlsEnabled = true;
        return this;
    }

    /**
     * Enable SASL Plain Text.
     * @deprecated  Deprecated, set using {@link CorfuRuntimeParameters} instead.
     **/
    @Deprecated
    public CorfuRuntime enableSaslPlainText(String usernameFile, String passwordFile) {
        log.warn("enableSaslPlainText: Deprecated, please set parameters instead");
        parameters.usernameFile = usernameFile;
        parameters.passwordFile = passwordFile;
        parameters.saslPlainTextEnabled = true;
        return this;
    }

    /**
     * Whether or not to disable backpointers
     *
     * @param disable True, if the cache should be disabled, false otherwise.
     * @return A CorfuRuntime to support chaining.
     * @deprecated  Deprecated, set using {@link CorfuRuntimeParameters} instead.
     */
    @Deprecated
    public CorfuRuntime setBackpointersDisabled(boolean disable) {
        log.warn("setBackpointersDisabled: Deprecated, please set parameters instead");
        parameters.setBackpointersDisabled(disable);
        return this;
    }

    /**
     * Whether or not to disable the cache
     *
     * @param disable True, if the cache should be disabled, false otherwise.
     * @return A CorfuRuntime to support chaining.
     * @deprecated  Deprecated, set using {@link CorfuRuntimeParameters} instead.
     */
    @Deprecated
    public CorfuRuntime setCacheDisabled(boolean disable) {
        log.warn("setCacheDisabled: Deprecated, please set parameters instead");
        parameters.setCacheDisabled(disable);
        return this;
    }

    /**
     * Whether or not to use the fast loader.
     *
     * @param enable True, if the fast loader should be used, false otherwise.
     * @return A CorfuRuntime to support chaining.
     * @deprecated  Deprecated, set using {@link CorfuRuntimeParameters} instead.
     */
    @Deprecated
    public CorfuRuntime setLoadSmrMapsAtConnect(boolean enable) {
        log.warn("setLoadSmrMapsAtConnect: Deprecated, please set parameters instead");
        parameters.setUseFastLoader(enable);
        return this;
    }

    /**
     * Whether or not hole filling is disabled
     *
     * @param disable True, if hole filling should be disabled
     * @return A CorfuRuntime to support chaining.
     * @deprecated  Deprecated, set using {@link CorfuRuntimeParameters} instead.
     */
    @Deprecated
    public CorfuRuntime setHoleFillingDisabled(boolean disable) {
        log.warn("setHoleFillingDisabled: Deprecated, please set parameters instead");
        parameters.setHoleFillingDisabled(disable);
        return this;
    }

    /** Set the number of cache entries.
     *
     * @param numCacheEntries   The number of cache entries.
     * @deprecated  Deprecated, set using {@link CorfuRuntimeParameters} instead.
     */
    @Deprecated
    public CorfuRuntime setNumCacheEntries(long numCacheEntries) {
        log.warn("setNumCacheEntries: Deprecated, please set parameters instead");
        parameters.setNumCacheEntries(numCacheEntries);
        return this;
    }

    /** Set the cache expiration time.
     *
     * @param expiryTime   The time before cache expiration, in seconds.
     * @deprecated  Deprecated, set using {@link CorfuRuntimeParameters} instead.
     */
    @Deprecated
    public CorfuRuntime setCacheExpiryTime(int expiryTime) {
        log.warn("setCacheExpiryTime: Deprecated, please set parameters instead");
        parameters.setCacheExpiryTime(expiryTime);
        return this;
    }

    /** Set the bulk read size.
     *
     * @param size  The bulk read size.
     * @deprecated  Deprecated, set using {@link CorfuRuntimeParameters} instead.
     */
    @Deprecated
    public CorfuRuntime setBulkReadSize(int size) {
        log.warn("setBulkReadSize: Deprecated, please set parameters instead");
        parameters.setBulkReadSize(size);
        return this;
    }


    /** Set the write retry time.
     *
     * @param writeRetry   The number of times to retry writes.
     * @deprecated  Deprecated, set using {@link CorfuRuntimeParameters} instead.
     */
    @Deprecated
    public CorfuRuntime setWriteRetry(int writeRetry) {
        log.warn("setWriteRetry: Deprecated, please set parameters instead");
        parameters.setWriteRetry(writeRetry);
        return this;
    }

    /** Set the trim retry time.
     *
     * @param trimRetry   The number of times to retry on trims.
     * @deprecated  Deprecated, set using {@link CorfuRuntimeParameters} instead.
     */
    @Deprecated
    public CorfuRuntime setTrimRetry(int trimRetry) {
        log.warn("setTrimRetry: Deprecated, please set parameters instead");
        parameters.setWriteRetry(trimRetry);
        return this;
    }

    /** Set the timeout of the fast loader, in minutes.
     *
     * @param timeout   The number of minutes to wait.
     * @deprecated  Deprecated, set using {@link CorfuRuntimeParameters} instead.
     */
    @Deprecated
    public CorfuRuntime setTimeoutInMinutesForFastLoading(int timeout) {
        log.warn("setTrimRetry: Deprecated, please set parameters instead");
        parameters.setFastLoaderTimeout(Duration.ofMinutes(timeout));
        return this;
    }


    /**
     * Add a layout server to the list of servers known by the CorfuRuntime.
     *
     * @param layoutServer A layout server to use.
     * @return A CorfuRuntime, to support the builder pattern.
     * @deprecated This method is deprecated. Please use the {@link CorfuRuntimeParameters} object.
     */
    @Deprecated
    public CorfuRuntime addLayoutServer(@Nonnull String layoutServer) {
        layoutServers.add(layoutServer);
        return this;
    }

    /**
     * Stop all routers associated with this runtime & disconnect them.
     *
     * @deprecated It is no longer possible to "stop" but not "shutdown" the runtime.
     *             Please call {@link this#shutdown()} instead.
     */
    @Deprecated
    public void stop() {
        log.warn("stop: Calling deprecated stop() method, call shutdown() instead");
        stop(false);
    }

    /**
     * Stop all routers associated with this runtime & disconnect them.
     *
     * @deprecated It is no longer possible to "stop" but not "shutdown" the runtime.
     *             Please call {@link this#shutdown()} instead.
     */
    @Deprecated
    public void stop(boolean shutdown) {
        shutdown();
    }
    // endregion
}
