package org.corfudb.runtime;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.Getter;
import lombok.Singular;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.comm.ChannelImplementation;
import org.corfudb.protocols.wireprotocol.MsgHandlingFilter;
import org.corfudb.protocols.wireprotocol.PriorityLevel;
import org.corfudb.protocols.wireprotocol.VersionInfo;
import org.corfudb.recovery.FastObjectLoader;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LayoutHandler;
import org.corfudb.runtime.clients.LogUnitHandler;
import org.corfudb.runtime.clients.ManagementHandler;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.clients.SequencerHandler;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
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

import javax.annotation.Nonnull;
import java.lang.Thread.UncaughtExceptionHandler;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by mwei on 12/9/15.
 */
@Slf4j
@Accessors(chain = true)
public class CorfuRuntime {

    /**
     * A class which holds parameters and settings for the {@link CorfuRuntime}.
     */
    @Builder
    @Data
    @ToString
    public static class CorfuRuntimeParameters {
        @Default
        private final long nettyShutdownQuitePeriod = 100;
        @Default
        private final long nettyShutdownTimeout = 300;

        // region Object Layer Parameters
        /**
         * True, if undo logging is disabled.
         */
        @Default
        boolean undoDisabled = false;

        /**
         * True, if optimistic undo logging is disabled.
         */
        @Default
        boolean optimisticUndoDisabled = false;

        /**
         * Max size for a write request.
         */
        @Default
        int maxWriteSize = 0;

        /**
         * Use fast loader to restore objects on connection.
         *
         * <p>If using this utility, you need to be sure that no one
         * is accessing objects until the tables are loaded
         * (i.e. when connect returns)
         */
        @Default
        boolean useFastLoader = false;

        /**
         * Set the bulk read size.
         */
        @Default
        int bulkReadSize = 10;

        /**
         * How much time the Fast Loader has to get the maps up to date.
         *
         * <p>Once the timeout is reached, the Fast Loader gives up. Every map that is
         * not up to date will be loaded through normal path.
         */
        @Default
        Duration fastLoaderTimeout = Duration.ofMinutes(30);
        // endregion

        // region Address Space Parameters
        /**
         * Number of times to attempt to read before hole filling.
         * @deprecated This is a no-op. Use holeFillWait
         * */
        @Deprecated
        @Default int holeFillRetry = 10;

        /** Time to wait between read requests reattempts before hole filling. */
        @Default Duration holeFillRetryThreshold = Duration.ofSeconds(1L);

        /**
         * Time limit after which the reader gives up and fills the hole.
         */
        @Default Duration holeFillTimeout = Duration.ofSeconds(10);

        /**
         * Whether or not to disable the cache.
         */
        @Default
        boolean cacheDisabled = false;

        /**
         * The maximum number of entries in the cache.
         */
        @Default
        long maxCacheEntries;

        /**
         * The max in-memory size of the cache in bytes
         */
        @Default
        long maxCacheWeight;

        /**
         * This is a hint to size the AddressSpaceView cache, a higher concurrency
         * level allows for less lock contention at the cost of more memory overhead.
         * The default value of zero will result in using the cache's internal default
         * concurrency level (i.e. 4). 
         */
        @Default
        int cacheConcurrencyLevel = 0;

        /**
         * Sets expireAfterAccess and expireAfterWrite in seconds.
         */
        @Default
        long cacheExpiryTime = Long.MAX_VALUE;
        // endregion

        // region Handshake Parameters
        /**
         * Sets handshake timeout in seconds.
         */
        @Default
        int handshakeTimeout = 10;
        // endregion

        // region Stream Parameters
        /**
         * True, if strategy to discover the address space of a stream relies on the follow backpointers.
         * False, if strategy to discover the address space of a stream relies on the get stream address map.
         */
        @Default
        boolean followBackpointersEnabled = false;

        /**
         * Whether or not to disable backpointers.
         */
        @Default
        boolean backpointersDisabled = false;

        /**
         * Whether or not hole filling should be disabled.
         */
        @Default
        boolean holeFillingDisabled = false;

        /**
         * Number of times to retry on an
         * {@link org.corfudb.runtime.exceptions.OverwriteException} before giving up.
         */
        @Default
        int writeRetry = 5;

        /**
         * The number of times to retry on a retriable
         * {@link org.corfudb.runtime.exceptions.TrimmedException} during a transaction.
         */
        @Default
        int trimRetry = 2;

        /**
         * Stream Batch Size: number of addresses to fetch in advance when stream address discovery mechanism
         * relies on address maps instead of follow backpointers, i.e., followBackpointersEnabled = false;
         */
        @Default
        int streamBatchSize = 10;

        /**
         * Checkpoint read Batch Size: number of checkpoint addresses to fetch in batch when stream
         * address discovery mechanism relies on address maps instead of follow backpointers;
         */
        @Default
        int checkpointReadBatchSize = 5;
        // endregion

        //region        Security parameters
        /**
         * True, if TLS is enabled.
         */
        @Default
        boolean tlsEnabled = false;

        /**
         * A path to the key store.
         */
        String keyStore;

        /**
         * A file containing the password for the key store.
         */
        String ksPasswordFile;

        /**
         * A path to the trust store.
         */
        String trustStore;

        /**
         * A path containing the password for the trust store.
         */
        String tsPasswordFile;

        /**
         * True, if SASL plain text authentication is enabled.
         */
        @Default
        boolean saslPlainTextEnabled = false;

        /**
         * A path containing the username file for SASL.
         */
        String usernameFile;

        /**
         * A path containing the password file for SASL.
         */
        String passwordFile;
        //endregion

        //region Connection parameters
        /**
         * {@link Duration} before requests timeout.
         * This is the duration after which the reader hole fills the address.
         */
        @Default
        Duration requestTimeout = Duration.ofSeconds(5);

        /**
         * This timeout (in seconds) is used to detect servers that
         * shutdown abruptly without terminating the connection properly.
         */
        @Default
        int idleConnectionTimeout = 7;

        /**
         * The period at which the client sends keep-alive messages to the
         * server (a message is only send there is no write activity on the channel
         * for the whole period.
         */
        @Default
        int keepAlivePeriod = 2;

        /**
         * {@link Duration} before connections timeout.
         */
        @Default
        Duration connectionTimeout = Duration.ofMillis(500);

        /**
         * {@link Duration} before reconnecting to a disconnected node.
         */
        @Default
        Duration connectionRetryRate = Duration.ofSeconds(1);

        /**
         * The period at which the runtime will run garbage collection
         */
        @Default
        Duration runtimeGCPeriod = Duration.ofMinutes(20);

        /**
         * The {@link UUID} for this client. Randomly generated by default.
         */
        @Default
        UUID clientId = UUID.randomUUID();

        /**
         * The {@link UUID} for the cluster this client is connecting to, or
         * {@code null} if the client should adopt the {@link UUID} of the first
         * server it connects to.
         */
        @Default
        UUID clusterId = null;

        /**
         * The type of socket which {@link NettyClientRouter}s should use. By default,
         * an NIO based implementation is used.
         */
        @Default
        ChannelImplementation socketType = ChannelImplementation.NIO;

        /**
         * Number of retries to reconnect to an unresponsive system before invoking the
         * systemDownHandler. This is mainly required to allow the fault detection mechanism
         * to detect and reconfigure the cluster.
         * The fault detection takes at least 3 seconds to recognize a failure.
         * Each retry is attempted after a sleep of {@literal connectionRetryRate}
         * invoking the systemDownHandler after a minimum of
         * (systemDownHandlerTriggerLimit * connectionRetryRate) seconds. Default: 20 seconds.
         */
        @Default
        int systemDownHandlerTriggerLimit = 20;

        /**
         * The initial list of layout servers.
         */
        @Singular
        List<NodeLocator> layoutServers;
        //endregion

        //region Threading Parameters
        /**
         * The {@link EventLoopGroup} which {@link NettyClientRouter}s will use.
         * If not specified, the runtime will generate this using the {@link ChannelImplementation}
         * specified in {@code socketType} and the {@link ThreadFactory} specified in
         * {@code nettyThreadFactory}.
         */
        EventLoopGroup nettyEventLoop;

        /**
         * A string which will be used to set the
         * {@link com.google.common.util.concurrent.ThreadFactoryBuilder#nameFormat} for the
         * {@code nettyThreadFactory}. By default, this is set to "netty-%d".
         * If you provide your own {@code nettyEventLoop}, this field is ignored.
         */
        @Default
        String nettyEventLoopThreadFormat = "netty-%d";

        /**
         * The number of threads that should be available in the {@link NettyClientRouter}'s
         * event pool. 0 means that we will use 2x the number of processors reported in the
         * system. If you provide your own {@code nettyEventLoop}, this field is ignored.
         */
        @Default
        int nettyEventLoopThreads = 0;

        /**
         * True, if the {@code NettyEventLoop} should be shutdown when the runtime is
         * shutdown. False otherwise.
         */
        @Default
        boolean shutdownNettyEventLoop = true;

        /**
         * Netty channel options, if provided. If no options are set, we default to
         * the defaults in {@link this#DEFAULT_CHANNEL_OPTIONS}.
         */
        @Singular
        Map<ChannelOption, Object> customNettyChannelOptions;

        /**
         * Default channel options, used if there are no options in the
         * {@link this#customNettyChannelOptions} field.
         */
        static final Map<ChannelOption, Object> DEFAULT_CHANNEL_OPTIONS =
                ImmutableMap.<ChannelOption, Object>builder()
                        .put(ChannelOption.TCP_NODELAY, true)
                        .put(ChannelOption.SO_REUSEADDR, true)
                        .build();

        /**
         * Get the netty channel options to be used by the netty client implementation.
         *
         * @return A map containing options which should be applied to each netty channel.
         */
        public Map<ChannelOption, Object> getNettyChannelOptions() {
            return customNettyChannelOptions.size() == 0
                    ? DEFAULT_CHANNEL_OPTIONS : customNettyChannelOptions;
        }

        /**
         * A {@link UncaughtExceptionHandler} which handles threads that have an uncaught
         * exception. Used on all {@link ThreadFactory}s the runtime creates, but if you
         * generate your own thread factory, this field is ignored. If this field is not set,
         * the runtime's default handler runs, which logs an error level message.
         */
        UncaughtExceptionHandler uncaughtExceptionHandler;
        //endregion

        /**
         * The number of times to retry invalidate when a layout change is expected.
         */
        @Default
        int invalidateRetry = 5;

        /**
         * Represents filtering logic to be applied on the inbound messages received by Netty Client
         * Router. If filters are not null, Netty Client Router add a filter handler and configures it
         * with provided filters. If filters are null, no filter handler will be added to Netty's pipeline.
         */
        @Default
        List<MsgHandlingFilter> nettyClientInboundMsgFilters = null;

        // Register handlers region

        // These two handlers are provided to give some control on what happen when system is down.
        // For applications that want to have specific behaviour when a the system appears
        // unavailable, they can register their own handler for both before the rpc request and
        // upon cluster unreachability.
        // An example of how to use these handlers implementing timeout is given in
        // test/src/test/java/org/corfudb/runtime/CorfuRuntimeTest.java

        /**
         * SystemDownHandler is invoked at any point when the Corfu client attempts to make an RPC
         * request to the Corfu cluster but is unable to complete this.
         * NOTE: This will also be invoked during connect if the cluster is unreachable.
         */
        @Default
        volatile Runnable systemDownHandler = () -> {
        };

        /**
         * BeforeRPCHandler callback is invoked every time before an RPC call.
         */
        @Default
        volatile Runnable beforeRpcHandler = () -> {
        };
        //endregion

        /**
         * The default priority of the requests made by this client.
         * Under resource constraints non-high priority requests
         * are dropped.
         */
        @Default
        PriorityLevel priorityLevel = PriorityLevel.NORMAL;
    }

    /**
     * The parameters used to configure this {@link CorfuRuntime}.
     */
    @Getter
    private final CorfuRuntimeParameters parameters;

    /**
     * The {@link EventLoopGroup} provided to netty routers.
     */
    @Getter
    private final EventLoopGroup nettyEventLoop;

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

    /**
     * List of initial set of layout servers, i.e., servers specified in
     * connection string on bootstrap.
     */
    @Getter
    private volatile List<String> bootstrapLayoutServers;

    /**
     * List of known layout servers, refreshed on each fetchLayout.
     */
    @Getter
    private volatile List<String> layoutServers;

    /**
     * Node Router Pool.
     */
    @Getter
    private NodeRouterPool nodeRouterPool;

    /**
     * A completable future containing a layout, when completed.
     */
    public volatile CompletableFuture<Layout> layout;

    /**
     * The {@link UUID} of the cluster we are currently connected to, or null, if
     * there is no cluster yet.
     */
    @Getter
    public volatile UUID clusterId;

    @Getter
    final ViewsGarbageCollector garbageCollector = new ViewsGarbageCollector(this);

    /**
     * Notifies that the runtime is no longer used
     * and async retries to fetch the layout can be stopped.
     */
    @Getter
    private volatile boolean isShutdown = false;


    /**
     * This thread is used by fetchLayout to find a new layout in the system
     */
    final ExecutorService runtimeExecutor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("CorfuRuntime-%d")
            .build());

    /**
     * Latest layout seen by the runtime.
     */
    private volatile Layout latestLayout = null;

    @Getter
    private static final MetricRegistry defaultMetrics = new MetricRegistry();

    /** Initialize a default static registry which through that different metrics
     * can be registered and reported */
    static {
        synchronized (defaultMetrics) {
            MetricsUtils.metricsReportingSetup(defaultMetrics);
        }
    }

    /**
     * Register SystemDownHandler.
     * Please use CorfuRuntimeParameters builder to register this.
     *
     * @param handler Handler to invoke in case of system Down.
     * @return CorfuRuntime instance.
     */
    @Deprecated
    public CorfuRuntime registerSystemDownHandler(Runnable handler) {
        this.getParameters().setSystemDownHandler(handler);
        return this;
    }

    /**
     * Register BeforeRPCHandler.
     * Please use CorfuRuntimeParameters builder to register this.
     *
     * @param handler Handler to invoke before every RPC.
     * @return CorfuRuntime instance.
     */
    @Deprecated
    public CorfuRuntime registerBeforeRpcHandler(Runnable handler) {
        this.getParameters().setBeforeRpcHandler(handler);
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
    private final Function<String, IClientRouter> getRouterFunction =
            overrideGetRouterFunction != null ? (address) ->
                    overrideGetRouterFunction.apply(this, address) : (address) -> {
                NodeLocator node = NodeLocator.parseString(address);
                // Generate a new router, start it and add it to the table.
                NettyClientRouter newRouter = new NettyClientRouter(node,
                        getNettyEventLoop(),
                        getParameters());
                log.debug("Connecting to new router {}", node);
                try {
                    newRouter.addClient(new LayoutHandler())
                            .addClient(new SequencerHandler())
                            .addClient(new LogUnitHandler())
                            .addClient(new ManagementHandler());
                } catch (Exception e) {
                    log.warn("Error connecting to router", e);
                    throw e;
                }
                return newRouter;
            };

    /**
     * Factory method for generating new {@link CorfuRuntime}s given a set of
     * {@link CorfuRuntimeParameters} to configure the runtime with.
     *
     * @param parameters A {@link CorfuRuntimeParameters} to use.
     * @return A new {@link CorfuRuntime}.
     */
    public static CorfuRuntime fromParameters(@Nonnull CorfuRuntimeParameters parameters) {
        return new CorfuRuntime(parameters);
    }

    /**
     * Construct a new {@link CorfuRuntime} given a {@link CorfuRuntimeParameters} instance.
     *
     * @param parameters {@link CorfuRuntimeParameters} to configure the runtime with.
     */
    private CorfuRuntime(@Nonnull CorfuRuntimeParameters parameters) {
        // Set the local parameters field
        this.parameters = parameters;

        // Populate the initial set of layout servers
        bootstrapLayoutServers = parameters.getLayoutServers().stream()
                .map(NodeLocator::toString)
                .collect(Collectors.toList());

        // Initialize list of layout servers (this list will get updated for every new layout)
        layoutServers = new ArrayList<>(bootstrapLayoutServers);

        // Set the initial cluster Id
        clusterId = parameters.getClusterId();

        // Generate or set the NettyEventLoop
        nettyEventLoop = parameters.nettyEventLoop == null ? getNewEventLoopGroup()
                : parameters.nettyEventLoop;

        // Initializing the node router pool.
        nodeRouterPool = new NodeRouterPool(getRouterFunction);

        log.info("Corfu runtime version {} initialized.", getVersionString());
    }

    /**
     * Get a new {@link EventLoopGroup} for scheduling threads for Netty. The
     * {@link EventLoopGroup} is typically passed to a router.
     *
     * @return An {@link EventLoopGroup}.
     */
    private EventLoopGroup getNewEventLoopGroup() {
        // Calculate the number of threads which should be available in the thread pool.
        int numThreads = parameters.nettyEventLoopThreads == 0
                ? Runtime.getRuntime().availableProcessors() * 2 :
                parameters.nettyEventLoopThreads;
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(parameters.nettyEventLoopThreadFormat)
                .setUncaughtExceptionHandler(this::handleUncaughtThread)
                .build();
        return parameters.socketType.getGenerator().generate(numThreads, factory);
    }

    /**
     * Function which is called whenever the runtime encounters an uncaught thread.
     *
     * @param thread    The thread which terminated.
     * @param throwable The throwable which caused the thread to terminate.
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

    /**
     * Shuts down the CorfuRuntime.
     * Stops async tasks from fetching the layout.
     * Cannot reuse the runtime once shutdown is called.
     */
    public void shutdown() {
        // Stopping async task from fetching layout.
        isShutdown = true;
        garbageCollector.stop();
        runtimeExecutor.shutdownNow();
        if (layout != null) {
            try {
                layout.cancel(true);
            } catch (Exception e) {
                log.error("Runtime shutting down. Exception in terminating fetchLayout: {}", e);
            }
        }

        stop(true);

        // Shutdown the event loop
        if (parameters.shutdownNettyEventLoop) {
            nettyEventLoop.shutdownGracefully();
        }
    }

    /**
     * Stop all routers associated with this runtime & disconnect them.
     */
    public void stop() {
        stop(false);
    }

    /**
     * Stop all routers associated with this Corfu Runtime.
     **/
    public void stop(boolean shutdown) {
        nodeRouterPool.shutdown();
        if (!shutdown) {
            nodeRouterPool = new NodeRouterPool(getRouterFunction);
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
        bootstrapLayoutServers = Pattern.compile(",")
                .splitAsStream(configurationString)
                .map(String::trim)
                .collect(Collectors.toList());
        layoutServers = new ArrayList<>(bootstrapLayoutServers);
        return this;
    }

    /**
     * Add a layout server to the list of servers known by the CorfuRuntime.
     *
     * @param layoutServer A layout server to use.
     * @return A CorfuRuntime, to support the builder pattern.
     */
    public CorfuRuntime addLayoutServer(String layoutServer) {
        bootstrapLayoutServers.add(layoutServer);
        layoutServers.add(layoutServer);
        return this;
    }

    /**
     * Get a router, given the address.
     *
     * @param address The address of the router to get.
     * @return The router.
     */
    public IClientRouter getRouter(String address) {
        return nodeRouterPool.getRouter(NodeLocator.parseString(address));
    }

    /**
     * Invalidate the current layout.
     * If the layout has been previously invalidated and a new layout has not yet been retrieved,
     * this function does nothing.
     */
    public synchronized CompletableFuture<Layout> invalidateLayout() {
        // Is there a pending request to retrieve the layout?
        if (layout.isDone()) {
            List<String> servers = Optional.ofNullable(latestLayout)
                    .map(Layout::getLayoutServers)
                    .orElse(bootstrapLayoutServers);

            layout = fetchLayout(servers);
        }

        return layout;
    }

    /**
     * Check if the cluster Id of the layout matches the client cluster Id.
     * If the client cluster Id is null, we update the client cluster Id.
     * <p>
     * If both the layout cluster Id and the client cluster Id is null, the check is skipped.
     * This can only occur in the case of legacy cluster without a cluster Id.
     *
     * @param layout The layout to check.
     * @throws WrongClusterException If the layout belongs to the wrong cluster.
     */
    private void checkClusterId(@Nonnull Layout layout) {
        // We haven't adopted a clusterId yet.
        if (clusterId == null) {
            clusterId = layout.getClusterId();
            if (clusterId != null) {
                log.info("Connected to new cluster {}", UuidUtils.asBase64(clusterId));
            }
        } else if (!clusterId.equals(layout.getClusterId())) {
            // We connected but got a cluster id we didn't expect.
            throw new WrongClusterException(clusterId, layout.getClusterId());
        }
    }

    /**
     * Detects connections to nodes in the router pool which are no longer present in the layout.
     * For each of these nodes, the router is stopped and the reference is removed from the pool.
     * If this is not done, the reference remains and Netty keeps attempting to reconnect to the
     * disconnected node.
     *
     * @param layout The latest layout.
     */
    private void pruneRemovedRouters(@Nonnull Layout layout) {
        nodeRouterPool.getNodeRouters().keySet().stream()
                // Check if endpoint is present in the layout.
                .filter(endpoint -> !layout.getAllServers()
                        // Converting to legacy endpoint format as the layout only contains
                        // legacy format - host:port.
                        .contains(endpoint.toEndpointUrl()))
                .forEach(endpoint -> {
                    try {
                        IClientRouter router = nodeRouterPool.getNodeRouters().remove(endpoint);
                        if (router != null) {
                            // Stop the channel from keeping connecting/reconnecting to server.
                            // Also if channel is not closed properly, router will be garbage collected.
                            router.stop();
                        }
                    } catch (Exception e) {
                        log.warn("fetchLayout: Exception in stopping and removing "
                                + "router connection to node {} :", endpoint, e);
                    }
                });
    }

    /**
     * Return a completable future which is guaranteed to contain a layout.
     * This future will continue retrying until it gets a layout.
     * If you need this completable future to fail, you should chain it with a timeout.
     *
     * @param servers Layout servers to fetch the layout from.
     * @return A completable future containing a layout.
     */
    private CompletableFuture<Layout> fetchLayout(List<String> servers) {

        return CompletableFuture.supplyAsync(() -> {

            List<String> layoutServersCopy = new ArrayList<>(servers);
            parameters.getBeforeRpcHandler().run();
            int systemDownTriggerCounter = 0;

            while (true) {

                Collections.shuffle(layoutServersCopy);
                // Iterate through the layout servers, attempting to connect to one
                for (String s : layoutServersCopy) {
                    log.trace("Trying connection to layout server {}", s);
                    try {
                        IClientRouter router = getRouter(s);
                        // Try to get a layout.
                        CompletableFuture<Layout> layoutFuture =
                                new LayoutClient(router, Layout.INVALID_EPOCH).getLayout();
                        // Wait for layout
                        Layout l = layoutFuture.get();

                        // If the layout we got has a smaller epoch than the latestLayout epoch,
                        // we discard it.
                        if (latestLayout != null && latestLayout.getEpoch() > l.getEpoch()) {
                            log.warn("fetchLayout: Received a layout with epoch {} from server "
                                            + "{}:{} smaller than latestLayout epoch {}, "
                                            + "discarded.",
                                    l.getEpoch(), router.getHost(), router.getPort(),
                                    latestLayout.getEpoch());
                            continue;
                        }

                        checkClusterId(l);

                        // Update/refresh list of layout servers
                        this.layoutServers = l.getLayoutServers();

                        layout = layoutFuture;
                        latestLayout = l;
                        log.debug("Layout server {} responded with layout {}", s, l);

                        // Prune away removed node routers from the nodeRouterPool.
                        pruneRemovedRouters(l);

                        return l;
                    } catch (InterruptedException ie) {
                        throw new UnrecoverableCorfuInterruptedError(
                                "Interrupted during layout fetch", ie);
                    } catch (ExecutionException ee){
                        if (ee.getCause() instanceof TimeoutException) {
                            log.warn("Tried to get layout from {} but failed by timeout", s);
                        } else {
                            log.warn("Tried to get layout from {} but failed with exception:", s, ee);
                        }
                    }
                    catch (Exception e) {
                        log.warn("Tried to get layout from {} but failed with exception:", s, e);
                    }
                }

                log.warn("Couldn't connect to any up-to-date layout servers, retrying in {}, "
                                + "Retried {} times, systemDownHandlerTriggerLimit = {}",
                        parameters.connectionRetryRate, systemDownTriggerCounter,
                        parameters.getSystemDownHandlerTriggerLimit());

                if (++systemDownTriggerCounter >= parameters.getSystemDownHandlerTriggerLimit()) {
                    log.info("fetchLayout: Invoking the systemDownHandler.");
                    parameters.getSystemDownHandler().run();
                }

                Sleep.sleepUninterruptibly(parameters.connectionRetryRate);
                if (isShutdown) {
                    return null;
                }
            }
        }, runtimeExecutor);
    }

    @SuppressWarnings("unchecked")
    private void checkVersion() {
        try {
            Layout currentLayout = CFUtils.getUninterruptibly(layout);
            List<CompletableFuture<VersionInfo>> versions =
                    currentLayout.getLayoutServers()
                            .stream().map(s -> getLayoutView().getRuntimeLayout().getBaseClient(s))
                            .map(BaseClient::getVersionInfo)
                            .collect(Collectors.toList());

            for (CompletableFuture<VersionInfo> versionCf : versions) {
                final VersionInfo version = CFUtils.getUninterruptibly(versionCf,
                        TimeoutException.class, NetworkException.class);
                if (version.getVersion() == null) {
                    log.error("Unexpected server version, server is too old to return"
                            + " version information");
                } else if (!version.getVersion().equals(getVersionString())) {
                    log.error("connect: expected version {}, but server version is {}",
                            getVersionString(), version.getVersion());
                } else {
                    log.info("connect: client version {}, server version is {}",
                            getVersionString(), version.getVersion());
                }
            }
        } catch (TimeoutException | NetworkException e) {
            log.error("connect: failed to get version. Couldn't connect to server.", e);
        } catch (Exception ex) {
            // Because checkVersion is just an informational step (log purpose), we don't need to retry
            // and we can actually ignore any exception while trying to fetch the server corfu version.
            // If at any point we decide to abort upon server mismatch this logic must change.
            log.error("connect: failed to get version.", ex);
        }
    }

    /**
     * Connect to the Corfu server instance.
     * When this function returns, the Corfu server is ready to be accessed.
     */
    public synchronized CorfuRuntime connect() {

        log.info("connect: runtime parameters {}", getParameters());

        if (layout == null) {
            log.info("Connecting to Corfu server instance, layout servers={}", bootstrapLayoutServers);
            // Fetch the current layout and save the future.
            layout = fetchLayout(bootstrapLayoutServers);
            try {
                layout.get();
            } catch (Exception e) {
                // A serious error occurred trying to connect to the Corfu instance.
                log.error("Fatal error connecting to Corfu server instance.", e);
                throw new UnrecoverableCorfuError(e);
            }
        }

        checkVersion();

        if (parameters.isUseFastLoader()) {
            FastObjectLoader fastLoader = new FastObjectLoader(this)
                    .setBatchReadSize(parameters.getBulkReadSize())
                    .setTimeoutInMinutesForLoading((int) parameters.fastLoaderTimeout.toMinutes());
            fastLoader.loadMaps();
        }

        garbageCollector.start();

        return this;
    }

    // Below are deprecated methods which should no longer be
    // used and may be deprecated in the future.

    // region Deprecated Constructors

    /**
     * Constructor for CorfuRuntime.
     *
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
     *
     * @deprecated Deprecated, set using {@link CorfuRuntimeParameters} instead.
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
     *
     * @deprecated Deprecated, set using {@link CorfuRuntimeParameters} instead.
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
     * @deprecated Deprecated, set using {@link CorfuRuntimeParameters} instead.
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
     * @deprecated Deprecated, set using {@link CorfuRuntimeParameters} instead.
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
     * @deprecated Deprecated, set using {@link CorfuRuntimeParameters} instead.
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
     * @deprecated Deprecated, set using {@link CorfuRuntimeParameters} instead.
     */
    @Deprecated
    public CorfuRuntime setHoleFillingDisabled(boolean disable) {
        log.warn("setHoleFillingDisabled: Deprecated, please set parameters instead");
        parameters.setHoleFillingDisabled(disable);
        return this;
    }

    /**
     * Set the cache expiration time.
     *
     * @param expiryTime The time before cache expiration, in seconds.
     * @deprecated Deprecated, set using {@link CorfuRuntimeParameters} instead.
     */
    @Deprecated
    public CorfuRuntime setCacheExpiryTime(int expiryTime) {
        log.warn("setCacheExpiryTime: Deprecated, please set parameters instead");
        parameters.setCacheExpiryTime(expiryTime);
        return this;
    }

    /**
     * Set the bulk read size.
     *
     * @param size The bulk read size.
     * @deprecated Deprecated, set using {@link CorfuRuntimeParameters} instead.
     */
    @Deprecated
    public CorfuRuntime setBulkReadSize(int size) {
        log.warn("setBulkReadSize: Deprecated, please set parameters instead");
        parameters.setBulkReadSize(size);
        return this;
    }


    /**
     * Set the write retry time.
     *
     * @param writeRetry The number of times to retry writes.
     * @deprecated Deprecated, set using {@link CorfuRuntimeParameters} instead.
     */
    @Deprecated
    public CorfuRuntime setWriteRetry(int writeRetry) {
        log.warn("setWriteRetry: Deprecated, please set parameters instead");
        parameters.setWriteRetry(writeRetry);
        return this;
    }

    /**
     * Set the trim retry time.
     *
     * @param trimRetry The number of times to retry on trims.
     * @deprecated Deprecated, set using {@link CorfuRuntimeParameters} instead.
     */
    @Deprecated
    public CorfuRuntime setTrimRetry(int trimRetry) {
        log.warn("setTrimRetry: Deprecated, please set parameters instead");
        parameters.setWriteRetry(trimRetry);
        return this;
    }

    /**
     * Set the timeout of the fast loader, in minutes.
     *
     * @param timeout The number of minutes to wait.
     * @deprecated Deprecated, set using {@link CorfuRuntimeParameters} instead.
     */
    @Deprecated
    public CorfuRuntime setTimeoutInMinutesForFastLoading(int timeout) {
        log.warn("setTrimRetry: Deprecated, please set parameters instead");
        parameters.setFastLoaderTimeout(Duration.ofMinutes(timeout));
        return this;
    }
    // endregion
}
