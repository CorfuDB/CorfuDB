package org.corfudb.runtime;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.comm.ChannelImplementation;
import org.corfudb.common.compression.Codec;
import org.corfudb.protocols.wireprotocol.MsgHandlingFilter;
import org.corfudb.protocols.wireprotocol.PriorityLevel;
import org.corfudb.protocols.wireprotocol.VersionInfo;
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
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.SequencerView;
import org.corfudb.runtime.view.StreamsView;
import org.corfudb.runtime.view.ManagementView;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.runtime.view.LayoutView;
import org.corfudb.runtime.view.LayoutManagementView;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.Sleep;
import org.corfudb.util.UuidUtils;
import org.corfudb.util.Version;

import javax.annotation.Nonnull;
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
import java.util.concurrent.atomic.AtomicReference;
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
    @Data
    @ToString
    public static class CorfuRuntimeParameters extends RuntimeParameters {

        public static CorfuRuntimeParametersBuilder builder() {
            return new CorfuRuntimeParametersBuilder();
        }

        /*
         * Max size for a write request.
         */
        int maxWriteSize = Integer.MAX_VALUE;

        /*
         * Set the bulk read size.
         */
        int bulkReadSize = 10;

        /*
         * How much time the Fast Loader has to get the maps up to date.
         *
         * <p>Once the timeout is reached, the Fast Loader gives up. Every map that is
         * not up to date will be loaded through normal path.
         */
        Duration fastLoaderTimeout = Duration.ofMinutes(30);
        // endregion

        // region Address Space Parameters
        /*
         * Number of times to attempt to read before hole filling.
         * @deprecated This is a no-op. Use holeFillWait
         */
        @Deprecated
        int holeFillRetry = 10;

        /* Time to wait between read requests reattempts before hole filling. */
        Duration holeFillRetryThreshold = Duration.ofSeconds(1L);

        /*
         * Time limit after which the reader gives up and fills the hole.
         */
        Duration holeFillTimeout = Duration.ofSeconds(10);

        /*
         * Whether or not to disable the cache.
         */
        boolean cacheDisabled = false;

        /*
         * The maximum number of entries in the cache.
         */
        long maxCacheEntries;

        /*
         * The max in-memory size of the cache in bytes
         */
        long maxCacheWeight;

        /*
         * This is a hint to size the AddressSpaceView cache, a higher concurrency
         * level allows for less lock contention at the cost of more memory overhead.
         * The default value of zero will result in using the cache's internal default
         * concurrency level (i.e. 4).
         */
        int cacheConcurrencyLevel = 0;

        /*
         * Sets expireAfterAccess and expireAfterWrite in seconds.
         */
        long cacheExpiryTime = Long.MAX_VALUE;
        // endregion

        // region Stream Parameters
        /*
         * True, if strategy to discover the address space of a stream relies on the follow backpointers.
         * False, if strategy to discover the address space of a stream relies on the get stream address map.
         */
        boolean followBackpointersEnabled = false;

        /*
         * Whether or not hole filling should be disabled.
         */
        boolean holeFillingDisabled = false;

        /*
         * Number of times to retry on an
         * {@link org.corfudb.runtime.exceptions.OverwriteException} before giving up.
         */
        int writeRetry = 5;

        /*
         * The number of times to retry on a retriable
         * {@link org.corfudb.runtime.exceptions.TrimmedException} during a transaction.
         */
        int trimRetry = 2;

        /*
         * The total number of retries the checkpointer will attempt on sequencer failover to
         * prevent epoch regressions. This is independent of the number of streams to be checkpointed.
         */
        int checkpointRetries = 5;

        /*
         * Stream Batch Size: number of addresses to fetch in advance when stream address discovery mechanism
         * relies on address maps instead of follow backpointers, i.e., followBackpointersEnabled = false;
         */
        int streamBatchSize = 10;

        /*
         * Checkpoint read Batch Size: number of checkpoint addresses to fetch in batch when stream
         * address discovery mechanism relies on address maps instead of follow backpointers;
         */
        int checkpointReadBatchSize = 5;
        // endregion

        /*
         * The period at which the runtime will run garbage collection
         */
        Duration runtimeGCPeriod = Duration.ofMinutes(20);

        /*
         * The {@link UUID} for the cluster this client is connecting to, or
         * {@code null} if the client should adopt the {@link UUID} of the first
         * server it connects to.
         */
        UUID clusterId = null;

        /*
         * Number of retries to reconnect to an unresponsive system before invoking the
         * systemDownHandler. This is mainly required to allow the fault detection mechanism
         * to detect and reconfigure the cluster.
         * The fault detection takes at least 3 seconds to recognize a failure.
         * Each retry is attempted after a sleep of {@literal connectionRetryRate}
         * invoking the systemDownHandler after a minimum of
         * (systemDownHandlerTriggerLimit * connectionRetryRate) seconds. Default: 20 seconds.
         */
        int systemDownHandlerTriggerLimit = 20;

        /*
         * The initial list of layout servers.
         */
        List<NodeLocator> layoutServers = new ArrayList<>();
        //endregion

        /*
         * The number of times to retry invalidate when a layout change is expected.
         */
        int invalidateRetry = 5;


        /*
         * The default priority of the requests made by this client.
         * Under resource constraints non-high priority requests
         * are dropped.
         */
        private PriorityLevel priorityLevel = PriorityLevel.NORMAL;

        /*
         * The compression codec to use to encode a write's payload
         */
        private Codec.Type codecType = Codec.Type.ZSTD;

        public static class CorfuRuntimeParametersBuilder extends RuntimeParametersBuilder {
            int maxWriteSize = Integer.MAX_VALUE;
            int bulkReadSize = 10;
            Duration fastLoaderTimeout = Duration.ofMinutes(30);
            int holeFillRetry = 10;
            Duration holeFillRetryThreshold = Duration.ofSeconds(1L);
            Duration holeFillTimeout = Duration.ofSeconds(10);
            boolean cacheDisabled = false;
            long maxCacheEntries;
            long maxCacheWeight;
            int cacheConcurrencyLevel = 0;
            long cacheExpiryTime = Long.MAX_VALUE;
            boolean followBackpointersEnabled = false;
            boolean holeFillingDisabled = false;
            int writeRetry = 5;
            int trimRetry = 2;
            int checkpointRetries = 5;
            int streamBatchSize = 10;
            int checkpointReadBatchSize = 5;
            Duration runtimeGCPeriod = Duration.ofMinutes(20);
            UUID clusterId = null;
            int systemDownHandlerTriggerLimit = 20;
            List<NodeLocator> layoutServers = new ArrayList<>();
            int invalidateRetry = 5;
            private PriorityLevel priorityLevel = PriorityLevel.NORMAL;
            private Codec.Type codecType = Codec.Type.ZSTD;

            public CorfuRuntimeParametersBuilder tlsEnabled(boolean tlsEnabled) {
                super.tlsEnabled(tlsEnabled);
                return this;
            }

            public CorfuRuntimeParametersBuilder keyStore(String keyStore) {
                super.keyStore(keyStore);
                return this;
            }

            public CorfuRuntimeParametersBuilder ksPasswordFile(String ksPasswordFile) {
                super.ksPasswordFile(ksPasswordFile);
                return this;
            }

            public CorfuRuntimeParametersBuilder trustStore(String trustStore) {
                super.trustStore(trustStore);
                return this;
            }

            public CorfuRuntimeParametersBuilder tsPasswordFile(String tsPasswordFile) {
                super.tsPasswordFile(tsPasswordFile);
                return this;
            }

            public CorfuRuntimeParametersBuilder saslPlainTextEnabled(boolean saslPlainTextEnabled) {
                super.saslPlainTextEnabled(saslPlainTextEnabled);
                return this;
            }

            public CorfuRuntimeParametersBuilder usernameFile(String usernameFile) {
                super.usernameFile(usernameFile);
                return this;
            }

            public CorfuRuntimeParametersBuilder passwordFile(String passwordFile) {
                super.passwordFile(passwordFile);
                return this;
            }

            public CorfuRuntimeParametersBuilder handshakeTimeout(int handshakeTimeout) {
                super.handshakeTimeout(handshakeTimeout);
                return this;
            }

            public CorfuRuntimeParametersBuilder requestTimeout(Duration requestTimeout) {
                super.requestTimeout(requestTimeout);
                return this;
            }

            public CorfuRuntimeParametersBuilder idleConnectionTimeout(int idleConnectionTimeout) {
                super.idleConnectionTimeout(idleConnectionTimeout);
                return this;
            }

            public CorfuRuntimeParametersBuilder keepAlivePeriod(int keepAlivePeriod) {
                super.keepAlivePeriod(keepAlivePeriod);
                return this;
            }

            public CorfuRuntimeParametersBuilder connectionTimeout(Duration connectionTimeout) {
                super.connectionTimeout(connectionTimeout);
                return this;
            }

            public CorfuRuntimeParametersBuilder connectionRetryRate(Duration connectionRetryRate) {
                super.connectionRetryRate(connectionRetryRate);
                return this;
            }

            public CorfuRuntimeParametersBuilder clientId(UUID clientId) {
                super.clientId(clientId);
                return this;
            }

            public CorfuRuntimeParametersBuilder socketType(ChannelImplementation socketType) {
                super.socketType(socketType);
                return this;
            }

            public CorfuRuntimeParametersBuilder nettyEventLoop(EventLoopGroup nettyEventLoop) {
                super.nettyEventLoop(nettyEventLoop);
                return this;
            }

            public CorfuRuntimeParametersBuilder nettyEventLoopThreadFormat(String nettyEventLoopThreadFormat) {
                super.nettyEventLoopThreadFormat(nettyEventLoopThreadFormat);
                return this;
            }

            public CorfuRuntimeParametersBuilder nettyEventLoopThreads(int nettyEventLoopThreads) {
                super.nettyEventLoopThreads(nettyEventLoopThreads);
                return this;
            }

            public CorfuRuntimeParametersBuilder shutdownNettyEventLoop(boolean shutdownNettyEventLoop) {
                super.shutdownNettyEventLoop(shutdownNettyEventLoop);
                return this;
            }

            public CorfuRuntimeParametersBuilder customNettyChannelOptions(Map<ChannelOption, Object> customNettyChannelOptions) {
                super.customNettyChannelOptions(customNettyChannelOptions);
                return this;
            }

            public CorfuRuntimeParametersBuilder uncaughtExceptionHandler(Thread.UncaughtExceptionHandler uncaughtExceptionHandler) {
                super.uncaughtExceptionHandler(uncaughtExceptionHandler);
                return this;
            }

            public CorfuRuntimeParametersBuilder nettyClientInboundMsgFilters(List<MsgHandlingFilter> nettyClientInboundMsgFilters) {
                super.nettyClientInboundMsgFilters(nettyClientInboundMsgFilters);
                return this;
            }

            public CorfuRuntimeParametersBuilder prometheusMetricsPort(int prometheusMetricsPort) {
                super.prometheusMetricsPort(prometheusMetricsPort);
                return this;
            }

            public CorfuRuntimeParametersBuilder systemDownHandler(Runnable systemDownHandler) {
                super.systemDownHandler(systemDownHandler);
                return this;
            }

            public CorfuRuntimeParametersBuilder beforeRpcHandler(Runnable beforeRpcHandler) {
                super.beforeRpcHandler(beforeRpcHandler);
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder maxWriteSize(int maxWriteSize) {
                this.maxWriteSize = maxWriteSize;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder bulkReadSize(int bulkReadSize) {
                this.bulkReadSize = bulkReadSize;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder fastLoaderTimeout(Duration fastLoaderTimeout) {
                this.fastLoaderTimeout = fastLoaderTimeout;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder holeFillRetry(int holeFillRetry) {
                this.holeFillRetry = holeFillRetry;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder holeFillRetryThreshold(Duration holeFillRetryThreshold) {
                this.holeFillRetryThreshold = holeFillRetryThreshold;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder holeFillTimeout(Duration holeFillTimeout) {
                this.holeFillTimeout = holeFillTimeout;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder cacheDisabled(boolean cacheDisabled) {
                this.cacheDisabled = cacheDisabled;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder maxCacheEntries(long maxCacheEntries) {
                this.maxCacheEntries = maxCacheEntries;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder maxCacheWeight(long maxCacheWeight) {
                this.maxCacheWeight = maxCacheWeight;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder cacheConcurrencyLevel(int cacheConcurrencyLevel) {
                this.cacheConcurrencyLevel = cacheConcurrencyLevel;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder cacheExpiryTime(long cacheExpiryTime) {
                this.cacheExpiryTime = cacheExpiryTime;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder followBackpointersEnabled(boolean followBackpointersEnabled) {
                this.followBackpointersEnabled = followBackpointersEnabled;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder holeFillingDisabled(boolean holeFillingDisabled) {
                this.holeFillingDisabled = holeFillingDisabled;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder writeRetry(int writeRetry) {
                this.writeRetry = writeRetry;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder trimRetry(int trimRetry) {
                this.trimRetry = trimRetry;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder checkpointRetries(int checkpointRetries) {
                this.checkpointRetries = checkpointRetries;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder streamBatchSize(int streamBatchSize) {
                this.streamBatchSize = streamBatchSize;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder checkpointReadBatchSize(int checkpointReadBatchSize) {
                this.checkpointReadBatchSize = checkpointReadBatchSize;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder runtimeGCPeriod(Duration runtimeGCPeriod) {
                this.runtimeGCPeriod = runtimeGCPeriod;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder clusterId(UUID clusterId) {
                this.clusterId = clusterId;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder systemDownHandlerTriggerLimit(int systemDownHandlerTriggerLimit) {
                this.systemDownHandlerTriggerLimit = systemDownHandlerTriggerLimit;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder layoutServers(List<NodeLocator> layoutServers) {
                this.layoutServers = layoutServers;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder invalidateRetry(int invalidateRetry) {
                this.invalidateRetry = invalidateRetry;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder priorityLevel(PriorityLevel priorityLevel) {
                this.priorityLevel = priorityLevel;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder codecType(Codec.Type codecType) {
                this.codecType = codecType;
                return this;
            }

            public CorfuRuntimeParameters build() {
                CorfuRuntimeParameters corfuRuntimeParameters = new CorfuRuntimeParameters();
                corfuRuntimeParameters.setTlsEnabled(tlsEnabled);
                corfuRuntimeParameters.setKeyStore(keyStore);
                corfuRuntimeParameters.setKsPasswordFile(ksPasswordFile);
                corfuRuntimeParameters.setTrustStore(trustStore);
                corfuRuntimeParameters.setTsPasswordFile(tsPasswordFile);
                corfuRuntimeParameters.setSaslPlainTextEnabled(saslPlainTextEnabled);
                corfuRuntimeParameters.setUsernameFile(usernameFile);
                corfuRuntimeParameters.setPasswordFile(passwordFile);
                corfuRuntimeParameters.setHandshakeTimeout(handshakeTimeout);
                corfuRuntimeParameters.setRequestTimeout(requestTimeout);
                corfuRuntimeParameters.setIdleConnectionTimeout(idleConnectionTimeout);
                corfuRuntimeParameters.setKeepAlivePeriod(keepAlivePeriod);
                corfuRuntimeParameters.setConnectionTimeout(connectionTimeout);
                corfuRuntimeParameters.setConnectionRetryRate(connectionRetryRate);
                corfuRuntimeParameters.setClientId(clientId);
                corfuRuntimeParameters.setSocketType(socketType);
                corfuRuntimeParameters.setNettyEventLoop(nettyEventLoop);
                corfuRuntimeParameters.setNettyEventLoopThreadFormat(nettyEventLoopThreadFormat);
                corfuRuntimeParameters.setNettyEventLoopThreads(nettyEventLoopThreads);
                corfuRuntimeParameters.setShutdownNettyEventLoop(shutdownNettyEventLoop);
                corfuRuntimeParameters.setCustomNettyChannelOptions(customNettyChannelOptions);
                corfuRuntimeParameters.setUncaughtExceptionHandler(uncaughtExceptionHandler);
                corfuRuntimeParameters.setNettyClientInboundMsgFilters(nettyClientInboundMsgFilters);
                corfuRuntimeParameters.setPrometheusMetricsPort(prometheusMetricsPort);
                corfuRuntimeParameters.setSystemDownHandler(systemDownHandler);
                corfuRuntimeParameters.setBeforeRpcHandler(beforeRpcHandler);
                corfuRuntimeParameters.setMaxWriteSize(maxWriteSize);
                corfuRuntimeParameters.setBulkReadSize(bulkReadSize);
                corfuRuntimeParameters.setFastLoaderTimeout(fastLoaderTimeout);
                corfuRuntimeParameters.setHoleFillRetry(holeFillRetry);
                corfuRuntimeParameters.setHoleFillRetryThreshold(holeFillRetryThreshold);
                corfuRuntimeParameters.setHoleFillTimeout(holeFillTimeout);
                corfuRuntimeParameters.setCacheDisabled(cacheDisabled);
                corfuRuntimeParameters.setMaxCacheEntries(maxCacheEntries);
                corfuRuntimeParameters.setMaxCacheWeight(maxCacheWeight);
                corfuRuntimeParameters.setCacheConcurrencyLevel(cacheConcurrencyLevel);
                corfuRuntimeParameters.setCacheExpiryTime(cacheExpiryTime);
                corfuRuntimeParameters.setFollowBackpointersEnabled(followBackpointersEnabled);
                corfuRuntimeParameters.setHoleFillingDisabled(holeFillingDisabled);
                corfuRuntimeParameters.setWriteRetry(writeRetry);
                corfuRuntimeParameters.setTrimRetry(trimRetry);
                corfuRuntimeParameters.setCheckpointRetries(checkpointRetries);
                corfuRuntimeParameters.setStreamBatchSize(streamBatchSize);
                corfuRuntimeParameters.setCheckpointReadBatchSize(checkpointReadBatchSize);
                corfuRuntimeParameters.setRuntimeGCPeriod(runtimeGCPeriod);
                corfuRuntimeParameters.setClusterId(clusterId);
                corfuRuntimeParameters.setSystemDownHandlerTriggerLimit(systemDownHandlerTriggerLimit);
                corfuRuntimeParameters.setLayoutServers(layoutServers);
                corfuRuntimeParameters.setInvalidateRetry(invalidateRetry);
                corfuRuntimeParameters.setPriorityLevel(priorityLevel);
                corfuRuntimeParameters.setCodecType(codecType);
                return corfuRuntimeParameters;
            }
        }
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
     * CorfuStore's table registry cache for Table lifecycle management.
     */
    private final AtomicReference<TableRegistry> tableRegistry = new AtomicReference<>(null);

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
     * lazy instantiation of the tableRegistry
     */
    public TableRegistry getTableRegistry() {
        TableRegistry tableRegistryObj = this.tableRegistry.get();
        if (tableRegistryObj == null) {
            synchronized (this) {
                tableRegistryObj = this.tableRegistry.get();
                if (tableRegistryObj == null) {
                    tableRegistryObj = new TableRegistry(this);
                    this.tableRegistry.set(tableRegistryObj);
                }
            }
        }
        return tableRegistryObj;
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

        // Try to expose metrics via Dropwizard CsvReporter JmxReporter and Slf4jReporter.
        MetricsUtils.metricsReportingSetup(defaultMetrics);
        if (parameters.getPrometheusMetricsPort() != MetricsUtils.NO_METRICS_PORT) {
            // Try to expose metrics via Prometheus.
            MetricsUtils.metricsReportingSetup(
                    defaultMetrics, parameters.getPrometheusMetricsPort());
        }

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
        TableRegistry tableRegistryObj = tableRegistry.get();
        if (tableRegistryObj != null) {
            tableRegistryObj.shutdown();
        }
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
        log.info("Bootstrap Layout Servers {}", bootstrapLayoutServers);
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
                                new LayoutClient(router, Layout.INVALID_EPOCH, Layout.INVALID_CLUSTER_ID).getLayout();
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
                    } catch (WrongClusterException we) {
                        // It is futile trying to re-connect to the wrong cluster
                        log.warn("Giving up since cluster is incorrect or reconfigured!");
                        log.info("fetchLayout: Invoking the systemDownHandler.");
                        parameters.getSystemDownHandler().run();
                        throw we;
                    } catch (ExecutionException ee) {
                        if (ee.getCause() instanceof TimeoutException) {
                            log.warn("Tried to get layout from {} but failed by timeout", s);
                        } else {
                            log.warn("Tried to get layout from {} but failed with exception:", s, ee);
                        }
                    } catch (Exception e) {
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
