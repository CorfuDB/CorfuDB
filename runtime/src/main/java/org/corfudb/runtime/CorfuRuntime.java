package org.corfudb.runtime;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.comm.ChannelImplementation;
import org.corfudb.common.compression.Codec;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider.MeterRegistryInitializer;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.util.FileWatcher;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LayoutHandler;
import org.corfudb.runtime.clients.LogUnitHandler;
import org.corfudb.runtime.clients.ManagementHandler;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.clients.SequencerHandler;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.runtime.view.AddressSpaceView;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutManagementView;
import org.corfudb.runtime.view.LayoutView;
import org.corfudb.runtime.view.ManagementView;
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.SequencerView;
import org.corfudb.runtime.view.StreamsView;
import org.corfudb.runtime.view.TableRegistry;
import org.corfudb.util.CFUtils;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.Sleep;
import org.corfudb.util.serializer.Serializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import javax.annotation.Nonnull;
import java.nio.file.Path;
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

import static org.corfudb.common.util.URLUtils.getVersionFormattedEndpointURL;

/**
 * Created by mwei on 12/9/15.
 */
@Slf4j
@Accessors(chain = true)
public class CorfuRuntime {

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
    private volatile NodeRouterPool nodeRouterPool;

    /**
     * File watcher for SSL key store to support auto hot-swapping.
     */
    @Getter
    private volatile Optional<FileWatcher> sslCertWatcher = Optional.empty();

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

    public static final Marker LOG_NOT_IMPORTANT = MarkerFactory.getMarker("NOT_IMPORTANT");

    public static final int MAX_UNCOMPRESSED_WRITE_SIZE = 100 << 20;


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

    /**
     * A set of serializers used to serialize/deserialize data
     */
    @Getter
    private final Serializers serializers = new Serializers();



    /**
     * A class which holds parameters and settings for the {@link CorfuRuntime}.
     */
    @Data
    @ToString
    public static class CorfuRuntimeParameters extends RuntimeParameters {

        /*
         * Max size for a write request.
         */
        int maxWriteSize = Integer.MAX_VALUE;

        /*
         * Set the bulk read size.
         */
        int bulkReadSize = 10;

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

        Duration mvoCacheExpiry = Duration.ofMinutes(10);

        /*
        * cache metrics are to be enabled only for the tuning exercise.
        */
        boolean cacheEntryMetricsDisabled = true;

        /*
         * Whether to disable the cache (both AddressSpaceView readCache and MVO Cache).
         */
        boolean cacheDisabled = false;

        /*
         * The maximum number of entries in the AddressSpaceView cache.
         * This will be overridden to 0 if cacheDisabled is true.
         */
        long maxCacheEntries = 500;

        /*
         * The maximum number of entries in the MVOCache.
         * This will be overridden to 0 if cacheDisabled is true.
         */
        long maxMvoCacheEntries = 2500;

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
         * Sets expireAfterAccess and expireAfterWrite for the AddressSpaceView cache in seconds.
         */
        long cacheExpiryTime = Long.MAX_VALUE;

        // endregion

        // region Stream Parameters
        /*

         */
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
         * The maximum number of SMR entries that will be grouped in a CheckpointEntry.CONTINUATION
         */
        int checkpointBatchSize = 50;

        /*
         * The maximum size of an uncompressed CheckpointEntry.CONTINUATION that can be written
         */
        long maxUncompressedCpEntrySize = 100_000_000;

        /*
         * The maximum number of SMR entries that will be grouped in a MultiSMREntry during Restore
         */
        int restoreBatchSize = 50;

        /*
         * Stream Batch Size: number of addresses to fetch in advance when stream address discovery mechanism
         * relies on address maps instead of follow backpointers, i.e., followBackpointersEnabled = false;
         */
        int streamBatchSize = 10;

        /*
         * Checkpoint read Batch Size: number of checkpoint addresses to fetch in batch when stream
         * address discovery mechanism relies on address maps instead of follow backpointers;
         */
        int checkpointReadBatchSize = 1;

        /*
         * Cache Option for local writes.
         *
         * If set to 'false', writes won't be cached on the write path.
         * Note that even if cacheWrites is set, the addressSpaceView
         *
         */
        boolean cacheWrites = true;

        // endregion

        /**
         * Default client name in case the client chooses to remain anonymous.
         */
        String clientName = "";

        /**
         * How often should the local client checkpointer run? 0 disables it completely.
         */
        long checkpointTriggerFreqMillis = 0;

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

        /*
         * Enable runtime metrics.
         */
        private boolean metricsEnabled = true;

        /*
         * Number of entries read in a single batch to compute highest sequence number (based on data entries and not holes)
         */
        int highestSequenceNumberBatchSize = 4;

        /*
         * Number of worker threads that will read streaming data
         */
        private int streamingWorkersThreadPoolSize = 2;

        /*
         * Streaming scheduler poll period
         */
        private Duration streamingPollPeriod = Duration.ofMillis(50);

        /*
         * Number of stream tags (batch size) that the scheduler will query on every poll
         */
        private int streamingSchedulerPollBatchSize = 25;

        /*
         * Available space that a stream should have before it can be polled. This parameter is used to
         * avoid overwhelming the consumer.
         */
        private int streamingSchedulerPollThreshold = 5;

        public static CorfuRuntimeParametersBuilder builder() {
            return new CorfuRuntimeParametersBuilder();
        }

        public static class CorfuRuntimeParametersBuilder extends RuntimeParametersBuilder {
            private int maxWriteSize = Integer.MAX_VALUE;
            private int bulkReadSize = 10;
            private int holeFillRetry = 10;
            private Duration holeFillRetryThreshold = Duration.ofSeconds(1L);
            private Duration holeFillTimeout = Duration.ofSeconds(10);
            private Duration mvoCacheExpiry = Duration.ofMinutes(10);
            private boolean cacheEntryMetricsDisabled = true;
            private boolean cacheDisabled = false;
            private long maxCacheEntries = 2500;
            private long maxMvoCacheEntries = 2500;
            private long maxCacheWeight;
            private int cacheConcurrencyLevel = 0;
            private long cacheExpiryTime = Long.MAX_VALUE;
            private boolean holeFillingDisabled = false;
            private int writeRetry = 5;
            private int trimRetry = 2;
            private int checkpointRetries = 5;
            private int checkpointBatchSize = 50;
            private long maxUncompressedCpEntrySize = 100_000_000;
            private int restoreBatchSize = 50;
            private int streamBatchSize = 10;
            private int checkpointReadBatchSize = 1;
            private Duration runtimeGCPeriod = Duration.ofMinutes(20);
            private UUID clusterId = null;
            private int systemDownHandlerTriggerLimit = 20;
            private List<NodeLocator> layoutServers = new ArrayList<>();
            private int invalidateRetry = 5;
            private PriorityLevel priorityLevel = PriorityLevel.NORMAL;
            private Codec.Type codecType = Codec.Type.ZSTD;
            private boolean metricsEnabled = true;
            private int streamingWorkersThreadPoolSize = 2;
            private Duration streamingPollPeriod = Duration.ofMillis(50);
            private int streamingSchedulerPollBatchSize = 25;
            private int streamingSchedulerPollThreshold = 5;
            private boolean cacheWrites = true;
            private String clientName = "CorfuClient";
            private long checkpointTriggerFreqMillis = 0;

            public CorfuRuntimeParametersBuilder streamingWorkersThreadPoolSize(int streamingWorkersThreadPoolSize) {
                this.streamingWorkersThreadPoolSize = streamingWorkersThreadPoolSize;
                return this;
            }

            public CorfuRuntimeParametersBuilder streamingPollPeriod(Duration streamingPollPeriod) {
                this.streamingPollPeriod = streamingPollPeriod;
                return this;
            }

            public CorfuRuntimeParametersBuilder streamingSchedulerPollBatchSize(int streamingSchedulerPollBatchSize) {
                this.streamingSchedulerPollBatchSize = streamingSchedulerPollBatchSize;
                return this;
            }

            public CorfuRuntimeParametersBuilder streamingSchedulerPollThreshold(int streamingSchedulerPollThreshold) {
                this.streamingSchedulerPollThreshold = streamingSchedulerPollThreshold;
                return this;
            }

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

            @Override
            public CorfuRuntimeParametersBuilder disableCertExpiryCheckFile(Path disableCertExpiryCheckFile) {
                this.disableCertExpiryCheckFile = disableCertExpiryCheckFile;
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

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder holeFillRetry(int holeFillRetry) {
                this.holeFillRetry = holeFillRetry;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder holeFillRetryThreshold(Duration holeFillRetryThreshold) {
                this.holeFillRetryThreshold = holeFillRetryThreshold;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder mvoCacheExpiry(Duration mvoCacheExpiry) {
                this.mvoCacheExpiry = mvoCacheExpiry;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder holeFillTimeout(Duration holeFillTimeout) {
                this.holeFillTimeout = holeFillTimeout;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder cacheEntryMetricsDisabled(boolean cacheEntryMetricsDisabled) {
                this.cacheEntryMetricsDisabled = cacheEntryMetricsDisabled;
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

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder maxMvoCacheEntries(long maxCacheEntries) {
                this.maxMvoCacheEntries = maxCacheEntries;
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

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder checkpointBatchSize(int checkpointBatchSize) {
                this.checkpointBatchSize = checkpointBatchSize;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder maxUncompressedCpEntrySize(long maxUncompressedCpEntrySize) {
                this.maxUncompressedCpEntrySize = maxUncompressedCpEntrySize;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder restoreBatchSize(int restoreBatchSize) {
                this.restoreBatchSize = restoreBatchSize;
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

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder cacheWrites(boolean cacheWrites) {
                this.cacheWrites = cacheWrites;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder clientName(String clientName) {
                this.clientName = clientName;
                return this;
            }

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder checkpointTriggerFreqMillis(
                    long checkpointTriggerFreqMillis) {
                this.checkpointTriggerFreqMillis = checkpointTriggerFreqMillis;
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

            public CorfuRuntimeParameters.CorfuRuntimeParametersBuilder metricsEnabled(boolean enabled) {
                this.metricsEnabled = enabled;
                return this;
            }

            public CorfuRuntimeParameters build() {
                CorfuRuntimeParameters corfuRuntimeParameters = new CorfuRuntimeParameters();
                corfuRuntimeParameters.setTlsEnabled(tlsEnabled);
                corfuRuntimeParameters.setKeyStore(keyStore);
                corfuRuntimeParameters.setKsPasswordFile(ksPasswordFile);
                corfuRuntimeParameters.setTrustStore(trustStore);
                corfuRuntimeParameters.setTsPasswordFile(tsPasswordFile);
                corfuRuntimeParameters.setDisableCertExpiryCheckFile(disableCertExpiryCheckFile);
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
                corfuRuntimeParameters.setSystemDownHandler(systemDownHandler);
                corfuRuntimeParameters.setBeforeRpcHandler(beforeRpcHandler);
                corfuRuntimeParameters.setMaxWriteSize(maxWriteSize);
                corfuRuntimeParameters.setBulkReadSize(bulkReadSize);
                corfuRuntimeParameters.setHoleFillRetry(holeFillRetry);
                corfuRuntimeParameters.setHoleFillRetryThreshold(holeFillRetryThreshold);
                corfuRuntimeParameters.setHoleFillTimeout(holeFillTimeout);
                corfuRuntimeParameters.setMvoCacheExpiry(mvoCacheExpiry);
                corfuRuntimeParameters.setCacheEntryMetricsDisabled(cacheEntryMetricsDisabled);
                corfuRuntimeParameters.setCacheDisabled(cacheDisabled);
                corfuRuntimeParameters.setMaxCacheEntries(maxCacheEntries);
                corfuRuntimeParameters.setMaxMvoCacheEntries(maxMvoCacheEntries);
                corfuRuntimeParameters.setMaxCacheWeight(maxCacheWeight);
                corfuRuntimeParameters.setCacheConcurrencyLevel(cacheConcurrencyLevel);
                corfuRuntimeParameters.setCacheExpiryTime(cacheExpiryTime);
                corfuRuntimeParameters.setHoleFillingDisabled(holeFillingDisabled);
                corfuRuntimeParameters.setWriteRetry(writeRetry);
                corfuRuntimeParameters.setTrimRetry(trimRetry);
                corfuRuntimeParameters.setCheckpointRetries(checkpointRetries);
                corfuRuntimeParameters.setCheckpointBatchSize(checkpointBatchSize);
                corfuRuntimeParameters.setMaxUncompressedCpEntrySize(maxUncompressedCpEntrySize);
                corfuRuntimeParameters.setRestoreBatchSize(restoreBatchSize);
                corfuRuntimeParameters.setStreamBatchSize(streamBatchSize);
                corfuRuntimeParameters.setCheckpointReadBatchSize(checkpointReadBatchSize);
                corfuRuntimeParameters.setRuntimeGCPeriod(runtimeGCPeriod);
                corfuRuntimeParameters.setClusterId(clusterId);
                corfuRuntimeParameters.setSystemDownHandlerTriggerLimit(systemDownHandlerTriggerLimit);
                corfuRuntimeParameters.setLayoutServers(layoutServers);
                corfuRuntimeParameters.setInvalidateRetry(invalidateRetry);
                corfuRuntimeParameters.setPriorityLevel(priorityLevel);
                corfuRuntimeParameters.setCodecType(codecType);
                corfuRuntimeParameters.setMetricsEnabled(metricsEnabled);
                corfuRuntimeParameters.setStreamingWorkersThreadPoolSize(streamingWorkersThreadPoolSize);
                corfuRuntimeParameters.setStreamingPollPeriod(streamingPollPeriod);
                corfuRuntimeParameters.setStreamingSchedulerPollBatchSize(streamingSchedulerPollBatchSize);
                corfuRuntimeParameters.setStreamingSchedulerPollThreshold(streamingSchedulerPollThreshold);
                corfuRuntimeParameters.setCacheWrites(cacheWrites);
                corfuRuntimeParameters.setClientName(clientName);
                corfuRuntimeParameters.setCheckpointTriggerFreqMillis(checkpointTriggerFreqMillis);
                return corfuRuntimeParameters;
            }
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

        if (parameters.metricsEnabled) {
            Logger logger = LoggerFactory.getLogger("org.corfudb.client.metricsdata");
            if (logger.isDebugEnabled()) {
                MeterRegistryInitializer.initClientMetrics(logger,
                        Duration.ofMinutes(1),
                        parameters.clientId.toString());
            } else {
                log.warn("No registered metrics logger provided.");
            }

        } else {
            log.warn("Runtime metrics are disabled.");
        }

        log.info("Corfu runtime version {} initialized.",
                Long.toHexString(GitRepositoryState.getCorfuSourceCodeVersion()));
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
     * Get an optional of the FileWatcher on Keystore file
     *
     * @return The Optional of FileWatcher on Keystore file. Empty if keystore is not set in runtime.
     */
    private Optional<FileWatcher> initializeSslCertWatcher() {
        String keyStorePath = this.parameters.getKeyStore();
        if (keyStorePath == null || keyStorePath.isEmpty()) {
            return Optional.empty();
        }
        FileWatcher sslWatcher = new FileWatcher(keyStorePath, this::reconnect);
        return Optional.of(sslWatcher);
    }

    /**
     * Shuts down the CorfuRuntime.
     * Stops async tasks from fetching the layout.
     * Cannot reuse the runtime once shutdown is called.
     */
    public void shutdown() {
        // Stopping async task from fetching layout.
        isShutdown = true;

        // Shutdown the mvoCache sync thread
        getObjectsView().getMvoCache().shutdown();

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

        // If it's the client who initialized the registry - shut it down
        MeterRegistryProvider.getMetricType().ifPresent(type -> {
            if (type == MeterRegistryProvider.MetricType.CLIENT) {
                MeterRegistryProvider.close();
            }
        });
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
        if (shutdown) {
            sslCertWatcher.ifPresent(FileWatcher::close);
            nodeRouterPool.shutdown();
        } else {
            log.info("stop: Re-Initializing nodeRouterPool.");
            nodeRouterPool = new NodeRouterPool(getRouterFunction);
        }
    }

    /**
     * Reestablish the netty connections to corfu servers
     */
    private void reconnect() {
        nodeRouterPool.reconnect();
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
     * Parse a configuration string and get a CorfuRuntime.
     * Both Pure IPv6 and Pure IPv4 addresses are supported.
     *
     *
     * @param configurationString The configuration string to parse.
     * @return A CorfuRuntime Configured based on the configuration string.
     */
    public CorfuRuntime parseConfigurationString(String configurationString) {
        // Parse comma sep. list.
        bootstrapLayoutServers = Pattern.compile(",")
                .splitAsStream(configurationString)
                .map(address -> getVersionFormattedEndpointURL(address.trim()))
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
    @VisibleForTesting
    public void checkClusterId(@Nonnull Layout layout) {
        // We haven't adopted a clusterId yet.
        if (clusterId == null) {
            clusterId = layout.getClusterId();
            if (clusterId != null) {
                log.info("Connected to new cluster {}", clusterId);
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
            Optional<Timer.Sample> fetchSample = MicroMeterUtils.startTimer();
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
                            log.warn("fetchLayout: latest layout epoch {} > received {}" +
                                            " from {}:{}, discarded.", latestLayout.getEpoch(), l.getEpoch(),
                                    router.getHost(), router.getPort());
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
                        MicroMeterUtils.time(fetchSample, "runtime.fetch_layout.timer");
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

    /**
     * Connect to the Corfu server instance.
     * When this function returns, the Corfu server is ready to be accessed.
     */
    public synchronized CorfuRuntime connect() {

        log.info("connect: runtime parameters {}", getParameters());

        // Start file watcher on Ssl certs
        if (!sslCertWatcher.isPresent()) {
            log.info("connect: Initializing sslCertWatcher.");
            sslCertWatcher = initializeSslCertWatcher();
        }

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

        // Build nodeRouterPool and connect with each endpoint
        Layout currentLayout = CFUtils.getUninterruptibly(layout);
        List<CompletableFuture<Boolean>> pingEndpoints =
                currentLayout.getLayoutServers()
                        .stream().map(s -> getLayoutView().getRuntimeLayout().getBaseClient(s))
                        .map(BaseClient::ping)
                        .collect(Collectors.toList());
        for (CompletableFuture<Boolean> ping : pingEndpoints) {
            try {
                CFUtils.getUninterruptibly(ping, Exception.class);
            } catch (Exception ex) {
                log.error("connect: Couldn't connect to server.", ex);
            }
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

    // endregion
}
