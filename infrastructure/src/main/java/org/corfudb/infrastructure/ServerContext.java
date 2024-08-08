package org.corfudb.infrastructure;

import static org.corfudb.common.util.URLUtils.getVersionFormattedHostAddress;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoopGroup;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.comm.ChannelImplementation;
import org.corfudb.common.config.ConfigParamNames;
import org.corfudb.infrastructure.datastore.DataStore;
import org.corfudb.infrastructure.datastore.KvDataStore.KvRecord;
import org.corfudb.infrastructure.paxos.PaxosDataStore;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.runtime.view.ConservativeFailureHandlerPolicy;
import org.corfudb.runtime.view.IReconfigurationHandlerPolicy;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.UuidUtils;
import org.corfudb.utils.lock.Lock;

import javax.annotation.Nonnull;
import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.DEFAULT_MAX_NUM_MSG_PER_BATCH;
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.DEFAULT_MAX_SNAPSHOT_ENTRIES_APPLIED;
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.MAX_CACHE_NUM_ENTRIES;
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.DEFAULT_MAX_MSG_BATCH_SIZE;


/**
 * Server Context:
 * <ul>
 * <li>Contains the common node level {@link DataStore}</li>
 * <li>Responsible for Server level EPOCH </li>
 * <li>Should contain common services/utilities that the different Servers in a node require.</li>
 * </ul>
 *
 * <p>Note:
 * It is created in {@link CorfuServer} and then
 * passed to all the servers including {@link NettyServerRouter}.
 *
 * <p>Created by mdhawan on 8/5/16.
 */
@Slf4j
public class ServerContext implements AutoCloseable {

    // Layout Server
    private static final String PREFIX_EPOCH = "SERVER_EPOCH";
    private static final String KEY_EPOCH = "CURRENT";
    private static final String PREFIX_LAYOUT = "LAYOUT";
    private static final String KEY_LAYOUT = "CURRENT";
    private static final String PREFIX_LAYOUTS = "LAYOUTS";

    // Sequencer Server
    private static final String KEY_SEQUENCER = "SEQUENCER";
    private static final String PREFIX_SEQUENCER_EPOCH = "EPOCH";

    // Management Server
    private static final String PREFIX_MANAGEMENT = "MANAGEMENT";
    private static final String MANAGEMENT_LAYOUT = "LAYOUT";

    // LogUnit Server
    private static final String PREFIX_LOGUNIT = "LOGUNIT";
    private static final String EPOCH_WATER_MARK = "EPOCH_WATER_MARK";

    // Corfu Replication Server
    public static final String PLUGIN_CONFIG_FILE_PATH = "../resources/corfu_plugin_config.properties";

    /** The node Id, stored as a base64 string. */
    private static final String NODE_ID = "NODE_ID";


    private static final KvRecord<String> NODE_ID_RECORD = KvRecord.of(NODE_ID, String.class);

    private static final KvRecord<Layout> CURR_LAYOUT_RECORD = KvRecord.of(
            PREFIX_LAYOUT, KEY_LAYOUT, Layout.class
    );

    private static final KvRecord<Long> SERVER_EPOCH_RECORD= KvRecord.of(
            PREFIX_EPOCH, KEY_EPOCH, Long.class
    );

    private static final KvRecord<Long> SEQUENCER_RECORD = KvRecord.of(
            KEY_SEQUENCER, PREFIX_SEQUENCER_EPOCH, Long.class
    );

    public static final KvRecord<Layout> MANAGEMENT_LAYOUT_RECORD = KvRecord.of(
            PREFIX_MANAGEMENT, MANAGEMENT_LAYOUT, Layout.class
    );

    private static final KvRecord<Long> LOG_UNIT_WATERMARK_RECORD = KvRecord.of(
            PREFIX_LOGUNIT, EPOCH_WATER_MARK, Long.class
    );


    /**
     * various duration constants.
     */
    public static final Duration SHUTDOWN_TIMER = Duration.ofSeconds(5);

    @Getter
    private final Map<String, Object> serverConfig;

    @Getter
    private final DataStore dataStore;

    @Getter
    @Setter
    private IServerRouter serverRouter;

    @Getter
    @Setter
    private IReconfigurationHandlerPolicy failureHandlerPolicy;

    @Getter
    private final EventLoopGroup clientGroup;

    @Getter
    private volatile EventLoopGroup workerGroup;

    @Getter (AccessLevel.PACKAGE)
    @Setter
    private NodeLocator nodeLocator;

    @Getter
    @Setter
    private String localEndpoint;

    @Getter
    private final Set<String> dsFilePrefixesForCleanup =
            Sets.newHashSet(PaxosDataStore.PREFIX_PHASE_1, PaxosDataStore.PREFIX_PHASE_2, PREFIX_LAYOUTS);

    /**
     * Returns a new ServerContext.
     *
     * @param serverConfig map of configuration strings to objects
     */
    public ServerContext(Map<String, Object> serverConfig) {
        this.serverConfig = serverConfig;
        this.dataStore = new DataStore(serverConfig, this::dataStoreFileCleanup);
        generateNodeId();
        this.failureHandlerPolicy = new ConservativeFailureHandlerPolicy();

        // Setup the netty event loops. In tests, these loops may be provided by
        // a test framework to save resources.
        final boolean providedEventLoops =
                 getChannelImplementation().equals(ChannelImplementation.LOCAL);

        if (providedEventLoops) {
            clientGroup = getServerConfig(EventLoopGroup.class, "client");
            workerGroup = getServerConfig(EventLoopGroup.class, "worker");
        } else {
            clientGroup = getNewClientGroup();
            workerGroup = getNewWorkerGroup();
        }

        String hostAddress = getVersionFormattedHostAddress((String) serverConfig.get("--address"));
        String address = hostAddress + ":" + serverConfig.get("<port>");
        nodeLocator = NodeLocator.parseString(address);
        localEndpoint = nodeLocator.toEndpointUrl();

        //TODO(Chetan): Remove this
        log.debug("ServerContext: LocalEndpoint set as " + localEndpoint);
    }

    int getBaseServerThreadCount() {
        Optional<String> threadCount = getServerConfig("--base-server-threads");
        return threadCount.map(Integer::parseInt).orElse(1);
    }

    public int getLogUnitThreadCount() {
        Optional<String> threadCount = getServerConfig("--logunit-threads");
        return threadCount.map(Integer::parseInt).orElse(Runtime.getRuntime().availableProcessors() * 2);
    }

    public int getManagementServerThreadCount() {
        Optional<String> threadCount = getServerConfig("--management-server-threads");
        return threadCount.map(Integer::parseInt).orElse(4);
    }

    public Optional<String> getCompactorConfig() {
        return getServerConfig("--compactor-config");
    }

    public Optional<String> getCompactorScriptPath() {
        return getServerConfig("--compactor-script");
    }

    public boolean getRunCompactorAsRoot() {
        return (boolean) getServerConfig().get("--run-compactor-as-root");
    }

    public String getPluginConfigFilePath() {
        String pluginConfigFilePath = getServerConfig(String.class, "--plugin");
        return pluginConfigFilePath == null ? PLUGIN_CONFIG_FILE_PATH : pluginConfigFilePath;
    }

    /**
     * Get an ExecutorService that can be used by the servers to
     * process RPCs. Uses a ServerThreadFactory as the underlying
     * thread factory.
     * @param threadCount   The number of threads to use in the pool
     * @param threadPrefix  The naming prefix
     * @return The newly created ExecutorService
     */
    public ExecutorService getExecutorService(int threadCount, String threadPrefix) {
        return getExecutorService(threadCount,
                new ServerThreadFactory(threadPrefix, new ServerThreadFactory.ExceptionHandler()));
    }

    /**
     * Get an ExecutorService that can be used by the servers to
     * process RPCs.
     * @param threadCount    The number of threads to use in the pool
     * @param threadFactory  The underlying thread factory
     * @return The newly created ExecutorService
     */
    public ExecutorService getExecutorService(int threadCount, @Nonnull ThreadFactory threadFactory) {
        return Executors.newFixedThreadPool(threadCount, threadFactory);
    }

    /**
     * Get a new ExecutorService backed by a cached thread pool.
     * @param threadPrefix  The naming prefix
     * @return The newly created ExecutorService
     */
    public static ExecutorService getCachedExecutorService(@Nonnull String threadPrefix) {
        return Executors.newCachedThreadPool(
                new ServerThreadFactory(threadPrefix, new ServerThreadFactory.ExceptionHandler()));
    }

    /**
     * Get the max number of messages can be sent over per batch.
     * @return
     */
    public int getLogReplicationMaxNumMsgPerBatch() {
        String val = getServerConfig(String.class, "--snapshot-batch");
        return val == null ? DEFAULT_MAX_NUM_MSG_PER_BATCH : Integer.parseInt(val);
    }

    public int getLockLeaseDuration() {
        Integer lockLeaseDuration;
        try {
            lockLeaseDuration = getServerConfig(Integer.class, "--lock-lease");
        } catch (ClassCastException e) {
            // In the testing framework we only support Strings
            lockLeaseDuration = Integer.valueOf(getServerConfig(String.class, "--lock-lease"));
        }
        return lockLeaseDuration == null ? Lock.leaseDuration : lockLeaseDuration;
    }

    /**
     * Get the max size of the log replication data message used by both snapshot data message and
     * log entry sync data message.
     *
     * @return max data message size
     */
    public int getLogReplicationMaxDataMessageSize() {
        String val = getServerConfig(String.class, "--max-replication-data-message-size");
        return val == null ? DEFAULT_MAX_MSG_BATCH_SIZE : Integer.parseInt(val);
    }

    /**
     * Get the max size of LR's runtime cache in number of entries.
     *
     * @return max cache number of entries
     */
    public int getLogReplicationCacheMaxSize() {
        String val = getServerConfig(String.class, "--lrCacheSize");
        return val == null ? MAX_CACHE_NUM_ENTRIES : Integer.parseInt(val);
    }

    /**
     * Get the max write size of a transaction for LR's runtime.
     * @return max write size of a transaction
     */
    public int getMaxWriteSize() {
        String val = getServerConfig(String.class, "--max-replication-write-size");
        return val == null ? Integer.MAX_VALUE : Integer.parseInt(val);
    }

    public int getMaxSnapshotEntriesApplied() {
        String val = getServerConfig(String.class, "--max-snapshot-entries-applied");
        return val == null ? DEFAULT_MAX_SNAPSHOT_ENTRIES_APPLIED : Integer.parseInt(val);
    }

    public int getSnapshotApplyWaitTime() {
        String val = getServerConfig(String.class, "--wait-before-apply-ms");
        return val == null ? 0 : Integer.parseInt(val);
    }

    public int getNegotiatingStateWaitTime() {
        String val = getServerConfig(String.class, "--wait-in-negotiating-state-ms");
        return val == null ? 0 : Integer.parseInt(val);
    }

    /**
     * Cleanup the DataStore files with names that are prefixes of the specified
     * fileName when so that the number of these files don't exceed the user-defined
     * retention limit. Cleanup is always done on files with lower epochs.
     */
    private void dataStoreFileCleanup(String fileName) {
        String logDirPath = getServerConfig(String.class, "--log-path");
        if (logDirPath == null) {
            return;
        }

        File logDir = new File(logDirPath);
        Set<String> prefixesToClean = getDsFilePrefixesForCleanup();
        int numRetention = Integer.parseInt(getServerConfig(String.class, "--metadata-retention"));

        prefixesToClean.stream()
                .filter(fileName::startsWith)
                .forEach(prefix -> {
                    File[] foundFiles = logDir.listFiles((dir, name) -> name.startsWith(prefix));
                    if (foundFiles == null || foundFiles.length <= numRetention) {
                        log.debug("DataStore cleanup not started for prefix: {}.", prefix);
                        return;
                    }
                    log.debug("Start cleaning up DataStore files with prefix: {}.", prefix);
                    Arrays.stream(foundFiles)
                            .sorted(Comparator.comparingInt(file -> {
                                // Extract epoch number from file name and cast to int for comparision
                                Matcher matcher = Pattern.compile("\\d+").matcher(file.getName());
                                return matcher.find(prefix.length()) ? Integer.parseInt(matcher.group()) : 0;
                            }))
                            .limit(foundFiles.length - numRetention)
                            .forEach(file -> {
                                try {
                                    if (Files.deleteIfExists(file.toPath())) {
                                        log.info("Removed DataStore file: {}", file.getName());
                                    }
                                } catch (Exception e) {
                                    log.error("Error when cleaning up DataStore files", e);
                                }
                            });
                });
    }

    /**
     * Get the {@link ChannelImplementation} to use.
     *
     * @return The server channel type.
     */
    public ChannelImplementation getChannelImplementation() {
        final String type = getServerConfig(String.class, "--implementation");
        return ChannelImplementation.valueOf(type.toUpperCase());
    }

    /**
     * Get an instance of {@link CorfuRuntimeParameters} representing the default Corfu Runtime's
     * parameters.
     *
     * @return an instance of {@link CorfuRuntimeParameters}
     */
    public CorfuRuntimeParameters getManagementRuntimeParameters() {
        int checkpointTriggerFreqMs = 0;
        if (serverConfig.get("--compaction-trigger-freq-ms") != null) {
            checkpointTriggerFreqMs = Integer.parseInt((String)serverConfig.get("--compaction-trigger-freq-ms"));
        }
        return CorfuRuntime.CorfuRuntimeParameters.builder()
                .priorityLevel(PriorityLevel.HIGH)
                .nettyEventLoop(clientGroup)
                .shutdownNettyEventLoop(false)
                .tlsEnabled((Boolean) serverConfig.get("--enable-tls"))
                .keyStore((String) serverConfig.get(ConfigParamNames.KEY_STORE))
                .ksPasswordFile((String) serverConfig.get(ConfigParamNames.KEY_STORE_PASS_FILE))
                .trustStore((String) serverConfig.get(ConfigParamNames.TRUST_STORE))
                .tsPasswordFile((String) serverConfig.get(ConfigParamNames.TRUST_STORE_PASS_FILE))
                .saslPlainTextEnabled((Boolean) serverConfig.get("--enable-sasl-plain-text-auth"))
                .usernameFile((String) serverConfig.get("--sasl-plain-text-username-file"))
                .passwordFile((String) serverConfig.get("--sasl-plain-text-password-file"))
                // Disable FileWatcher for management client as when the certs are changed,
                // server already re-initializes the connections
                .disableFileWatcher(true)
                .bulkReadSize(Integer.parseInt((String) serverConfig.get("--batch-size")))
                .clientName("CorfuServer")
                .checkpointTriggerFreqMillis(checkpointTriggerFreqMs)
                .build();
    }

    /**
     * Generate a Node Id if not present.
     */
    private void generateNodeId() {
        Optional<String> currentId = getDataStore().find(NODE_ID_RECORD);
        if (currentId.isPresent()) {
            log.info("Node Id = {}", currentId);
        } else {
            String idString = UuidUtils.asBase64(UUID.randomUUID());
            log.info("No Node Id, setting to new Id={}", idString);
            getDataStore().put(NODE_ID_RECORD, idString);
        }
    }

    /**
     * Get the node id as an UUID.
     *
     * @return  A UUID for this node.
     */
    public UUID getNodeId() {
        return UuidUtils.fromBase64(getNodeIdBase64());
    }

    /** Get the node id as a base64 string.
     *
     * @return A node ID for this node, as a base64 string.
     */
    public String getNodeIdBase64() {
        return getDataStore().get(NODE_ID_RECORD);
    }

    /**
     * Get a field from the server configuration map.
     *
     * @param type          The type of the field.
     * @param optionName    The name of the option to retrieve.
     * @param <T>           The type of the field to return.
     * @return              The field with the give option name.
     */
    @SuppressWarnings("unchecked")
    public <T> T getServerConfig(Class<T> type, String optionName) {
        return (T) getServerConfig().get(optionName);
    }

    /**
     * Get a field from the server configuration map.
     *
     * @param optionName    The name of the option to retrieve.
     * @param <T>           The type of the field to return.
     * @return              The field with the give option name.
     */
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getServerConfig(String optionName) {
        return Optional.ofNullable((T) getServerConfig().get(optionName));
    }

    /**
     * Install a single node layout if and only if no layout is currently installed.
     * Synchronized, so this method is thread-safe.
     *
     *  @return True, if a new layout was installed, false otherwise.
     */
    public synchronized boolean installSingleNodeLayoutIfAbsent() {
        if (isSingleNodeSetup() && !findCurrentLayout().isPresent()) {
            setCurrentLayout(getNewSingleNodeLayout());
            return true;
        }
        return false;
    }

    /**
     * Check if it's a single node setup.
     * @return True if it is, false otherwise.
     */
    public boolean isSingleNodeSetup(){
        return Optional
                .ofNullable(getServerConfig().get("--single"))
                .map(single -> Boolean.parseBoolean(single.toString()))
                .orElse(false);
    }

    /**
     * Get a new single node layout used for self-bootstrapping a server started with
     * the -s flag.
     *
     *  @returns A new single node layout with a unique cluster Id
     *  @throws IllegalArgumentException    If the cluster id was not auto, base64 or a UUID string
     */
    public Layout getNewSingleNodeLayout() {
        final String clusterIdString = (String) getServerConfig().get("--cluster-id");
        UUID clusterId;
        if (clusterIdString.equals("auto")) {
            clusterId = UUID.randomUUID();
        } else {
            // Is it a UUID?
            try {
                clusterId = UUID.fromString(clusterIdString);
            } catch (IllegalArgumentException ignore) {
                // Must be a base64 id, otherwise we will throw InvalidArgumentException again
                clusterId = UuidUtils.fromBase64(clusterIdString);
            }
        }

        String localAddress = getNodeLocator().toEndpointUrl();

        log.info("getNewSingleNodeLayout: Bootstrapping with cluster Id {} [{}], localAddress {}",
            clusterId, UuidUtils.asBase64(clusterId), localAddress);

        return new Layout(
            Collections.singletonList(localAddress),
            Collections.singletonList(localAddress),
            Collections.singletonList(new LayoutSegment(
                Layout.ReplicationMode.CHAIN_REPLICATION,
                0L,
                -1L,
                Collections.singletonList(
                    new Layout.LayoutStripe(
                        Collections.singletonList(localAddress)
                    )
                )
            )),
            0L,
            clusterId
        );
    }

    public Optional<Layout> findCurrentLayout() {
        return Optional.ofNullable(getCurrentLayout());
    }

    /**
     * Get the current {@link Layout} stored in the {@link DataStore}.
     *
     * @return The current stored {@link Layout}
     */
    public Layout getCurrentLayout() {
        return dataStore.get(CURR_LAYOUT_RECORD);
    }

    /**
     * Set the current {@link Layout} stored in the {@link DataStore}.
     *
     * @param layout The {@link Layout} to set in the {@link DataStore}.
     */
    public void setCurrentLayout(Layout layout) {
        getDataStore().put(CURR_LAYOUT_RECORD, layout);
    }

    /**
     * Get the list of servers registered in serverRouter
     *
     * @return A list of servers registered in serverRouter
     */
    public List<AbstractServer> getServers() {
        return serverRouter.getServers();
    }

    /**
     * The epoch of this router. This is managed by the base server implementation.
     */
    public synchronized long getServerEpoch() {
        return dataStore.get(SERVER_EPOCH_RECORD, Layout.INVALID_EPOCH);
    }

    /**
     * Set the serverRouter epoch.
     *
     * @param serverEpoch the epoch to set
     */
    public synchronized void setServerEpoch(long serverEpoch, IServerRouter r) {
        Long lastEpoch = dataStore.get(SERVER_EPOCH_RECORD);
        if (lastEpoch == null || lastEpoch < serverEpoch) {
            dataStore.put(SERVER_EPOCH_RECORD, serverEpoch);
            r.setServerEpoch(serverEpoch);
            getServers().forEach(s -> s.sealServerWithEpoch(serverEpoch));
        } else if (lastEpoch > serverEpoch){
            // Regressing, throw an exception.
            throw new WrongEpochException(lastEpoch);
        }
        // If both epochs are same then no need to do anything.
    }

    public void setLayoutInHistory(Layout layout) {
        KvRecord<Layout> currLayoutRecord = KvRecord.of(
                PREFIX_LAYOUTS, String.valueOf(layout.getEpoch()), Layout.class
        );
        dataStore.put(currLayoutRecord, layout);
    }

    /**
     * Persists the sequencer epoch. This is set only by the SequencerServer in the resetServer.
     * No lock required as it relies on the resetServer lock.
     *
     * @param sequencerEpoch Epoch to persist.
     */
    public void setSequencerEpoch(long sequencerEpoch) {
        dataStore.put(SEQUENCER_RECORD, sequencerEpoch);
    }

    /**
     * Fetch the persisted sequencer epoch.
     *
     * @return Sequencer epoch.
     */
    public long getSequencerEpoch() {
        return dataStore
                .get(SEQUENCER_RECORD, Layout.INVALID_EPOCH);
    }

    /**
     * Sets the management layout in the persistent datastore.
     *
     * @param newLayout Layout to be persisted
     */
    public synchronized Layout saveManagementLayout(Layout newLayout) {
        Layout currentLayout = copyManagementLayout();

        // Cannot update with a null layout.
        if (newLayout == null) {
            log.warn("Attempted to update with null. Current layout: {}", currentLayout);
            return currentLayout;
        }

        // Update only if new layout has a higher epoch than the existing layout.
        if (currentLayout == null || newLayout.getEpoch() > currentLayout.getEpoch()) {
            dataStore.put(MANAGEMENT_LAYOUT_RECORD, newLayout);
            currentLayout = copyManagementLayout();
            log.info("Update to new layout at epoch {}", currentLayout.getEpoch());
            return currentLayout;
        }

        return currentLayout;

    }

    /**
     * Fetches the management layout from the persistent datastore.
     *
     * @return The last persisted layout
     */
    public Layout getManagementLayout() {
        return dataStore.get(MANAGEMENT_LAYOUT_RECORD);
    }

    /**
     * Sets the log unit epoch water mark.
     *
     * @param resetEpoch Epoch at which the reset command was received.
     */
    public synchronized void setLogUnitEpochWaterMark(long resetEpoch) {
        dataStore.put(LOG_UNIT_WATERMARK_RECORD, resetEpoch);
    }

    /**
     * Fetches the epoch at which the last epochWaterMark operation was received.
     *
     * @return Reset epoch.
     */
    public synchronized long getLogUnitEpochWaterMark() {
        Long resetEpoch = dataStore.get(LOG_UNIT_WATERMARK_RECORD);
        return resetEpoch == null ? Layout.INVALID_EPOCH : resetEpoch;
    }

    /**
     * Fetches and creates a copy of the Management Layout from the local datastore.
     *
     * @return Copy of the management layout from the datastore.
     */
    public Layout copyManagementLayout() {
        Layout l = getManagementLayout();
        if (l != null) {
            return new Layout(l);
        } else {
            return null;
        }
    }

    /**
     * Shuts down the workerGroup and spawns a new one
     */
    public void refreshWorkerGroupThreads() {
        CorfuRuntime.CorfuRuntimeParameters params = getManagementRuntimeParameters();

        workerGroup.shutdownGracefully(
                params.getNettyShutdownQuitePeriod(),
                params.getNettyShutdownTimeout(),
                TimeUnit.MILLISECONDS
        );

        try {
            // Wait a while for existing tasks to terminate
            if (!workerGroup.awaitTermination(params.getNettyShutdownTimeout(), TimeUnit.MILLISECONDS)) {
                log.error("refreshWorkerGroup: Executor Pool workerGroup did not terminate.");
            } else {
                log.info("refreshWorkerGroup: Executor Pool workerGroup terminated successfully.");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            workerGroup.shutdownNow();
            // Preserve interrupt status on the current thread
            Thread.currentThread().interrupt();
        }

        log.info("refreshWorkerGroup: Creating new workerGroup threads. workerGroup current state:" +
                        " isShuttingDown {}, isShutdown {}, isTerminated {}.",
                workerGroup.isShuttingDown(),
                workerGroup.isShutdown(),
                workerGroup.isTerminated());

        workerGroup = getNewWorkerGroup();
    }

    /**
     * Get a new "worker" group, which services incoming requests.
     *
     * @return A worker group.
     */
    private @Nonnull EventLoopGroup getNewWorkerGroup() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(getThreadPrefix() + "worker-%d")
                .build();

        final int requestedThreads =
                Integer.parseInt(getServerConfig(String.class, "--Threads"));
        final int numThreads = requestedThreads == 0
                ? Runtime.getRuntime().availableProcessors() * 2
                : requestedThreads;
        EventLoopGroup group = getChannelImplementation().getGenerator()
            .generate(numThreads, threadFactory);

        log.info("getWorkerGroup: Type {} with {} threads",
                group.getClass().getSimpleName(), numThreads);
        return group;
    }

    /**
     * Get a new "client" group, which services incoming client requests.
     *
     * @return A worker group.
     */
    private @Nonnull EventLoopGroup getNewClientGroup() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(getThreadPrefix() + "client-%d")
                .build();

        final int requestedThreads =
            Integer.parseInt(getServerConfig(String.class, "--Threads"));
        final int numThreads = requestedThreads == 0
            ? Runtime.getRuntime().availableProcessors() * 2
            : requestedThreads;
        EventLoopGroup group = getChannelImplementation().getGenerator()
            .generate(numThreads, threadFactory);

        log.info("getClientGroup: Type {} with {} threads",
            group.getClass().getSimpleName(), numThreads);
        return group;
    }

    /**
     * Get the prefix for threads this server creates.
     *
     * @return A string that should be prepended to threads this server creates.
     */
    public @Nonnull String getThreadPrefix() {
        final String prefix = getServerConfig(String.class, "--Prefix");
        if (prefix.equals("")) {
            return "";
        } else {
            return prefix + "-";
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>Cleans up and releases all resources (such as thread pools and files) opened
     * by this {@link ServerContext}.
     */
    @Override
    public void close() {
        CorfuRuntimeParameters params = getManagementRuntimeParameters();
        // Shutdown the active event loops unless they were provided to us
        if (!getChannelImplementation().equals(ChannelImplementation.LOCAL)) {
            clientGroup.shutdownGracefully(
                    params.getNettyShutdownQuitePeriod(),
                    params.getNettyShutdownTimeout(),
                    TimeUnit.MILLISECONDS
            );
            workerGroup.shutdownGracefully(
                    params.getNettyShutdownQuitePeriod(),
                    params.getNettyShutdownTimeout(),
                    TimeUnit.MILLISECONDS
            );
        }
    }
}
