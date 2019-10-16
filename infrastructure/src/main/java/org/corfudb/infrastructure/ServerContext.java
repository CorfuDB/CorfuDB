package org.corfudb.infrastructure;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoopGroup;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.comm.ChannelImplementation;
import org.corfudb.infrastructure.datastore.DataStore;
import org.corfudb.infrastructure.datastore.KvDataStore.KvRecord;
import org.corfudb.infrastructure.log.StreamLogDataStore;
import org.corfudb.infrastructure.log.StreamLogParams;
import org.corfudb.infrastructure.paxos.PaxosDataStore;
import org.corfudb.protocols.wireprotocol.PriorityLevel;
import org.corfudb.protocols.wireprotocol.failuredetector.FailureDetectorMetrics;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.ConservativeFailureHandlerPolicy;
import org.corfudb.runtime.view.IReconfigurationHandlerPolicy;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.NodeLocator;
import org.corfudb.util.UuidUtils;

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
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.corfudb.util.MetricsUtils.isMetricsReportingSetUp;

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

    // Failure detector
    private static final String PREFIX_FAILURE_DETECTOR = "FAILURE_DETECTOR";

    // LogUnit Server
    private static final String PREFIX_LOGUNIT = "LOGUNIT";
    private static final String EPOCH_WATER_MARK = "EPOCH_WATER_MARK";

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

    private static final KvRecord<Layout> MANAGEMENT_LAYOUT_RECORD = KvRecord.of(
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
    private final EventLoopGroup bossGroup;

    @Getter
    private final EventLoopGroup workerGroup;

    @Getter (AccessLevel.PACKAGE)
    private final NodeLocator nodeLocator;

    @Getter (AccessLevel.PACKAGE)
    private final String localEndpoint;

    @Getter
    private static final MetricRegistry metrics = new MetricRegistry();

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
            bossGroup = getServerConfig(EventLoopGroup.class, "boss");
        } else {
            clientGroup = getNewClientGroup();
            workerGroup = getNewWorkerGroup();
            bossGroup = getNewBossGroup();
        }

        nodeLocator = NodeLocator
                .parseString(serverConfig.get("--address") + ":" + serverConfig.get("<port>"));
        localEndpoint = nodeLocator.toEndpointUrl();

        // Metrics setup & reporting configuration
        if (!isMetricsReportingSetUp(metrics)) {
            MetricsUtils.metricsReportingSetup(metrics);
        }
    }

    int getBaseServerThreadCount() {
        Integer threadCount = getServerConfig(Integer.class, "--base-server-threads");
        return threadCount == null ? 1 : threadCount;
    }

    int getLayoutServerThreadCount() {
        Integer threadCount = getServerConfig(Integer.class, "--layout-server-threads");
        return threadCount == null ? 1 : threadCount;
    }

    int getLogunitThreadCount() {
        Integer threadCount = getServerConfig(Integer.class, "--logunit-threads");
        return threadCount == null ? Runtime.getRuntime().availableProcessors() * 2 : threadCount;
    }

    int getManagementServerThreadCount() {
        Integer threadCount = getServerConfig(Integer.class, "--management-server-threads");
        return threadCount == null ? 4 : threadCount;
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
    ChannelImplementation getChannelImplementation() {
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
        return CorfuRuntime.CorfuRuntimeParameters.builder()
                .priorityLevel(PriorityLevel.HIGH)
                .nettyEventLoop(clientGroup)
                .shutdownNettyEventLoop(false)
                .tlsEnabled((Boolean) serverConfig.get("--enable-tls"))
                .keyStore((String) serverConfig.get("--keystore"))
                .ksPasswordFile((String) serverConfig.get("--keystore-password-file"))
                .trustStore((String) serverConfig.get("--truststore"))
                .tsPasswordFile((String) serverConfig.get("--truststore-password-file"))
                .saslPlainTextEnabled((Boolean) serverConfig.get("--enable-sasl-plain-text-auth"))
                .usernameFile((String) serverConfig.get("--sasl-plain-text-username-file"))
                .passwordFile((String) serverConfig.get("--sasl-plain-text-password-file"))
                .bulkReadSize(Integer.parseInt((String) serverConfig.get("--batch-size")))
                .build();
    }

    /**
     * Generate a Node Id if not present.
     */
    private void generateNodeId() {
        String currentId = getDataStore().get(NODE_ID_RECORD);
        if (currentId == null) {
            String idString = UuidUtils.asBase64(UUID.randomUUID());
            log.info("No Node Id, setting to new Id={}", idString);
            getDataStore().put(NODE_ID_RECORD, idString);
        } else {
            log.info("Node Id = {}", currentId);
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
     * Install a single node layout if and only if no layout is currently installed.
     * Synchronized, so this method is thread-safe.
     *
     *  @return True, if a new layout was installed, false otherwise.
     */
    public synchronized boolean installSingleNodeLayoutIfAbsent() {
        if ((Boolean) getServerConfig().get("--single") && getCurrentLayout() == null) {
            setCurrentLayout(getNewSingleNodeLayout());
            return true;
        }
        return false;
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
        log.info("getNewSingleNodeLayout: Bootstrapping with cluster Id {} [{}]",
            clusterId, UuidUtils.asBase64(clusterId));
        String localAddress = getServerConfig().get("--address") + ":"
            + getServerConfig().get("<port>");
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

    /**
     * Get the current {@link Layout} stored in the {@link DataStore}.
     *
     * @return The current stored {@link Layout}
     */
    public Layout getCurrentLayout() {
        return getDataStore().get(CURR_LAYOUT_RECORD);
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
        Long epoch = dataStore.get(SERVER_EPOCH_RECORD);
        return epoch == null ? 0 : epoch;
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
        } else if (serverEpoch == lastEpoch) {
            // Setting to the same epoch, don't need to do anything.
        } else {
            // Regressing, throw an exception.
            throw new WrongEpochException(lastEpoch);
        }
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
        Long epoch = dataStore.get(SEQUENCER_RECORD);
        return epoch == null ? Layout.INVALID_EPOCH : epoch;
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
     * Save detected failure in a history. History represents all cluster state changes.
     * Disabled by default.
     *
     * @param detector failure detector state
     */
    public synchronized void saveFailureDetectorMetrics(FailureDetectorMetrics detector) {
        boolean enabled = Boolean.parseBoolean(System.getProperty("corfu.failuredetector", Boolean.FALSE.toString()));
        if (!enabled){
            return;
        }

        KvRecord<FailureDetectorMetrics> fdRecord = KvRecord.of(
                PREFIX_FAILURE_DETECTOR,
                String.valueOf(getManagementLayout().getEpoch()),
                FailureDetectorMetrics.class
        );

        dataStore.put(fdRecord, detector);
    }

    /**
     * Get latest change in a cluster state saved in data store. Or provide default value if history is disabled.
     *
     * @return latest failure saved in the history
     */
    public FailureDetectorMetrics getFailureDetectorMetrics() {
        boolean enabled = Boolean.parseBoolean(System.getProperty("corfu.failuredetector", Boolean.FALSE.toString()));
        if(!enabled){
            return getDefaultFailureDetectorMetric(getManagementLayout());
        }

        KvRecord<FailureDetectorMetrics> fdRecord = KvRecord.of(
                PREFIX_FAILURE_DETECTOR,
                String.valueOf(getManagementLayout().getEpoch()),
                FailureDetectorMetrics.class
        );

        return Optional
                .ofNullable(dataStore.get(fdRecord))
                .orElseGet(() -> getDefaultFailureDetectorMetric(getManagementLayout()));
    }

    /**
     * Provide default metric.
     * @param layout current layout
     * @return default value
     */
    private FailureDetectorMetrics getDefaultFailureDetectorMetric(Layout layout) {
        return FailureDetectorMetrics.builder()
                .localNode(getLocalEndpoint())
                .layout(layout.getLayoutServers())
                .unresponsiveNodes(layout.getUnresponsiveServers())
                .epoch(layout.getEpoch())
                .build();
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
     * Get a new "boss" group, which services (accepts) incoming connections.
     *
     * @return A boss group.
     */
    private EventLoopGroup getNewBossGroup() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat(getThreadPrefix() + "accept-%d")
                .build();
        EventLoopGroup group = getChannelImplementation().getGenerator()
                .generate(1, threadFactory);
        log.info("getBossGroup: Type {}", group.getClass().getSimpleName());
        return group;
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

    private int getCompactionWorkerThreads() {
        int requested = Integer.parseInt(getServerConfig(String.class, "--compaction-worker-threads"));
        if (requested != 0) {
            return requested;
        }

        return Runtime.getRuntime().availableProcessors();
    }

    private int getProtectedSegments() {
        int protectedSegments = Integer.parseInt(getServerConfig(String.class, "--protected-segments"));
        if (protectedSegments < 1) {
            throw new IllegalArgumentException("Number of protected segments for compaction " +
                    "should at least be 1.");
        }

        return protectedSegments;
    }

    /**
     * Get a new instance of {@link StreamLogParams} representing the stream log parameters.
     *
     * @return an instance of {@link StreamLogParams}
     */
    public StreamLogParams getStreamLogParams() {
        return StreamLogParams.builder()
                .logPath(getServerConfig(String.class, "--log-path"))
                .verifyChecksum(!getServerConfig(Boolean.class, "--no-verify"))
                .logSizeQuotaPercentage(Double.parseDouble(getServerConfig(String.class, "--log-size-quota-percentage")))
                .maxOpenStreamSegments(Integer.parseInt(getServerConfig(String.class, "--max-open-stream-segments")))
                .maxOpenGarbageSegments(Integer.parseInt(getServerConfig(String.class, "--max-open-garbage-segments")))
                .compactionPolicyType(getServerConfig(String.class, "--compaction-policy"))
                .compactionInitialDelayMin(Integer.parseInt(getServerConfig(String.class, "--compaction-initial-delay")))
                .compactionPeriodMin(Integer.parseInt(getServerConfig(String.class, "--compaction-period")))
                .maxSegmentsForCompaction(Integer.parseInt(getServerConfig(String.class, "--max-segments-for-compaction")))
                .protectedSegments(getProtectedSegments())
                .compactionWorkers(getCompactionWorkerThreads())
                .segmentGarbageRatioThreshold(Double.parseDouble(getServerConfig(String.class, "--segment-garbage-ratio-threshold")))
                .segmentGarbageSizeThresholdMB(Double.parseDouble(getServerConfig(String.class, "--segment-garbage-size-threshold")))
                .totalGarbageSizeThresholdMB(Double.parseDouble(getServerConfig(String.class, "--total-garbage-size-threshold")))
                .build();
    }

    public StreamLogDataStore getStreamLogDataStore() {
        return new StreamLogDataStore(dataStore);
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
            bossGroup.shutdownGracefully(
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
