package org.corfudb.infrastructure;

import com.codahale.metrics.MetricRegistry;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoopGroup;
import java.time.Duration;
import java.util.*;

import java.util.concurrent.ThreadFactory;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.comm.ChannelImplementation;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.ConservativeFailureHandlerPolicy;
import org.corfudb.runtime.view.IReconfigurationHandlerPolicy;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.infrastructure.management.FailureDetector;
import org.corfudb.infrastructure.management.HealingDetector;
import org.corfudb.infrastructure.management.IDetector;
import org.corfudb.runtime.view.SequencerHealingPolicy;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.UuidUtils;

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
    private static final String PREFIX_PHASE_1 = "PHASE_1";
    private static final String KEY_SUFFIX_PHASE_1 = "RANK";
    private static final String PREFIX_PHASE_2 = "PHASE_2";
    private static final String KEY_SUFFIX_PHASE_2 = "DATA";
    private static final String PREFIX_LAYOUTS = "LAYOUTS";

    // Sequencer Server
    private static final String PREFIX_TAIL_SEGMENT = "TAIL_SEGMENT";
    private static final String KEY_TAIL_SEGMENT = "CURRENT";
    private static final String PREFIX_STARTING_ADDRESS = "STARTING_ADDRESS";
    private static final String KEY_STARTING_ADDRESS = "CURRENT";
    private static final String KEY_SEQUENCER = "SEQUENCER";
    private static final String PREFIX_SEQUENCER_EPOCH = "EPOCH";

    // Management Server
    private static final String PREFIX_MANAGEMENT = "MANAGEMENT";
    private static final String MANAGEMENT_LAYOUT = "LAYOUT";

    /** The node Id, stored as a base64 string. */
    public static final String NODE_ID = "NODE_ID";

    /**
     * various duration constants.
     */
    public static final Duration SMALL_INTERVAL = Duration.ofMillis(60_000);
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
    private IDetector failureDetector;

    @Getter
    @Setter
    private IDetector healingDetector;

    @Getter
    @Setter
    private IReconfigurationHandlerPolicy failureHandlerPolicy;

    @Getter
    @Setter
    private IReconfigurationHandlerPolicy healingHandlerPolicy;

    @Getter
    private final EventLoopGroup clientGroup;

    @Getter
    private final EventLoopGroup bossGroup;

    @Getter
    private final EventLoopGroup workerGroup;

    @Getter
    @Setter
    private boolean bindToAllInterfaces = false;

    @Getter
    public static final MetricRegistry metrics = new MetricRegistry();

    /**
     * Returns a new ServerContext.
     *
     * @param serverConfig map of configuration strings to objects
     */
    public ServerContext(Map<String, Object> serverConfig) {
        this.serverConfig = serverConfig;
        this.dataStore = new DataStore(serverConfig);
        generateNodeId();
        this.serverRouter = serverRouter;
        this.failureDetector = new FailureDetector();
        this.healingDetector = new HealingDetector();
        this.failureHandlerPolicy = new ConservativeFailureHandlerPolicy();
        this.healingHandlerPolicy = new SequencerHealingPolicy();

        // Setup the netty event loops. In tests, these loops may be provided by
        // a test framework to save resources.
        final boolean providedEventLoops =
                 getChannelImplementation().equals(ChannelImplementation.LOCAL);

        clientGroup = providedEventLoops
            ? getServerConfig(EventLoopGroup.class, "client") :
            getNewClientGroup();
        workerGroup = providedEventLoops
            ? getServerConfig(EventLoopGroup.class, "worker") :
            getNewWorkerGroup();
        bossGroup = providedEventLoops
            ? getServerConfig(EventLoopGroup.class, "boss") :
            getNewBossGroup();

        // Metrics setup & reporting configuration
        String mp = "corfu.server.";
        synchronized (metrics) {
            if (!isMetricsReportingSetUp(metrics)) {
//                addJvmMetrics(metrics, mp);
//                MetricsUtils.addCacheGauges(metrics, mp + "datastore.cache.", dataStore.getCache());
                MetricsUtils.metricsReportingSetup(metrics);
            }
        }
    }

    /** Get the {@link ChannelImplementation}to use.
     *
     * @return              The server channel type.
     */
    public ChannelImplementation getChannelImplementation() {
        final String type = getServerConfig(String.class, "--implementation");
        return ChannelImplementation.valueOf(type.toUpperCase());
    }


    public CorfuRuntimeParameters getDefaultRuntimeParameters() {
        return CorfuRuntime.CorfuRuntimeParameters.builder()
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
                .build();
    }

    /** Generate a Node Id if not present.
     *
     */
    private void generateNodeId() {
        String currentId = getDataStore().get(String.class, "", ServerContext.NODE_ID);
        if (currentId == null) {
            String idString = UuidUtils.asBase64(UUID.randomUUID());
            log.info("No Node Id, setting to new Id={}", idString);
            getDataStore().put(String.class, "", ServerContext.NODE_ID, idString);
        } else {
            log.info("Node Id = {}", currentId);
        }
    }

    /** Get the node id as an UUID.
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
        return getDataStore().get(String.class, "", ServerContext.NODE_ID);
    }

    /** Get a field from the server configuration map.
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


    /** Install a single node layout if and only if no layout is currently installed.
     *  Synchronized, so this method is thread-safe.
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

    /** Get a new single node layout used for self-bootstrapping a server started with
     *  the -s flag.
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

    /** Get the current {@link Layout} stored in the {@link DataStore}.
     *  @return The current stored {@link Layout}
     */
    public Layout getCurrentLayout() {
        return getDataStore().get(Layout.class, PREFIX_LAYOUT, KEY_LAYOUT);
    }

    /** Set the current {@link Layout} stored in the {@link DataStore}.
     *
     * @param layout The {@link Layout} to set in the {@link DataStore}.
     */
    public void setCurrentLayout(Layout layout) {
        getDataStore().put(Layout.class, PREFIX_LAYOUT, KEY_LAYOUT, layout);
    }

    /**
     * The epoch of this router. This is managed by the base server implementation.
     */
    public synchronized long getServerEpoch() {
        Long epoch = dataStore.get(Long.class, PREFIX_EPOCH, KEY_EPOCH);
        return epoch == null ? 0 : epoch;
    }

    /**
     * Set the serverRouter epoch.
     *
     * @param serverEpoch the epoch to set
     */
    public synchronized void setServerEpoch(long serverEpoch, IServerRouter r) {
        Long lastEpoch = dataStore.get(Long.class, PREFIX_EPOCH, KEY_EPOCH);
        if (lastEpoch == null || lastEpoch < serverEpoch) {
            dataStore.put(Long.class, PREFIX_EPOCH, KEY_EPOCH, serverEpoch);
            r.setServerEpoch(serverEpoch);
        } else if (serverEpoch == lastEpoch) {
            // Setting to the same epoch, don't need to do anything.
        } else {
            // Regressing, throw an exception.
            throw new WrongEpochException(lastEpoch);
        }
    }

    public Rank getPhase1Rank() {
        return dataStore.get(Rank.class, PREFIX_PHASE_1,
                getServerEpoch() + KEY_SUFFIX_PHASE_1);
    }

    public void setPhase1Rank(Rank rank) {
        dataStore.put(Rank.class, PREFIX_PHASE_1,
                getServerEpoch() + KEY_SUFFIX_PHASE_1, rank);
    }

    public Phase2Data getPhase2Data() {
        return dataStore.get(Phase2Data.class, PREFIX_PHASE_2,
                getServerEpoch() + KEY_SUFFIX_PHASE_2);
    }

    public void setPhase2Data(Phase2Data phase2Data) {
        dataStore.put(Phase2Data.class, PREFIX_PHASE_2,
                getServerEpoch() + KEY_SUFFIX_PHASE_2, phase2Data);
    }

    public void setLayoutInHistory(Layout layout) {
        dataStore.put(Layout.class, PREFIX_LAYOUTS, String.valueOf(layout.getEpoch()), layout);
    }

    public long getTailSegment() {
        Long tailSegment = dataStore.get(Long.class, PREFIX_TAIL_SEGMENT, KEY_TAIL_SEGMENT);
        return tailSegment == null ? 0 : tailSegment;
    }

    public void setTailSegment(long tailSegment) {
        dataStore.put(Long.class, PREFIX_TAIL_SEGMENT, KEY_TAIL_SEGMENT, tailSegment);
    }

    /**
     * Returns the dataStore starting address.
     *
     * @return the starting address
     */
    public long getStartingAddress() {
        Long startingAddress = dataStore.get(Long.class, PREFIX_STARTING_ADDRESS,
                KEY_STARTING_ADDRESS);
        return startingAddress == null ? 0 : startingAddress;
    }

    public void setStartingAddress(long startingAddress) {
        dataStore.put(Long.class, PREFIX_STARTING_ADDRESS, KEY_STARTING_ADDRESS, startingAddress);
    }

    /**
     * Persists the sequencer epoch. This is set only by the SequencerServer in the resetServer.
     * No lock required as it relies on the resetServer lock.
     *
     * @param sequencerEpoch Epoch to persist.
     */
    public void setSequencerEpoch(long sequencerEpoch) {
        dataStore.put(Long.class, KEY_SEQUENCER, PREFIX_SEQUENCER_EPOCH, sequencerEpoch);
    }

    /**
     * Fetch the persisted sequencer epoch.
     *
     * @return Sequencer epoch.
     */
    public long getSequencerEpoch() {
        Long epoch = dataStore.get(Long.class, KEY_SEQUENCER, PREFIX_SEQUENCER_EPOCH);
        return epoch == null ? Layout.INVALID_EPOCH : epoch;
    }

    /**
     * Sets the management layout in the persistent datastore.
     *
     * @param layout Layout to be persisted
     */
    public synchronized void saveManagementLayout(Layout layout) {
        // Cannot update with a null layout.
        if (layout == null) {
            log.warn("saveManagementLayout: Attempted to update with null layout");
            return;
        }
        Layout currentLayout = getManagementLayout();
        // Update only if new layout has a higher epoch than the existing layout.
        if (currentLayout == null || layout.getEpoch() > currentLayout.getEpoch()) {
            // Persisting this new updated layout
            dataStore.put(Layout.class, PREFIX_MANAGEMENT, MANAGEMENT_LAYOUT, layout);
            log.info("saveManagementLayout: Updated to new layout at epoch {}",
                    getManagementLayout().getEpoch());
        } else {
            log.debug("saveManagementLayout: "
                            + "Ignoring layout because new epoch {} <= old epoch {}",
                    layout.getEpoch(), currentLayout.getEpoch());
        }
    }

    /**
     * Fetches the management layout from the persistent datastore.
     *
     * @return The last persisted layout
     */
    public synchronized Layout getManagementLayout() {
        return dataStore.get(Layout.class, PREFIX_MANAGEMENT, MANAGEMENT_LAYOUT);
    }

    /** Get a new "boss" group, which services (accepts) incoming connections.
     *
     * @return              A boss group.
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

    /** Get a new "worker" group, which services incoming requests.
     *
     * @return          A worker group.
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

    /** Get a new "client" group, which services incoming client requests.
     *
     * @return          A worker group.
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

    /** Get the prefix for threads this server creates.
     *
     * @return  A string that should be prepended to threads this server creates.
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
        // Shutdown the active event loops unless they were provided to us
        if (!getChannelImplementation().equals(ChannelImplementation.LOCAL)) {
            clientGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
