package org.corfudb.logreplication.runtime;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoopGroup;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.LogReplicationClientRouter;
import org.corfudb.infrastructure.logreplication.LogReplicationPluginConfig;
import org.corfudb.infrastructure.LogReplicationServerRouter;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.LogReplicationTransportType;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.logreplication.LogReplicationSourceManager;
import org.corfudb.logreplication.infrastructure.CorfuReplicationDiscoveryService;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationNegotiationResponse;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.NodeRouterPool;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.collections.*;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.NodeLocator;
import org.corfudb.utils.CommonTypes;
import org.corfudb.utils.LogReplicationStreams.Namespace;
import org.corfudb.utils.LogReplicationStreams.TableInfo;
import org.corfudb.utils.LogReplicationStreams.Version;
import org.corfudb.utils.LogReplicationStreams.VersionString;


import javax.annotation.Nonnull;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;


import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class CorfuLogReplicationRuntime {
    /**
     * The parameters used to configure this {@link CorfuLogReplicationRuntime}.
     */
    @Getter
    private final LogReplicationRuntimeParameters parameters;
    private final String LOG_REPLICATION_STREAMS_NAME_TABLE = "LogReplicationStreams";
    private final String LOG_REPLICATION_PLUGIN_VERSION_TABLE = "LogReplicationPluginVersion";

    private LogReplicationTransportType transport;

    /**
     * Node Router Pool.
     */
    private NodeRouterPool nodeRouterPool;

    /**
     * Log Replication Client - to remote Log Replication Server
     */
    private LogReplicationClient client;

    /**
     * Corfu Runtime
     */
    private CorfuRuntime corfuRuntime;

    /**
     * Streams to Replicate
     */
    private Set<String> streamsToReplicate;

    private static final String PLUGIN_CONFIG_FILE_PATH = "/config/corfu/corfu_plugin_config.properties";

    /**
     * Log Replication Source Manager - to local Corfu Log Unit
     */
    @Getter
    private LogReplicationSourceManager sourceManager;

    /**
     * The {@link EventLoopGroup} provided to netty routers.
     */
    @Getter
    private final EventLoopGroup nettyEventLoop;

    /**
     * External Plugin which fetches the streams names to replicate
     */
    @Getter
    private LogReplicationStreamNameFetcher logReplicationStreamNameFetcher;

    public CorfuLogReplicationRuntime(@Nonnull LogReplicationRuntimeParameters parameters) {
        this.parameters = parameters;

        this.transport = parameters.getTransport();

        // Generate or set the NettyEventLoop
        nettyEventLoop =  getNewEventLoopGroup();

        // Initializing the node router pool.
        nodeRouterPool = new NodeRouterPool(getRouterFunction);

        connectToCorfuRuntime();

        initStreamNameFetcherPlugin();

        // Initialize the streamsToReplicate
        if (verifyTableExists(LOG_REPLICATION_PLUGIN_VERSION_TABLE) &&
            verifyTableExists(LOG_REPLICATION_STREAMS_NAME_TABLE)) {
            // The tables exist but may have been created by another runtime in which case they have to be opened with
            // key/value/metadata type info
            openExistingStreamNameAndVersionTables();
            if (!tableVersionMatchesPlugin()) {
                // delete the tables and recreate them
                deleteExistingStreamNameAndVersionTables();
                createStreamNameAndVersionTables(logReplicationStreamNameFetcher.fetchStreamsToReplicate());
            }
        } else {
            // If any 1 of the 2 tables does not exist, delete and recreate them both as they may have been corrupted.
            deleteExistingStreamNameAndVersionTables();
            createStreamNameAndVersionTables(logReplicationStreamNameFetcher.fetchStreamsToReplicate());
        }
        streamsToReplicate = getStreamsToReplicateFromTable();
    }

    private void connectToCorfuRuntime() {
        corfuRuntime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
                .parseConfigurationString(parameters.getLocalCorfuEndpoint());
        corfuRuntime.connect();
    }

    private void initStreamNameFetcherPlugin() {
        LogReplicationPluginConfig config = new LogReplicationPluginConfig(PLUGIN_CONFIG_FILE_PATH);
        File jar = new File(config.getStreamFetcherPluginJARPath());
        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class plugin = Class.forName(config.getStreamFetcherClassCanonicalName(), true, child);
            logReplicationStreamNameFetcher = (LogReplicationStreamNameFetcher) plugin.getDeclaredConstructor()
                .newInstance();
        } catch (Exception e) {
            log.error("Fatal error: Failed to get Stream Fetcher Plugin", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    /**
     * Get a new {@link EventLoopGroup} for scheduling threads for Netty. The
     * {@link EventLoopGroup} is typically passed to a router.
     *
     * @return An {@link EventLoopGroup}.
     */
    private EventLoopGroup getNewEventLoopGroup() {
        // Calculate the number of threads which should be available in the thread pool.
        int numThreads = Runtime.getRuntime().availableProcessors() * 2;
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(parameters.getNettyEventLoopThreadFormat())
                .setUncaughtExceptionHandler(this::handleUncaughtThread)
                .build();
        return parameters.getSocketType().getGenerator().generate(numThreads, factory);
    }

    /**
     * A function to handle getting routers. Used by test framework to inject
     * a test router. Can also be used to provide alternative logic for obtaining
     * a router.
     */
    @Getter
    private final Function<String, IClientRouter> getRouterFunction = (address) -> {
                NodeLocator node = NodeLocator.parseString(address);
                IClientRouter newRouter;

                log.trace("Get Router for underlying {} transport on {}", transport, address);

                if (transport.equals(LogReplicationTransportType.CUSTOM)) {

                    LogReplicationPluginConfig config = new LogReplicationPluginConfig(LogReplicationServerRouter.PLUGIN_CONFIG_FILE_PATH);

                    File jar = new File(config.getTransportAdapterJARPath());

                    try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
                        Class adapter = Class.forName(config.getTransportClientClassCanonicalName(), true, child);
                        newRouter = new LogReplicationClientRouter(node, getParameters(), adapter);
                    } catch (Exception e) {
                        log.error("Fatal error: Failed to create channel", e);
                        throw new UnrecoverableCorfuError(e);
                    }

                } else {
                    newRouter = new NettyClientRouter(node,
                            getNettyEventLoop(),
                            getParameters());
                }

                log.debug("Connecting to new router {}", node);
                try {
                    newRouter.addClient(new LogReplicationHandler());
                } catch (Exception e) {
                    log.warn("Error connecting to router", e);
                    throw e;
                }
                return newRouter;
            };

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

    public void connect(CorfuReplicationDiscoveryService discoveryService, String siteID) {
        log.info("Connected");
        IClientRouter router = getRouter(parameters.getRemoteLogReplicationServerEndpoint());
        client = new LogReplicationClient(router, discoveryService, siteID);

        LogReplicationConfig config = new LogReplicationConfig(streamsToReplicate, UUID.randomUUID(), UUID.randomUUID());
        log.info("Set Source Manager to connect to local Corfu on {}", parameters.getLocalCorfuEndpoint());
        sourceManager = new LogReplicationSourceManager(parameters.getLocalCorfuEndpoint(),
                client, config);
    }


    private boolean verifyTableExists(String tableName) {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        try {
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, tableName);
        } catch (NoSuchElementException e) {
            // Table does not exist
            return false;
        } catch (IllegalArgumentException e) { }
        return true;
    }

    private void openExistingStreamNameAndVersionTables() {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        try {
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_STREAMS_NAME_TABLE, TableInfo.class,
                    Namespace.class, CommonTypes.Uuid.class, TableOptions.builder().build());
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE, VersionString.class,
                    Version.class, CommonTypes.Uuid.class, TableOptions.builder().build());
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.warn("Exception when opening existing table {}", e);
        }
    }

    private boolean tableVersionMatchesPlugin() {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE);
        VersionString versionString = VersionString.newBuilder().setName("Version").build();
        Query q = corfuStore.query(CORFU_SYSTEM_NAMESPACE);
        Version version = (Version) q.getRecord(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString).getPayload();
        return (Objects.equals(version.getVersion(), logReplicationStreamNameFetcher.getVersion()));
    }

    private void deleteExistingStreamNameAndVersionTables() {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        try {
            corfuStore.deleteTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_STREAMS_NAME_TABLE);
            corfuStore.deleteTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE);
        } catch (NoSuchElementException e) {
            // If the table does not exist, simply return
            return;
        }
    }

    private void createStreamNameAndVersionTables(Map<String, String> streams) {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        try {
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_STREAMS_NAME_TABLE, TableInfo.class,
                    Namespace.class, CommonTypes.Uuid.class, TableOptions.builder().build());
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_PLUGIN_VERSION_TABLE, VersionString.class,
                    Version.class, CommonTypes.Uuid.class, TableOptions.builder().build());
            TxBuilder tx = corfuStore.tx(CORFU_SYSTEM_NAMESPACE);

            // Populate the plugin version in the version table
            VersionString versionString = VersionString.newBuilder().setName("Version").build();
            Version version = Version.newBuilder().setVersion(logReplicationStreamNameFetcher.getVersion()).build();
            CommonTypes.Uuid uuid = CommonTypes.Uuid.newBuilder().setLsb(0L).setMsb(0L).build();
            tx.create(LOG_REPLICATION_PLUGIN_VERSION_TABLE, versionString, version, uuid);

            // Copy all stream names to the stream names table
            for (Map.Entry<String, String> entry : streams.entrySet()) {
                TableInfo tableInfo = TableInfo.newBuilder().setName(entry.getKey()).build();
                Namespace namespace = Namespace.newBuilder().setName(entry.getValue()).build();
                uuid = CommonTypes.Uuid.newBuilder().setLsb(0L).setMsb(0L).build();
                tx.create(LOG_REPLICATION_STREAMS_NAME_TABLE, tableInfo, namespace, uuid);
            }
            tx.commit();
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.warn("Exception when opening the table {}", e);
        }
    }

    private Set<String> getStreamsToReplicateFromTable() {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, LOG_REPLICATION_STREAMS_NAME_TABLE);
        Query q = corfuStore.query(CORFU_SYSTEM_NAMESPACE);
        Set<TableInfo> tables = q.keySet(LOG_REPLICATION_STREAMS_NAME_TABLE, null);
        Set<String> tableNames = new HashSet<>();
        tables.forEach(table -> {
            tableNames.add(table.getName());
        });
        return tableNames;
    }

  public LogReplicationQueryLeaderShipResponse queryLeadership() throws ExecutionException, InterruptedException {
      log.info("***** Send QueryLeadership Request on a client to: {}", client.getRouter().getPort());
        return client.sendQueryLeadership().get();
    }


    public LogReplicationNegotiationResponse startNegotiation() throws Exception {
        log.info("Send Negotiation Request");
        return client.sendNegotiationRequest().get();
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

    public void startSnapshotSync() {
        UUID snapshotSyncRequestId = sourceManager.startSnapshotSync();
        log.info("Start Snapshot Sync[{}]", snapshotSyncRequestId);
    }

    public void startLogEntrySync() {
        sourceManager.startReplication();
    }

    /***
     * clean up router, stop source manager.
     */
    //TODO: stop the router etc.
    public void stop() {
        sourceManager.shutdown();
    }

    public long getMaxStreamTail() {
        long maxTail = Address.NON_ADDRESS;
        log.debug("streamtoReplicate " + streamsToReplicate);
        for (String s : streamsToReplicate) {
            UUID currentUUID = CorfuRuntime.getStreamID(s);
            Map<UUID, Long> tailMap = corfuRuntime.getAddressSpaceView().getAllTails().getStreamTails();
            Long currentTail = tailMap.get(currentUUID);
            if (currentTail != null) {
                maxTail = Math.max(maxTail, currentTail);
            }
        }

        return maxTail;
    }

    /**
     *
     * @return
     */
    public long getNumEntriesToSend(long ts) {
       long ackTS = sourceManager.getLogReplicationFSM().getAckedTimestamp();
       return ts - ackTS;
    }
}
