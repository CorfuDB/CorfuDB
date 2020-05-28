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
import org.corfudb.util.NodeLocator;
import org.corfudb.utils.CommonTypes;
import org.corfudb.utils.LogReplicationStreams.Namespace;
import org.corfudb.utils.LogReplicationStreams.TableInfo;


import javax.annotation.Nonnull;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
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
        if (streamsToReplicateTableExists()) {
            // The table exists but it may have been created by another runtime in which case, it has to be opened with
            // key/value/metadata type info
            openExistingStreamInfoTable();
        } else {
            // Create the table
            initializeReplicationStreamsTable(logReplicationStreamNameFetcher.fetchStreamsToReplicate());
        }
        // TODO - pankti - check the version.  If the table version is <(current version), delete the table and wait
        // for it to get created
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
            logReplicationStreamNameFetcher= (LogReplicationStreamNameFetcher) plugin.getDeclaredConstructor()
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


    private boolean streamsToReplicateTableExists() {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        try {
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, "LogReplicationStreams");
        } catch (NoSuchElementException e) {
            // Table does not exist
            return false;
        } catch (IllegalArgumentException e) { }
        return true;
    }

    private void openExistingStreamInfoTable() {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        try {
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, "LogReplicationStreams", TableInfo.class,
                    Namespace.class, CommonTypes.Uuid.class, TableOptions.builder().build());
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.warn("Exception when opening existing table {}", e);
        }
    }

    private void initializeReplicationStreamsTable(Map<String, String> streams) {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        try {
            corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, "LogReplicationStreams", TableInfo.class,
                    Namespace.class, CommonTypes.Uuid.class, TableOptions.builder().build());
            TxBuilder tx = corfuStore.tx(CORFU_SYSTEM_NAMESPACE);
            for (Map.Entry<String, String> entry : streams.entrySet()) {
                TableInfo tableInfo = TableInfo.newBuilder().setName(entry.getKey()).build();
                Namespace namespace = Namespace.newBuilder().setName(entry.getValue()).build();
                CommonTypes.Uuid uuid = CommonTypes.Uuid.newBuilder().setLsb(0L).setMsb(0L).build();
                tx.create("LogReplicationStreams", tableInfo, namespace, uuid);
            }
            tx.commit();
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            log.warn("Exception when opening the table {}", e);
        }
    }

    private Set<String> getStreamsToReplicateFromTable() {
        CorfuStore corfuStore = new CorfuStore(corfuRuntime);
        corfuStore.openTable(CORFU_SYSTEM_NAMESPACE, "LogReplicationStreams");
        Query q = corfuStore.query(CORFU_SYSTEM_NAMESPACE);
        Set<TableInfo> tables = q.keySet("LogReplicationStreams", null);
        Set<String> tableNames = new HashSet<>();
        tables.forEach(table -> {
            log.info("Retrieved {}", table.getName());
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

}
