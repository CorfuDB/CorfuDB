package org.corfudb.logreplication.runtime;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoopGroup;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.transport.logreplication.LogReplicationClientRouter;
import org.corfudb.infrastructure.logreplication.LogReplicationPluginConfig;
import org.corfudb.transport.logreplication.LogReplicationServerRouter;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.LogReplicationTransportType;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.logreplication.LogReplicationSourceManager;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.infrastructure.CorfuReplicationDiscoveryService;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryMetadataResponse;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.NodeRouterPool;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.InterClusterConnectionDescriptor;
import org.corfudb.util.NodeLocator;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;


/**
 * Client Runtime to connect to a Corfu Log Replication Server
 *
 * @author amartinezman
 */
@Slf4j
public class CorfuLogReplicationRuntime {
    /**
     * The parameters used to configure this {@link CorfuLogReplicationRuntime}.
     */
    @Getter
    private final LogReplicationRuntimeParameters parameters;

    /**
     * Transport type - custom defined or netty
     * TODO: this will be removed as soon as we implement Netty as an Adapter
     */
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
     * Log Replication Source Manager - to local Corfu Log Unit
     */
    @Getter
    private LogReplicationSourceManager sourceManager;

    /**
     * The {@link EventLoopGroup} provided to netty routers.
     */
    @Getter
    private final EventLoopGroup nettyEventLoop;

    private LogReplicationConfig logReplicationConfig;

    @Getter
    private CorfuRuntime corfuRuntime;

    public CorfuLogReplicationRuntime(@Nonnull LogReplicationRuntimeParameters parameters) {
        this.parameters = parameters;

        this.transport = parameters.getTransport();

        this.logReplicationConfig = parameters.getReplicationConfig();

        // Generate or set the NettyEventLoop
        nettyEventLoop =  getNewEventLoopGroup();

        // Initializing the node router pool.
        nodeRouterPool = new NodeRouterPool(getRouterFunction, true);

        connectToCorfuRuntime();
    }

    private void connectToCorfuRuntime() {
        corfuRuntime = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder().build())
            .parseConfigurationString(parameters.getLocalCorfuEndpoint());
        corfuRuntime.connect();
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
    private final Function<InterClusterConnectionDescriptor, IClientRouter> getRouterFunction = (site) -> {
                String address = site.getRemoteNodeLocator().toEndpointUrl();
                NodeLocator node = NodeLocator.parseString(address);
                IClientRouter newRouter;

                log.trace("Get Router for underlying {} transport on {}", transport, address);

                if (transport.equals(LogReplicationTransportType.CUSTOM)) {

                    LogReplicationPluginConfig config = new LogReplicationPluginConfig(LogReplicationServerRouter.PLUGIN_CONFIG_FILE_PATH);

                    File jar = new File(config.getTransportAdapterJARPath());

                    try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
                        Class adapter = Class.forName(config.getTransportClientClassCanonicalName(), true, child);
                        newRouter = new LogReplicationClientRouter(site.getRemoteNodeLocator(), site.getRemoteClusterId(),
                                getParameters(), adapter, site.getLocalClusterId());
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

    public void connect(CorfuReplicationDiscoveryService discoveryService) {
        log.info("Connected");
        NodeLocator remoteNodeLocator = NodeLocator.parseString(parameters.getRemoteLogReplicationServerEndpoint());
        InterClusterConnectionDescriptor siteDescriptor = new InterClusterConnectionDescriptor(remoteNodeLocator,
                parameters.getRemoteSiteId(), parameters.getLocalSiteId());
        IClientRouter router = getRouter(siteDescriptor);
        client = new LogReplicationClient(router, discoveryService, parameters.getRemoteSiteId());

        log.info("Set Source Manager to connect to local Corfu on {}", parameters.getLocalCorfuEndpoint());
        sourceManager = new LogReplicationSourceManager(parameters.getLocalCorfuEndpoint(),
                client, logReplicationConfig);
    }

    public LogReplicationQueryLeaderShipResponse queryLeadership() throws ExecutionException, InterruptedException {
        log.info("Request leadership of client {}:{}", client.getRouter().getHost(), client.getRouter().getPort());
        return client.sendQueryLeadership().get();
    }

    public LogReplicationQueryMetadataResponse sendQueryMetadataRequest() throws Exception {
        log.info("Send Query Metadata Request");
        return client.sendQueryMetadata().get();
    }

    /**
     * Get a router, given the address.
     *
     * @param remoteSite remote site descriptor, includes address of router to get and remote site id.
     * @return The router.
     */
    public IClientRouter getRouter(InterClusterConnectionDescriptor remoteSite) {
        return nodeRouterPool.getRouter(remoteSite);
    }

    /**
     * Start snapshot (full) sync
     */
    public void startSnapshotSync() {
        UUID snapshotSyncRequestId = sourceManager.startSnapshotSync();
        log.info("Start Snapshot Sync[{}]", snapshotSyncRequestId);
    }

    public void startLogEntrySync(LogReplicationEvent event) {
        sourceManager.startReplication(event);
    }

    public void startReplication(LogReplicationEvent event) {
        sourceManager.startReplication(event);
    }

    /**
     * Clean up router, stop source manager.
     */
    //TODO: stop the router etc.
    public void stop() {
        sourceManager.shutdown();
    }

    public long getMaxStreamTail() {
        Set<String> streamsToReplicate = logReplicationConfig.getStreamsToReplicate();
        long maxTail = Address.NON_ADDRESS;
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

    public long getNumEntriesToSend(long ts) {
       long ackTS = sourceManager.getLogReplicationFSM().getAckedTimestamp();
       return ts - ackTS;
    }
}
