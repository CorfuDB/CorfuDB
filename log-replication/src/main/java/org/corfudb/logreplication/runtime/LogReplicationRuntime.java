package org.corfudb.logreplication.runtime;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.EventLoopGroup;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.logreplication.CorfuReplicationTransportConfig;
import org.corfudb.infrastructure.CustomClientRouter;
import org.corfudb.infrastructure.CustomServerRouter;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.LogReplicationTransportType;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.logreplication.SourceManager;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationNegotiationResponse;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;
import org.corfudb.runtime.NodeRouterPool;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.util.NodeLocator;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;


@Slf4j
public class LogReplicationRuntime {
    /**
     * The parameters used to configure this {@link LogReplicationRuntime}.
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
     * Log Replication Source Manager - to local Corfu Log Unit
     */
    @Getter
    private SourceManager sourceManager;

    /**
     * The {@link EventLoopGroup} provided to netty routers.
     */
    @Getter
    private final EventLoopGroup nettyEventLoop;

    public LogReplicationRuntime(@Nonnull LogReplicationRuntimeParameters parameters) {
        this.parameters = parameters;

        this.transport = parameters.getTransport();

        // Generate or set the NettyEventLoop
        nettyEventLoop =  getNewEventLoopGroup();

        // Initializing the node router pool.
        nodeRouterPool = new NodeRouterPool(getRouterFunction);
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

                    CorfuReplicationTransportConfig config = new CorfuReplicationTransportConfig(CustomServerRouter.TRANSPORT_CONFIG_FILE_PATH);

                    File jar = new File(config.getAdapterJARPath());

                    try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
                        Class adapter = Class.forName(config.getAdapterClientClassName(), true, child);
                        newRouter = new CustomClientRouter(node, getParameters(), adapter);
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

    public void connect () {
        log.info("Connected");
        IClientRouter router = getRouter(parameters.getRemoteLogReplicationServerEndpoint());
        client = new LogReplicationClient(router);

        // TODO (Anny) TEMP fix the tables to replicate
        Set<String> tablesToReplicate = new HashSet<>(Arrays.asList("Table001", "Table002", "Table003"));
        LogReplicationConfig config = new LogReplicationConfig(tablesToReplicate, UUID.randomUUID(), UUID.randomUUID());
        log.info("Set Source Manager to connect to local Corfu on {}", parameters.getLocalCorfuEndpoint());
        sourceManager = new SourceManager(parameters.getLocalCorfuEndpoint(),
                client, config);
    }

    public LogReplicationQueryLeaderShipResponse queryLeadership() throws Exception {
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
