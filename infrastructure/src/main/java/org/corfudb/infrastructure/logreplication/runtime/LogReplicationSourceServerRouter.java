package org.corfudb.infrastructure.logreplication.runtime;

import com.google.common.collect.ImmutableList;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.CorfuReplicationManager;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.infrastructure.logreplication.transport.client.ChannelAdapterException;
import org.corfudb.infrastructure.logreplication.transport.server.IServerChannelAdapter;
import org.corfudb.protocols.service.CorfuProtocolMessage;
import org.corfudb.runtime.clients.IClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getDefaultProtocolVersionMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getRequestMsg;

@Slf4j
public class LogReplicationSourceServerRouter extends LogReplicationSourceRouterHelper implements IServerRouter {

    /**
     * This map stores the mapping from message type to netty server handler.
     */
//    private final Map<CorfuMessage.RequestPayloadMsg.PayloadCase, AbstractServer> handlerMap;

    /**
     * The epoch of this router. This is managed by the base server implementation.
     */
    @Getter
    @Setter
    private volatile long serverEpoch;

    /**
     * This map stores the mapping from message type to netty server handler.
     */
    private final Map<CorfuMessage.RequestPayloadMsg.PayloadCase, AbstractServer> msgHandlerMap;


    /** The {@link AbstractServer}s this {@link LogReplicationSourceServerRouter} routes messages for. */
//    final List<AbstractServer> servers;

    /** Construct a new {@link LogReplicationSourceServerRouter}.
     *
     */
    public LogReplicationSourceServerRouter(ClusterDescriptor remoteCluster, String localClusterId,
                                            LogReplicationRuntimeParameters parameters, CorfuReplicationManager replicationManager,
                                            ReplicationSession session, Map<Class, AbstractServer> serverMap) {
        super(remoteCluster, localClusterId, parameters, replicationManager, session, false);
        this.serverEpoch = ((BaseServer) serverMap.get(BaseServer.class)).serverContext.getServerEpoch();
//        this.servers = ImmutableList.copyOf(servers);
        this.msgHandlerMap = new EnumMap<>(CorfuMessage.RequestPayloadMsg.PayloadCase.class);

        serverMap.values().forEach(server -> {
            try {
                server.getHandlerMethods().getHandledTypes().forEach(x -> msgHandlerMap.put(x, server));
            } catch (UnsupportedOperationException ex) {
                log.trace("No registered CorfuMsg handler for server {}", server, ex);
            }
        });

        // create an adapter and set it
//        this.serverAdapter = getAdapter(baseServer.serverContext);

    }

    public void setAdapter(IServerChannelAdapter serverAdapter) {
        this.setServerChannelAdapter(serverAdapter);
    }

//    private IServerChannelAdapter getAdapter(ServerContext serverContext) {
//
//        LogReplicationPluginConfig config = new LogReplicationPluginConfig(serverContext.getPluginConfigFilePath());
//        File jar = new File(config.getTransportAdapterJARPath());
//
//        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
//            Class adapter = Class.forName(config.getTransportServerClassCanonicalName(), true, child);
//            return (IServerChannelAdapter) adapter.getDeclaredConstructor(
//                    ServerContext.class, LogReplicationSinkServerRouter.class, LogReplicationSourceServerRouter.class)
//                    .newInstance(serverContext, null, this);
//        } catch (Exception e) {
//            log.error("Fatal error: Failed to create serverAdapter", e);
//            throw new UnrecoverableCorfuError(e);
//        }
//    }

    // ============ IServerRouter Methods =============

    @Override
    public void sendResponse(CorfuMessage.ResponseMsg response, ChannelHandlerContext ctx) {
        log.info("Ready to send response {}", response.getPayload().getPayloadCase());
        try {
            this.serverChannelAdapter.send(response);
            log.info("Sent response: {}", response);
        } catch (IllegalArgumentException e) {
            log.warn("Illegal response type. Ignoring message.", e);
        }
    }

    @Override
    public Optional<Layout> getCurrentLayout() {
        return Optional.empty();
    }

    @Override
    public List<AbstractServer> getServers() {
        return new ArrayList<>();
    }

    @Override
    public void setServerContext(ServerContext serverContext) {

    }

    // ================================================

    /**
     * When the SOURCE is the connection endpoint, the only request it can receive is the leadership_Query.
     * This request is received and passed to an appropriate handler.
     *
     * @param message
     */
    public void receive(CorfuMessage.RequestMsg message) {
        log.info("Received message {}.", message.getPayload().getPayloadCase());

        AbstractServer handler = msgHandlerMap.get(message.getPayload().getPayloadCase());
        if (handler == null) {
            // The message was unregistered, we are dropping it.
            log.warn("Received unregistered message {}, dropping", message);
        } else {
            if (validateEpoch(message.getHeader())) {
                // Route the message to the handler.
                if (log.isTraceEnabled()) {
                    log.trace("Message routed to {}: {}", handler.getClass().getSimpleName(), message);
                }

                try {
                    handler.handleMessage(message, null, this);
                } catch (Throwable t) {
                    log.error("channelRead: Handling {} failed due to {}:{}",
                            message.getPayload().getPayloadCase(),
                            t.getClass().getSimpleName(),
                            t.getMessage(),
                            t);
                }
                String remoteLeaderId = message.getPayload().getLrLeadershipQuery().getSessionInfo().getLocalClusterId();
                runtimeFSM.setRemoteLeaderNodeId(remoteLeaderId);
            }
        }
    }

    /**
     * Receive messages from the 'custom' serverAdapter implementation. This message will be forwarded
     * for processing.
     *
     * @param message
     */
    public void receive(CorfuMessage.ResponseMsg message) {
        log.info("Received message {}", message.getPayload().getPayloadCase());

        if (validateEpoch(message.getHeader())) {
            if(message.getPayload().getPayloadCase().equals(CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_SUBSCRIBE_REQUEST)) {
                this.startReplication(runtimeFSM.getRemoteLeaderNodeId().get());
            } else {
                super.receive(message);
            }

        }
    }

    /**
     * Validate the epoch of a CorfuMsg, and send a WRONG_EPOCH response if
     * the server is in the wrong epoch. Ignored if the message type is reset (which
     * is valid in any epoch).
     *
     * @param header The incoming header to validate.
     * @return True, if the epoch is correct, but false otherwise.
     */
    private boolean validateEpoch(CorfuMessage.HeaderMsg header) {
        long serverEpoch = getServerEpoch();
        if (!header.getIgnoreEpoch() && header.getEpoch()!= serverEpoch) {
            log.trace("Incoming message with wrong epoch, got {}, expected {}",
                    header.getEpoch(), serverEpoch);
            sendWrongEpochError(header, null);
            return false;
        }
        return true;
    }

    /**
     * {@inheritDoc}
     *
     * @deprecated This operation is no longer supported. The router will only route messages for
     * servers provided at construction time.
     */
    @Override
    @Deprecated
    public void addServer(AbstractServer server) {
        throw new UnsupportedOperationException("No longer supported");
    }

}
