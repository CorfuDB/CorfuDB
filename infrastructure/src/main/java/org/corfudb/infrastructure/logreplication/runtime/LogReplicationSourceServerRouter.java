package org.corfudb.infrastructure.logreplication.runtime;

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
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent;
import org.corfudb.infrastructure.logreplication.transport.server.IServerChannelAdapter;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg.PayloadCase;
import org.corfudb.runtime.view.Layout;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Interacts with the custom transport layer and the Log replication SOURCE components.
 *
 * Being the connection receiver, this router receives messages from the transport layer and forward to the
 * Log Replication Source components via LogReplicationSourceBaseRouter.
 */
@Slf4j
public class LogReplicationSourceServerRouter extends LogReplicationSourceBaseRouter implements IServerRouter {

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

    /** Construct a new {@link LogReplicationSourceServerRouter}.
     *
     */
    public LogReplicationSourceServerRouter(ClusterDescriptor remoteCluster,
                                            LogReplicationRuntimeParameters parameters, CorfuReplicationManager replicationManager,
                                            LogReplicationSession session, Map<Class, AbstractServer> serverMap) {
        super(remoteCluster, parameters, replicationManager, session, false);
        this.serverEpoch = ((BaseServer) serverMap.get(BaseServer.class)).serverContext.getServerEpoch();
        this.msgHandlerMap = new EnumMap<>(CorfuMessage.RequestPayloadMsg.PayloadCase.class);

        serverMap.values().forEach(server -> {
            try {
                server.getHandlerMethods().getHandledTypes().forEach(x -> msgHandlerMap.put(x, server));
            } catch (UnsupportedOperationException ex) {
                log.trace("No registered CorfuMsg handler for server {}", server, ex);
            }
        });
    }

    public void setAdapter(IServerChannelAdapter serverAdapter) {
        this.setServerChannelAdapter(serverAdapter);
    }

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
        //serverContext is not used by Log Replication.
    }

    // ================================================

    /**
     * When the SOURCE is the connection receiver, the only request it can receive is the leadership_Query.
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
        } else if (message.getPayload().getPayloadCase().equals(PayloadCase.LR_LEADERSHIP_QUERY)){
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
                String remoteLeaderId = message.getHeader().getSession().getSinkClusterId();
                runtimeFSM.setRemoteLeaderNodeId(remoteLeaderId);
            }
        } else {
            throw new UnsupportedOperationException("Received " + message.getPayload().getPayloadCase());
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
            log.trace("Incoming message with wrong epoch, got {}, expected {}", header.getEpoch(), serverEpoch);
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


    public void connectionDown() {
        log.debug("Caught Network Exception for session {}", session);
        runtimeFSM.input(new LogReplicationRuntimeEvent(LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.ON_CONNECTION_DOWN,
                session.getSinkClusterId()));
    }

}
