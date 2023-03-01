package org.corfudb.infrastructure.logreplication.runtime;

import com.google.common.collect.ImmutableList;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.BaseServer;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.infrastructure.logreplication.transport.server.IServerChannelAdapter;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.view.Layout;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This class represents the Corfu interface to route incoming messages from external adapters when
 * custom communication channels are used.
 *
 * Created by annym on 14/5/20.
 */
@Slf4j
public class LogReplicationSinkServerRouter implements IServerRouter {

    /**
     * Server transport adapter
     */
    @Getter
    private IServerChannelAdapter serverAdapter;

    /**
     * This map stores the mapping from message type to netty server handler.
     */
    @Getter
    private final Map<RequestPayloadMsg.PayloadCase, AbstractServer> handlerMap;

    /**
     * The epoch of this router. This is managed by the base server implementation.
     */
    @Getter
    @Setter
    private volatile long serverEpoch;

    /** The {@link AbstractServer}s this {@link LogReplicationSinkServerRouter} routes messages for. */
    final List<AbstractServer> servers;


    /** Construct a new {@link LogReplicationSinkServerRouter}.
     *
     * @param serverMap   A map of {@link AbstractServer}s this router will route
     *                  messages for.
     */
    public LogReplicationSinkServerRouter(Map<Class, AbstractServer> serverMap) {
        this.serverEpoch = ((BaseServer) serverMap.get(BaseServer.class)).serverContext.getServerEpoch();
        this.servers = ImmutableList.copyOf(serverMap.values());
        this.handlerMap = new EnumMap<>(RequestPayloadMsg.PayloadCase.class);

        servers.forEach(server -> {
            try {
                server.getHandlerMethods().getHandledTypes().forEach(x -> handlerMap.put(x, server));
            } catch (UnsupportedOperationException ex) {
                log.trace("No registered CorfuMsg handler for server {}", server, ex);
            }
        });
    }

    public void setAdapter(IServerChannelAdapter serverAdapter) {
        this.serverAdapter = serverAdapter;
    }

    // ============ IServerRouter Methods =============

    @Override
    public void sendResponse(ResponseMsg response, ChannelHandlerContext ctx) {
        log.info("In SinkServerRouter Ready to send response {}", response.getPayload().getPayloadCase());
        try {
                serverAdapter.send(response);
            log.trace("Sent response: {}", response);
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
        return servers;
    }

    @Override
    public void setServerContext(ServerContext serverContext) {

    }

    // ================================================

    /**
     * Receive messages from the 'custom' serverAdapter implementation. This message will be forwarded
     * for processing.
     *
     * @param message
     */
    public void receive(RequestMsg message) {
        log.debug("Received message {}", message.getPayload().getPayloadCase());

        AbstractServer handler = handlerMap.get(message.getPayload().getPayloadCase());
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
                    handler.handleMessage(message, null,  this);
                } catch (Throwable t) {
                    log.error("channelRead: Handling {} failed due to {}:{}",
                            message.getPayload().getPayloadCase(),
                            t.getClass().getSimpleName(),
                            t.getMessage(),
                            t);
                }
            }
        }
    }

    public void receive(ResponseMsg message) {
        // no op.
    }

    /**
     * Validate the epoch of a CorfuMsg, and send a WRONG_EPOCH response if
     * the server is in the wrong epoch. Ignored if the message type is reset (which
     * is valid in any epoch).
     *
     * @param header The incoming header to validate.
     * @return True, if the epoch is correct, but false otherwise.
     */
    protected boolean validateEpoch(HeaderMsg header) {
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
