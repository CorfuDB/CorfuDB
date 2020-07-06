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
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.LogReplicationPluginConfig;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.Layout;
import org.corfudb.infrastructure.logreplication.transport.server.IServerChannelAdapter;
import org.corfudb.infrastructure.logreplication.utils.CorfuMessageConverter;
import org.corfudb.utils.common.CorfuMessageProtoBufException;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
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
public class LogReplicationServerRouter implements IServerRouter {

    @Getter
    private IServerChannelAdapter serverAdapter;

    /**
     * This map stores the mapping from message type to netty server handler.
     */
    private final Map<CorfuMsgType, AbstractServer> handlerMap;

    /**
     * The epoch of this router. This is managed by the base server implementation.
     */
    @Getter
    @Setter
    volatile long serverEpoch;

    /** The {@link AbstractServer}s this {@link LogReplicationServerRouter} routes messages for. */
    final List<AbstractServer> servers;

    /** Construct a new {@link LogReplicationServerRouter}.
     *
     * @param servers   A list of {@link AbstractServer}s this router will route
     *                  messages for.
     */
    public LogReplicationServerRouter(List<AbstractServer> servers) {
        this.serverEpoch = ((BaseServer) servers.get(0)).serverContext.getServerEpoch();
        this.servers = ImmutableList.copyOf(servers);
        this.handlerMap = new EnumMap<>(CorfuMsgType.class);
        servers.forEach(server -> server.getHandler().getHandledTypes()
                .forEach(x -> handlerMap.put(x, server)));
        this.serverAdapter = getAdapter(((BaseServer) servers.get(0)).serverContext);
    }

    private IServerChannelAdapter getAdapter(ServerContext serverContext) {

        LogReplicationPluginConfig config = new LogReplicationPluginConfig(serverContext.getPluginConfigFilePath());
        File jar = new File(config.getTransportAdapterJARPath());

        try (URLClassLoader child = new URLClassLoader(new URL[]{jar.toURI().toURL()}, this.getClass().getClassLoader())) {
            Class adapter = Class.forName(config.getTransportServerClassCanonicalName(), true, child);
            return (IServerChannelAdapter) adapter.getDeclaredConstructor(ServerContext.class, LogReplicationServerRouter.class).newInstance(serverContext, this);
        } catch (Exception e) {
            log.error("Fatal error: Failed to create serverAdapter", e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    // ============ IServerRouter Methods =============

    @Override
    public void sendResponse(CorfuMsg inMsg, CorfuMsg outMsg) {
        log.info("Ready to send response {}", outMsg.getMsgType());
        outMsg.copyBaseFields(inMsg);
        try {
            serverAdapter.send(CorfuMessageConverter.toProtoBuf(outMsg));
            log.info("Sent response: {}", outMsg);
        } catch (IllegalArgumentException e) {
            log.warn("Illegal response type. Ignoring message.", e);
        }
    }

    @Override
    public void sendResponse(ChannelHandlerContext ctx, CorfuMsg inMsg, CorfuMsg outMsg) {
        // This is specific to Netty implementation, which for the Log Replication Server
        // is implemented as one of many transport plugins. This is here as we inherit the
        // infrastructure design from CorfuServer but should be removed in the future.
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
     * @param protoMessage
     */
    public void receive(CorfuMessage protoMessage) {
        CorfuMsg corfuMsg;
        try {
            log.info("Received message {}", protoMessage.getType().name());
            // Transform protoBuf into CorfuMessage
            corfuMsg = CorfuMessageConverter.fromProtoBuf(protoMessage);
        } catch (CorfuMessageProtoBufException e) {
            log.error("Exception while trying to convert {} from protoBuf", protoMessage.getType(), e);
            return;
        }

        AbstractServer handler = handlerMap.get(corfuMsg.getMsgType());
        if (handler == null) {
            // The message was unregistered, we are dropping it.
            log.warn("Received unregistered message {}, dropping", corfuMsg);
        } else {
            if (validateEpoch(corfuMsg)) {
                // Route the message to the handler.
                if (log.isTraceEnabled()) {
                    log.trace("Message routed to {}: {}", handler.getClass().getSimpleName(), corfuMsg);
                }

                try {
                    handler.handleMessage(corfuMsg, this);
                } catch (Throwable t) {
                    log.error("channelRead: Handling {} failed due to {}:{}",
                            corfuMsg != null ? corfuMsg.getMsgType() : "UNKNOWN",
                            t.getClass().getSimpleName(),
                            t.getMessage(),
                            t);
                }
            }
        }
    }

    /**
     * Validate the epoch of a CorfuMsg, and send a WRONG_EPOCH response if
     * the server is in the wrong epoch. Ignored if the message type is reset (which
     * is valid in any epoch).
     *
     * @param msg The incoming message to validate.
     * @return True, if the epoch is correct, but false otherwise.
     */
    private boolean validateEpoch(CorfuMsg msg) {
        long serverEpoch = getServerEpoch();
        if (!msg.getMsgType().ignoreEpoch && msg.getEpoch() != serverEpoch) {
            log.trace("Incoming message with wrong epoch, got {}, expected {}, message was: {}",
                    msg.getEpoch(), serverEpoch, msg);
            sendResponse(null, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH,
                    serverEpoch));
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
