package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;


/**
 * The netty server router routes incoming messages to registered roles using
 * the
 * Created by mwei on 12/1/15.
 */
@Slf4j
@ChannelHandler.Sharable
public class NettyServerRouter extends ChannelInboundHandlerAdapter
        implements IServerRouter {


    /**
     * This map stores the mapping from message type to netty server handler.
     */
    private final Map<CorfuMsgType, AbstractServer> handlerMap;

    /**
     * The epoch of this router. This is managed by the base server implementation.
     */
    @Getter
    @Setter
    long serverEpoch;

    /** The {@link AbstractServer}s this {@link NettyServerRouter} routes messages for. */
    @Getter
    final List<AbstractServer> servers;

    /** Construct a new {@link NettyServerRouter}.
     *
     * @param servers   A list of {@link AbstractServer}s this router will route
     *                  messages for.
     */
    public NettyServerRouter(List<AbstractServer> servers) {
        this.servers = servers;
        handlerMap = new EnumMap<>(CorfuMsgType.class);
        servers.forEach(server -> server.getHandler().getHandledTypes()
            .forEach(x -> handlerMap.put(x, server)));
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

    /**
     * Send a netty message through this router, setting the fields in the outgoing message.
     *
     * @param ctx    Channel handler context to use.
     * @param inMsg  Incoming message to respond to.
     * @param outMsg Outgoing message.
     */
    public void sendResponse(ChannelHandlerContext ctx, CorfuMsg inMsg, CorfuMsg outMsg) {
        outMsg.copyBaseFields(inMsg);
        ctx.writeAndFlush(outMsg, ctx.voidPromise());
        log.trace("Sent response: {}", outMsg);
    }

    /**
     * Validate the epoch of a CorfuMsg, and send a WRONG_EPOCH response if
     * the server is in the wrong epoch. Ignored if the message type is reset (which
     * is valid in any epoch).
     *
     * @param msg The incoming message to validate.
     * @param ctx The context of the channel handler.
     * @return True, if the epoch is correct, but false otherwise.
     */
    public boolean validateEpoch(CorfuMsg msg, ChannelHandlerContext ctx) {
        long serverEpoch = getServerEpoch();
        if (!msg.getMsgType().ignoreEpoch && msg.getEpoch() != serverEpoch) {
            sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH,
                    serverEpoch));
            log.trace("Incoming message with wrong epoch, got {}, expected {}, message was: {}",
                    msg.getEpoch(), serverEpoch, msg);
            return false;
        }
        return true;
    }

    /**
     * Handle an incoming message read on the channel.
     *
     * @param ctx Channel handler context
     * @param msg The incoming message on that channel.
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            // The incoming message should have been transformed to a CorfuMsg earlier in the
            // pipeline.
            CorfuMsg m = ((CorfuMsg) msg);
            // We get the handler for this message from the map
            AbstractServer handler = handlerMap.get(m.getMsgType());
            if (handler == null) {
                // The message was unregistered, we are dropping it.
                log.warn("Received unregistered message {}, dropping", m);
            } else {
                if (validateEpoch(m, ctx)) {
                    // Route the message to the handler.
                    if (log.isTraceEnabled()) {
                        log.trace("Message routed to {}: {}", handler.getClass().getSimpleName(),
                                msg);
                    }

                    handler.getExecutor().submit(() -> {
                        try {
                            handler.handleMessage(m, ctx, this);
                        } catch (Throwable t) {
                            log.error("channelRead: Handling {} failed due to {}:{}",
                                    m != null ? m.getMsgType() : "UNKNOWN",
                                    t.getClass().getSimpleName(),
                                    t.getMessage(),
                                    t);
                        }
                    });
                }
            }
        } catch (Exception e) {
            log.error("Exception during read!", e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Error in handling inbound message, {}", cause);
        ctx.close();
    }

}
