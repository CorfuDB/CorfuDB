package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuSetEpochMsg;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * The netty server router routes incoming messages to registered roles using
 * the
 * Created by mwei on 12/1/15.
 */
@Slf4j
@ChannelHandler.Sharable
public class NettyServerRouter extends ChannelInboundHandlerAdapter
        implements IServerRouter {

    public static final String PREFIX_EPOCH = "SERVER_EPOCH";
    public static final String KEY_EPOCH = "CURRENT";

    /**
     * This map stores the mapping from message type to netty server handler.
     */
    Map<CorfuMsg.CorfuMsgType, AbstractServer> handlerMap;

    BaseServer baseServer;

    DataStore dataStore;

    /**
     * The epoch of this router. This is managed by the base server implementation.
     */
    @Override
    public long getServerEpoch() {
        Long epoch = dataStore.get(Long.class, PREFIX_EPOCH, KEY_EPOCH);
        return epoch == null ? 0 : epoch;
    }

    @Override
    public void setServerEpoch(long serverEpoch) {
        dataStore.put(Long.class, PREFIX_EPOCH, KEY_EPOCH, serverEpoch);
    }

    public NettyServerRouter(Map<String, Object> opts) {
        handlerMap = new ConcurrentHashMap<>();
        this.dataStore = new DataStore(opts);
        baseServer = new BaseServer();
        addServer(baseServer);
    }

    /**
     * Add a new netty server handler to the router.
     *
     * @param server The server to add.
     */
    public void addServer(AbstractServer server) {
        // Iterate through all types of CorfuMsgType, registering the handler
        Arrays.<CorfuMsg.CorfuMsgType>stream(CorfuMsg.CorfuMsgType.values())
                .forEach(x -> {
                    if (x.handler.isInstance(server)) {
                        handlerMap.put(x, server);
                        log.trace("Registered {} to handle messages of type {}", server, x);
                    }
                });
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
        outMsg.setEpoch(getServerEpoch());
        ctx.writeAndFlush(outMsg);
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
        if (!msg.getMsgType().ignoreEpoch && msg.getEpoch() != getServerEpoch()) {
            sendResponse(ctx, msg, new CorfuSetEpochMsg(CorfuMsg.CorfuMsgType.WRONG_EPOCH,
                    getServerEpoch()));
            log.trace("Incoming message with wrong epoch, got {}, expected {}, message was: {}",
                    msg.getEpoch(), getServerEpoch(), msg);
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
            // The incoming message should have been transformed to a CorfuMsg earlier in the pipeline.
            CorfuMsg m = ((CorfuMsg) msg);
            // We get the handler for this message from the map
            AbstractServer handler = handlerMap.get(m.getMsgType());
            if (handler == null) {
                // The message was unregistered, we are dropping it.
                log.warn("Received unregistered message {}, dropping", m);
            } else {
                if (validateEpoch(m, ctx)) {
                    // Route the message to the handler.
                    log.trace("Message routed to {}: {}", handler.getClass().getSimpleName(), msg);
                    handler.handleMessage(m, ctx, this);
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
