package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.RouterRuleMsg;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


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

    ExecutorService handlerWorkers = Executors.newWorkStealingPool(Runtime.getRuntime().availableProcessors());

    /**
     * This map stores the mapping from message type to netty server handler.
     */
    Map<CorfuMsgType, AbstractServer> handlerMap;

    BaseServer baseServer;

    Set<CorfuMsgType> corfuMsgTypeRuleSet = new HashSet<>();
    Double dropProbability = 0.0;

    /**
     * The epoch of this router. This is managed by the base server implementation.
     */
    @Getter
    @Setter
    long serverEpoch;

    public NettyServerRouter(Map<String, Object> opts) {
        handlerMap = new ConcurrentHashMap<>();
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
        server.getHandler().getHandledTypes()
                .forEach(x -> {
                    handlerMap.put(x, server);
                    log.trace("Registered {} to handle messages of type {}", server, x);
                });
    }

    public void removeServer(AbstractServer server) {
        // Iterate through all types of CorfuMsgType, un-registering the handler
        server.getHandler().getHandledTypes()
                .forEach(x -> {
                    handlerMap.remove(x, server);
                    log.trace("Un-Registered {} to handle messages of type {}", server, x);
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

    public void setRouterRule(RouterRuleMsg routerRule) {
        if (routerRule.getClearAllRules()) {
            corfuMsgTypeRuleSet.clear();
        }
        corfuMsgTypeRuleSet.addAll(routerRule.getCorfuMsgTypeSet());
        dropProbability = routerRule.getDropProbability();
        log.info("New rule set on server router : {}", routerRule.toString());
    }

    public boolean applyRule(CorfuMsg msg) {
        if (corfuMsgTypeRuleSet.contains(msg.getMsgType())) {
            return new Random().nextDouble() <= dropProbability;
        }
        return false;
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
            if (m.getMsgType().equals(CorfuMsgType.ROUTER_RULE)) {
                setRouterRule(((CorfuPayloadMsg<RouterRuleMsg>) m).getPayload());
                sendResponse(ctx, m, new CorfuMsg(CorfuMsgType.ACK));
                return;
            }
            if (applyRule(m)) {
                log.warn("Dropping message : {} as chosen by rule.", m.toString());
                return;
            }

            // We get the handler for this message from the map
            AbstractServer handler = handlerMap.get(m.getMsgType());
            if (handler == null) {
                // The message was unregistered, we are dropping it.
                log.warn("Received unregistered message {}, dropping", m);
            } else {
                if (validateEpoch(m, ctx)) {
                    // Route the message to the handler.
                    log.trace("Message routed to {}: {}", handler.getClass().getSimpleName(), msg);
                    handlerWorkers.submit(() -> handler.handleMessage(m, ctx, this));
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
