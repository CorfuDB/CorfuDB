package org.corfudb.infrastructure;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.runtime.clients.TestChannelContext;
import org.corfudb.runtime.clients.TestRule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mwei on 12/13/15.
 */
@Slf4j
public class TestServerRouter implements IServerRouter {

    @Getter
    public List<CorfuMsg> responseMessages;

    @Getter
    public Map<CorfuMsgType, AbstractServer> handlerMap;

    @Getter
    public List<AbstractServer> servers;

    public List<TestRule> rules;

    AtomicLong requestCounter;

    @Getter
    long serverEpoch;

    @Getter
    int port = 0;

    public TestServerRouter() {
        reset();
    }

    public TestServerRouter(int port) {
        reset();
        this.port = port;
    }

    public void reset() {
        this.responseMessages = new ArrayList<>();
        this.requestCounter = new AtomicLong();
        this.servers = new ArrayList<>();
        this.handlerMap = new ConcurrentHashMap<>();
        this.rules = new ArrayList<>();
    }

    @Override
    public void sendResponse(ChannelHandlerContext ctx, CorfuMsg inMsg, CorfuMsg outMsg) {
        outMsg.copyBaseFields(inMsg);
        outMsg.setEpoch(getServerEpoch());
        if (rules.stream()
                .map(x -> x.evaluate(outMsg, this))
                .allMatch(x -> x)) {
            if (ctx != null && ctx instanceof TestChannelContext) {
                ctx.writeAndFlush(outMsg);
            } else {
                this.responseMessages.add(outMsg);
            }
        }
    }

    /**
     * Register a server to route messages to
     *
     * @param server The server to route messages to
     */
    @Override
    public void addServer(AbstractServer server) {
        servers.add(server);
        server.getHandler()
                .getHandledTypes().forEach(x -> {
            handlerMap.put(x, server);
            log.trace("Registered {} to handle messages of type {}", server, x);
        });
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
        if (!msg.getMsgType().ignoreEpoch && msg.getEpoch() != serverEpoch) {
            sendResponse(ctx, msg, new CorfuPayloadMsg<>(CorfuMsgType.WRONG_EPOCH,
                    getServerEpoch()));
            log.trace("Incoming message with wrong epoch, got {}, expected {}, message was: {}",
                    msg.getEpoch(), serverEpoch, msg);
            return false;
        }
        return true;
    }

    public void sendServerMessage(CorfuMsg msg) {
        sendServerMessage(msg, null);
    }

    public void sendServerMessage(CorfuMsg msg, ChannelHandlerContext ctx) {
        AbstractServer as = handlerMap.get(msg.getMsgType());
        if (validateEpoch(msg, ctx)) {
            if (as != null) {
                // refactor and move threading to handler
                as.handleMessage(msg, ctx, this);
            }
            else {
                log.trace("Unregistered message of type {} sent to router", msg.getMsgType());
            }
        } else {
            log.trace("Message with wrong epoch {}, expected {}", msg.getEpoch(), serverEpoch);
        }
    }

    public void setServerEpoch(long serverEpoch) {
        this.serverEpoch = serverEpoch;
        getServers().forEach(s -> s.sealServerWithEpoch(serverEpoch));
    }
}
