package org.corfudb.infrastructure;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.runtime.clients.TestChannelContext;
import org.corfudb.runtime.clients.TestRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 12/13/15.
 */
@Slf4j
public class TestServerRouter implements IServerRouter {

    @Getter
    public List<CorfuMsg> responseMessages;

    @Getter
    public Map<CorfuMsg.CorfuMsgType, AbstractServer> handlerMap;

    public List<TestRule> rules;

    AtomicLong requestCounter;

    @Getter
    @Setter
    long serverEpoch;

    public TestServerRouter() {
        reset();
    }

    public void reset() {
        this.responseMessages = new ArrayList<>();
        this.requestCounter = new AtomicLong();
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
        // Iterate through all types of CorfuMsgType, registering the handler
        Arrays.<CorfuMsg.CorfuMsgType>stream(CorfuMsg.CorfuMsgType.values())
                .forEach(x -> {
                    if (x.handler.isInstance(server)) {
                        handlerMap.put(x, server);
                        log.trace("Registered {} to handle messages of type {}", server, x);
                    }
                });
    }


    public void sendServerMessage(CorfuMsg msg) {
        AbstractServer as = handlerMap.get(msg.getMsgType());
        if (as != null) {
            as.handleMessage(msg, null, this);
        }
        else {
            log.trace("Unregistered message of type {} sent to router", msg.getMsgType());
        }
    }

    public void sendServerMessage(CorfuMsg msg, ChannelHandlerContext ctx) {
        AbstractServer as = handlerMap.get(msg.getMsgType());
        if (as != null) {
            as.handleMessage(msg, ctx, this);
        }
        else {
            log.trace("Unregistered message of type {} sent to router", msg.getMsgType());
        }
    }

    /**
     * This simulates the serialization and deserialization that happens in the Netty pipeline for all messages
     * from server to client.
     *
     * @param message
     * @return
     */

    public CorfuMsg simulateSerialization(CorfuMsg message) {
        /* simulate serialization/deserialization */
        ByteBuf oBuf = ByteBufAllocator.DEFAULT.buffer();
        //Class<? extends CorfuMsg> type = message.getMsgType().messageType;
        //extra assert needed to simulate real Netty behavior
        //assertThat(message.getClass().getSimpleName()).isEqualTo(type.getSimpleName());
        //type.cast(message).serialize(oBuf);
        message.serialize(oBuf);
        oBuf.resetReaderIndex();
        CorfuMsg msgOut = CorfuMsg.deserialize(oBuf);
        oBuf.release();
        return msgOut;
    }

}
