package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mwei on 12/13/15.
 */
public class TestServerRouter implements IServerRouter {

    @Setter
    IServer serverUnderTest;
    AtomicLong requestCounter;

    @Getter
    @Setter
    long epoch;

    @Getter
    public List<CorfuMsg> responseMessages;

    public TestServerRouter()
    {
        reset();
    }

    public void reset()
    {
        this.responseMessages = new ArrayList<>();
        this.requestCounter = new AtomicLong();
    }

    @Override
    public void sendResponse(ChannelHandlerContext ctx, CorfuMsg inMsg, CorfuMsg outMsg) {
        this.responseMessages.add(outMsg);
    }

    public void sendServerMessage(CorfuMsg msg)
    {
        msg.setRequestID(requestCounter.getAndIncrement());
        serverUnderTest.handleMessage(msg, null, this);
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
