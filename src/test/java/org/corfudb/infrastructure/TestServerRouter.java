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

    AbstractServer serverUnderTest;

    void setServerUnderTest(AbstractServer server) {
        serverUnderTest = server;
    }

    AtomicLong requestCounter;

    @Getter
    @Setter
    long serverEpoch;

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
        outMsg.setEpoch(serverEpoch);
        this.responseMessages.add(outMsg);
    }

    public void sendServerMessage(CorfuMsg msg)
    {
        msg.setEpoch(serverEpoch);
        msg.setRequestID(requestCounter.getAndIncrement());
        serverUnderTest.handleMessage(msg, null, this);
    }
}
