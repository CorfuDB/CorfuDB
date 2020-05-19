package org.corfudb.logreplication.runtime;

import io.netty.channel.ChannelHandlerContext;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.PriorityLevel;
import org.corfudb.runtime.clients.ClientMsgHandler;
import org.corfudb.runtime.clients.IClient;
import org.corfudb.runtime.clients.IClientRouter;

import java.util.Set;

public class ProtoLogReplicationHandler implements IClient {

    @Override
    public void setRouter(IClientRouter router) {

    }

    @Override
    public void setPriorityLevel(PriorityLevel level) {

    }

    @Override
    public IClientRouter getRouter() {
        return null;
    }

    @Override
    public ClientMsgHandler getMsgHandler() {
        return null;
    }

    @Override
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx) {

    }

    @Override
    public Set<CorfuMsgType> getHandledTypes() {
        return null;
    }
}
