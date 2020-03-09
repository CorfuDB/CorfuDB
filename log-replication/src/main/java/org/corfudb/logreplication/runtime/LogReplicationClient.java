package org.corfudb.logreplication.runtime;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.runtime.clients.IClient;
import org.corfudb.runtime.clients.IClientRouter;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class LogReplicationClient implements IClient {

    @Getter
    @Setter
    private IClientRouter router;

    public LogReplicationClient(IClientRouter router) {
        setRouter(router);
    }

    public CompletableFuture<Boolean> ping() {
        System.out.println("Ping!!!!!! ");
        return getRouter().sendMessageAndGetCompletable(
                new CorfuMsg(CorfuMsgType.PING).setEpoch(0));
    }

    @Override
    public void setRouter(IClientRouter router) {
        this.router = router;
    }

    @Override
    public IClientRouter getRouter() {
        return router;
    }
}
