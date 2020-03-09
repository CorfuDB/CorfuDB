package org.corfudb.logreplication.runtime;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.runtime.clients.ClientHandler;
import org.corfudb.runtime.clients.ClientMsgHandler;
import org.corfudb.runtime.clients.IClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.IHandler;
import org.corfudb.runtime.clients.LayoutClient;

import java.lang.invoke.MethodHandles;


/**
 * A client to the Log Replication Server
 */
@Slf4j
public class LogReplicationHandler implements IClient, IHandler<LayoutClient> {

    @Setter
    @Getter
    IClientRouter router;

    @Getter
    public ClientMsgHandler msgHandler = new ClientMsgHandler(this)
            .generateHandlers(MethodHandles.lookup(), this);

    /**
     * Handle a pong response from the server.
     *
     * @param msg The ping request message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return Always True, since the ping message was successful.
     */
    @ClientHandler(type = CorfuMsgType.PONG)
    private static Object handlePong(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        log.info("PONG");
        return true;
    }

    /**
     * Handle a ping request from the server.
     *
     * @param msg The ping request message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     * @return The return value, null since this is a message from the server.
     */
    @ClientHandler(type = CorfuMsgType.PING)
    private static Object handlePing(CorfuMsg msg, ChannelHandlerContext ctx, IClientRouter r) {
        log.info("PING");
        System.out.println("Ping received by Server!!!!!");
        r.sendResponseToServer(ctx, msg, new CorfuMsg(CorfuMsgType.PONG));
        return null;
    }

    @Override
    public LayoutClient getClient(long epoch) {
        return null;
    }


}
