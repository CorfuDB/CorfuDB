package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class LogReplicationServer extends AbstractServer {

    final ServerContext serverContext;

    private final ExecutorService executor;

    @Getter
    private final HandlerMethods handler = HandlerMethods.generateHandler(MethodHandles.lookup(), this);

    @Override
    public boolean isServerReadyToHandleMsg(CorfuMsg msg) {
        return getState() == ServerState.READY;
    }

    public LogReplicationServer(@Nonnull ServerContext context) {
        this.serverContext = context;
        executor = Executors.newFixedThreadPool(1,
                new ServerThreadFactory("LogReplicationServer-", new ServerThreadFactory.ExceptionHandler()));
    }

    @Override
    protected void processRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        executor.submit(() -> getHandler().handle(msg, ctx, r));
    }

    @Override
    public void shutdown() {
        super.shutdown();
        executor.shutdown();
    }

    /**
     * Respond to a ping message.
     *
     * @param msg   The incoming message
     * @param ctx   The channel context
     * @param r     The server router.
     */
    @ServerHandler(type = CorfuMsgType.PING)
    private void ping(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.info("PING received by Replication Server");
        r.sendResponse(ctx, msg, CorfuMsgType.PONG.msg());
    }
}
