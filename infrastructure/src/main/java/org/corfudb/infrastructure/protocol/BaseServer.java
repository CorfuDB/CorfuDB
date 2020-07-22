package org.corfudb.infrastructure.protocol;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.protocol.proto.CorfuProtocol;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerThreadFactory;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;
import org.corfudb.common.protocol.proto.CorfuProtocol.MessageType;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class BaseServer extends AbstractServer {

    final ServerContext serverContext;
    private final ExecutorService executor;

    /** HandlerMethod for the base server. */
    @Getter
    private final RequestHandlerMethods handler = RequestHandlerMethods.generateHandler(MethodHandles.lookup(), this);

    @Override
    public boolean isServerReadyToHandleReq(Request req) {
        return getState() == ServerState.READY;
    }

    public BaseServer(@Nonnull ServerContext context) {
        this.serverContext = context;
        executor = Executors.newFixedThreadPool(serverContext.getBaseServerThreadCount(),
                new ServerThreadFactory("baseServer-", new ServerThreadFactory.ExceptionHandler()));
    }

    @Override
    protected void processRequest(Request req, ChannelHandlerContext ctx, IServerRouter r) {
        executor.submit(() -> getHandler().handle(req, ctx, r));
    }

    @Override
    public void shutdown() {
        super.shutdown();
        executor.shutdown();
    }

    /**
     * Respond to a ping message.
     *
     * @param req   The incoming request message.
     * @param ctx   The channel context.
     * @param r     The server router.
     */
    @AnnotatedServerHandler(type = MessageType.PING)
    private void handlePing(Request req, ChannelHandlerContext ctx, IServerRouter r) {
        //TODO(Zach): Handle request and send response
        //TODO: checkArgument(req.hasPingRequest());
        log.info("Ping message received from {} {}", req.getHeader().getClientId().getMsb(),
                req.getHeader().getClientId().getLsb());
    }

    /**
     * Restart the JVM. This mechanism leverages that corfu_server runs in a bash script
     * which monitors the exit code of Corfu. If the exit code is 200, then it restarts
     * the server.
     *
     * @param req   The incoming request message.
     * @param ctx   The channel context.
     * @param r     The server router.
     */
    @AnnotatedServerHandler(type = MessageType.RESTART)
    private void handleRestart(Request req, ChannelHandlerContext ctx, IServerRouter r) {
        //TODO(Zach): Handle request and send response
        //TODO: checkArgument(req.hasRestartRequest());
    }
}
