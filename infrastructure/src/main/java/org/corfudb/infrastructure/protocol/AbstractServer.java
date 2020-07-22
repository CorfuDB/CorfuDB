package org.corfudb.infrastructure.protocol;

import io.netty.channel.ChannelHandlerContext;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.protocol.proto.CorfuProtocol.Request;

import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public abstract class AbstractServer {

    /**
     * Current server state
     */
    private final AtomicReference<ServerState> state = new AtomicReference<>(ServerState.READY);

    /**
     * Get the message handler for this instance.
     *
     * @return A message handler.
     */
    public abstract RequestHandlerMethods getHandler();

    /**
     * Seal the server with the epoch.
     *
     * @param epoch Epoch to seal with
     */
    public void sealServerWithEpoch(long epoch) {
        // Overridden in log unit to flush operations stamped with an old epoch
    }

    public abstract boolean isServerReadyToHandleReq(Request req);

    /**
     * A stub that handlers can override to manage their threading, otherwise
     * the requests will be executed on the IO threads
     * @param req An incoming request message.
     * @param ctx The channel handler context.
     * @param r The router that took in the message.
     */
    protected void processRequest(Request req, ChannelHandlerContext ctx, IServerRouter r) {
        getHandler().handle(req, ctx, r);
    }

    /**
     * Handle a incoming Netty message.
     *
     * @param req An incoming request message.
     * @param ctx The channel handler context.
     * @param r   The router that took in the message.
     */
    public final void handleRequest(Request req, ChannelHandlerContext ctx, IServerRouter r) {
        if (getState() == ServerState.SHUTDOWN) {
            log.warn("Server received {} but is already shutdown.", req.getHeader().getType().toString());
            return;
        }

        //TODO(Zach): Check if server is ready to handle request messages and handle appropriately.

        processRequest(req, ctx, r);
    }

    protected void setState(ServerState newState) {
        state.updateAndGet(currState -> {
            if (currState == ServerState.SHUTDOWN && newState != ServerState.SHUTDOWN) {
                throw new IllegalStateException("Server is in SHUTDOWN state. Can't be changed");
            }

            return newState;
        });
    }

    public ServerState getState() {
        return state.get();
    }

    /**
     * Shutdown the server.
     */
    public void shutdown() {
        setState(ServerState.SHUTDOWN);
    }

    /**
     * The server state.
     * Represents server in a particular state: READY, NOT_READY, SHUTDOWN.
     */
    public enum ServerState {
        //TODO(Zach): What about NOT_READY?
        READY, SHUTDOWN
    }
}
