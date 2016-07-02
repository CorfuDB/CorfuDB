package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;

/**
 * Created by mwei on 12/4/15.
 */
public abstract class AbstractServer {

    @Getter
    @Setter
    boolean shutdown;

    public AbstractServer() {
        shutdown = false;
    }

    /**
     * Handle a incoming Netty message.
     *
     * @param msg An incoming message.
     * @param ctx The channel handler context.
     * @param r   The router that took in the message.
     */
    public abstract void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r);

    /**
     * Reset the server.
     */
    public abstract void reset();

    /**
     * Shutdown the server.
     */
    public void shutdown() {
        setShutdown(true);
    }

}
