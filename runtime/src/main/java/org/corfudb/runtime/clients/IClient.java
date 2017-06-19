package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;

import java.util.Set;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;

/**
 * This is an interface which all clients to a ClientRouter must implement.
 * Created by mwei on 12/8/15.
 */
public interface IClient {

    /**
     * Set the router used by the Netty client.
     *
     * @param router The router to be used by the Netty client.
     */
    void setRouter(IClientRouter router);

    /**
     * Get the router used by the Netty client.
     */
    IClientRouter getRouter();

    default ClientMsgHandler getMsgHandler() {
        throw new UnsupportedOperationException("Message handler not provided, "
                + "please override handleMessage!");
    }

    /**
     * Handle a incoming message on the channel.
     *
     * @param msg The incoming message
     * @param ctx The channel handler context
     */
    default void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx) {
        getMsgHandler().handle(msg, ctx);
    }

    /**
     * Returns a set of message types that the client handles.
     *
     * @return The set of message types this client handles.
     */
    default Set<CorfuMsgType> getHandledTypes() {
        return getMsgHandler().getHandledTypes();
    }
}
