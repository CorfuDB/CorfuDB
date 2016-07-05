package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;
import org.corfudb.protocols.wireprotocol.CorfuMsg;

import java.util.Set;

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
     * Handle a incoming message on the channel
     *
     * @param msg The incoming message
     * @param ctx The channel handler context
     * @param r   The router that routed the incoming message.
     */
    void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx);

    /**
     * Returns a set of message types that the client handles.
     *
     * @return The set of message types this client handles.
     */
    Set<CorfuMsg.CorfuMsgType> getHandledTypes();
}
