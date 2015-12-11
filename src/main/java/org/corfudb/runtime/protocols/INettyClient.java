package org.corfudb.runtime.protocols;

import io.netty.channel.ChannelHandlerContext;
import org.corfudb.infrastructure.NettyServerRouter;
import org.corfudb.infrastructure.wireprotocol.NettyCorfuMsg;

import java.util.Set;

/**
 * Created by mwei on 12/8/15.
 */
public interface INettyClient {

    /** Set the router used by the Netty client.
     *
     * @param router    The router to be used by the Netty client.
     */
    void setRouter(NettyClientRouter router);

    /**
     * Handle a incoming message on the channel
     * @param msg   The incoming message
     * @param ctx   The channel handler context
     * @param r     The router that routed the incoming message.
     */
    void handleMessage(NettyCorfuMsg msg, ChannelHandlerContext ctx);

    /**
     * Returns a set of message types that the client handles.
     * @return  The set of message types this client handles.
     */
    Set<NettyCorfuMsg.NettyCorfuMsgType> getHandledTypes();
}
