package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import org.corfudb.runtime.wireprotocol.NettyCorfuMsg;

/**
 * Created by mwei on 12/4/15.
 */
public interface INettyServer {

    /** Handle a incoming Netty message.
     *
     * @param msg   An incoming message.
     * @param ctx   The channel handler context.
     * @param r     The router that took in the message.
     */
    void handleMessage(NettyCorfuMsg msg, ChannelHandlerContext ctx, NettyServerRouter r);

    /** Reset the server.
     *
     */
    void reset();
}
