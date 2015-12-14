package org.corfudb.runtime.clients;

import com.google.common.collect.ImmutableSet;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.LayoutResponseMsg;
import org.corfudb.runtime.view.Layout;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Created by mwei on 12/9/15.
 */
public class LayoutClient implements IClient {

    @Setter
    IClientRouter router;

    /**
     * Handle a incoming message on the channel
     *
     * @param msg The incoming message
     * @param ctx The channel handler context
     */
    @Override
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx) {
        switch (msg.getMsgType())
        {
            case LAYOUT_RESPONSE:
                router.completeRequest(msg.getRequestID(), ((LayoutResponseMsg)msg).getLayout());
                break;
        }
    }

    /** The messages this client should handle. */
    @Getter
    public final Set<CorfuMsg.CorfuMsgType> HandledTypes =
            new ImmutableSet.Builder<CorfuMsg.CorfuMsgType>()
                    .add(CorfuMsg.CorfuMsgType.LAYOUT_REQUEST)
                    .add(CorfuMsg.CorfuMsgType.LAYOUT_RESPONSE)
                    .build();

    /**
     * Retrieves the layout from the endpoint, asynchronously.
     * @return A future which will be completed with the current layout.
     */
    public CompletableFuture<Layout> getLayout() {
        return router.sendMessageAndGetCompletable(
                new CorfuMsg(CorfuMsg.CorfuMsgType.LAYOUT_REQUEST));
    }

}
