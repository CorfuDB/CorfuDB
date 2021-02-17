package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;
import java.util.Set;

import org.corfudb.runtime.proto.ServerErrors.ServerErrorMsg.ErrorCase;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;

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
     * Set the priority level of messages sent by this client
     * @param level
     */
    default void setPriorityLevel(PriorityLevel level) {
        //no-op
    }

    /**
     * Get the router used by the Netty client.
     */
    IClientRouter getRouter();

    /**
     * @return The Response handler used by the Netty Client.
     */
    default ClientResponseHandler getResponseHandler() {
        throw new UnsupportedOperationException("Response handler not provided, "
                + "please override handleMessage");
    }

    /**
     * Handle a incoming response on the channel.
     *
     * @param msg The incoming response.
     * @param ctx The channel handler context.
     */
    default void handleMessage(ResponseMsg msg, ChannelHandlerContext ctx) {
        getResponseHandler().handle(msg, ctx);
    }

    /**
     * Returns a set of payload cases that the client handles.
     *
     * @return The set of payload cases this client handles.
     */
    default Set<PayloadCase> getHandledCases() {
        return getResponseHandler().getHandledCases();
    }

    default Set<ErrorCase> getHandledErrors() {
        return getResponseHandler().getHandledErrors();
    }
}
