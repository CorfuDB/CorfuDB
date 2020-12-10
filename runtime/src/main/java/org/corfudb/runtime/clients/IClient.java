package org.corfudb.runtime.clients;

import io.netty.channel.ChannelHandlerContext;
import java.util.Set;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.PriorityLevel;
import org.corfudb.runtime.proto.ServerErrors.ServerErrorMsg.ErrorCase;
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
     * @deprecated [RM]
     * @return The response msg handler used by the client.
     */
    @Deprecated
    default ClientMsgHandler getMsgHandler() {
        throw new UnsupportedOperationException("Message handler not provided, "
                + "please override handleMessage!");
    }

    /**
     * For old CorfuMsg, use {@link #getMsgHandler()}
     *
     * @return The Response handler used by the Netty Client.
     */
    default ClientResponseHandler getResponseHandler() {
        throw new UnsupportedOperationException("Response handler not provided, "
                + "please override handleMessage");
    }

    /**
     * @deprecated [RM]
     * Handle a incoming message on the channel.
     * For protobuf, use {@link #handleMessage(ResponseMsg, ChannelHandlerContext)}
     *
     * @param msg The incoming message
     * @param ctx The channel handler context
     */
    @Deprecated
    default void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx) {
        getMsgHandler().handle(msg, ctx);
    }

    /**
     * Handle a incoming response on the channel.
     * For old CorfuMsg, use {@link #handleMessage(CorfuMsg, ChannelHandlerContext)}
     *
     * @param msg The incoming response.
     * @param ctx The channel handler context.
     */
    default void handleMessage(ResponseMsg msg, ChannelHandlerContext ctx) {
        getResponseHandler().handle(msg, ctx);
    }

    /**
     * @deprecated [RM]
     * Returns a set of message types that the client handles.
     * For protobuf, use {@link #getHandledCases()}
     *
     * @return The set of message types this client handles.
     */
    @Deprecated
    default Set<CorfuMsgType> getHandledTypes() {
        return getMsgHandler().getHandledTypes();
    }

    /**
     * Returns a set of payload cases that the client handles.
     * For old CorfuMsg, use {@link #getHandledTypes()}
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
