package org.corfudb.runtime.clients;

import java.util.concurrent.CompletableFuture;

import io.netty.channel.ChannelHandlerContext;
import org.corfudb.protocols.wireprotocol.orchestrator.Request;
import org.corfudb.runtime.protocol.proto.CorfuProtocol.Request;


/**
 *
 */
public interface IClientProtobufRouter {

    /**
     * Add a new client to the router
     * TODO: implementation of this method
     * @param client The client to add to the router
     * @return This IClientProtobufRouter
     */
    IClientProtobufRouter addClient(IClient client);

    /**
     *
     * @param request
     * @param ctx
     * @param <T>
     * @return
     */
    <T> CompletableFuture<T> sendRequestAndGetCompletable(Request request, ChannelHandlerContext ctx);

    /**
     *
     * @param request
     * @param ctx
     */
    void sendRequest(Request request, ChannelHandlerContext ctx);

    /**
     *
     * @param requestID
     * @param completion
     * @param <T>
     */
    <T> void completeRequest(long requestID, T completion);

    /**
     *
     * @param requestID
     * @param cause
     */
    void completeExceptionally(long requestID, Throwable cause);

    /**
     *
     */
    void stop();

    /**
     *
     * @return
     */
    String getHost();

    /**
     *
     * @return
     */
    Integer getPort();

    /**
     *
     * @param timeoutConnect
     */
    void setTimeoutConnect(long timeoutConnect);

    /**
     *
     * @param timeoutRetry
     */
    void setTimeoutRetry(long timeoutRetry);

    /**
     *
     * @param timeoutResponse
     */
    void setTimeoutResponse(long timeoutResponse);

}
