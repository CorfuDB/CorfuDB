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
     *
     * @param client The client to add to the router
     * @return This IClientProtobufRouter
     */
    IClientProtobufRouter addClient(IClient client);

    /**
     * Send a request message and get a completable future to be fulfilled by the reply.
     *
     * @param request The request message to send.
     * @param ctx Context of the channel handler.
     * @param <T> The type of completable to return.
     * @return A completable future which will be fulfilled by the reply,
     * or a timeout in the case there is no response.
     */
    <T> CompletableFuture<T> sendRequestAndGetCompletable(Request request, ChannelHandlerContext ctx);

    /**
     * Send a one way message, without adding a completable future.
     *
     * @param request The request message to send.
     * @param ctx Context of the channel handler.
     */
    void sendRequest(Request request, ChannelHandlerContext ctx);

    /**
     * Complete a given outstanding request with a completion value.
     *
     * @param requestID  The request to complete.
     * @param completion The value to complete the request with.
     * @param <T>        The type of the completion.
     */
    <T> void completeRequest(long requestID, T completion);

    /**
     * Exceptionally complete a request with a given cause.
     *
     * @param requestID The request to complete.
     * @param cause     The cause to give for the exceptional completion.
     */
    void completeExceptionally(long requestID, Throwable cause);

    /**
     * Stops routing requests.
     */
    void stop();

    /**
     * Get the host that this router is routing requests for.
     */
    String getHost();

    /**
     * Get the port that this router is routing requests for.
     */
    Integer getPort();

    /**
     * Set the Connect timeout
     *
     * @param timeoutConnect timeout for connection in milliseconds.
     */
    void setTimeoutConnect(long timeoutConnect);

    /**
     * Set the retry timeout
     *
     * @param timeoutRetry timeout to make a retry in milliseconds.
     */
    void setTimeoutRetry(long timeoutRetry);

    /**
     * Set the Response timeout
     *
     * @param timeoutResponse Response timeout in milliseconds.
     */
    void setTimeoutResponse(long timeoutResponse);

}
