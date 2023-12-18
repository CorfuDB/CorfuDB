package org.corfudb.runtime.clients;

import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import java.util.concurrent.CompletableFuture;

import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;

import javax.annotation.Nonnull;

/**
 * This is an interface in which all client routers must implement.
 * Client routers are classes which talk to server routers. Clients are registered
 * on client routers using the addClient() interface, and can be retrieved using the
 * getClient() interface.
 *
 * <p>Created by mwei on 12/13/15.
 */
public interface IClientRouter {

    /**
     * Add a new client to the router.
     *
     * @param client The client to add to the router.
     * @return This IClientRouter, to support chaining and the builder pattern.
     */
    IClientRouter addClient(IClient client);

    /**
     * Send a request message and get a completable future to be fulfilled by the reply.
     *
     * @param payload
     * @param epoch
     * @param clusterId
     * @param priority
     * @param ignoreClusterId
     * @param ignoreEpoch
     * @param <T> The type of completable to return.
     * @return A completable future which will be fulfilled by the reply,
     * or a timeout in the case there is no response.
     */
    <T> CompletableFuture<T> sendRequestAndGetCompletable(CorfuMessage.RequestPayloadMsg payload, long epoch,
                                                          UuidMsg clusterId, CorfuMessage.PriorityLevel priority,
                                                          ClusterIdCheck ignoreClusterId, EpochCheck ignoreEpoch);

    /**
     * Send a request message and get a completable future to be fulfilled by the reply.
     *
     * @param payload
     * @param <T> The type of completable to return.
     * @return A completable future which will be fulfilled by the reply,
     * or a timeout in the case there is no response.
     */
    <T> CompletableFuture<T> sendRequestAndGetCompletable(
            @Nonnull RequestPayloadMsg payload,
            @Nonnull String endpoint);

    /**
     * Send a one way message, without adding a completable future.
     *
     * @param payload
     * @param epoch
     * @param clusterId
     * @param priority
     * @param ignoreClusterId
     * @param ignoreEpoch
     */
    void sendRequest(CorfuMessage.RequestPayloadMsg payload, long epoch, UuidMsg clusterId,
                     CorfuMessage.PriorityLevel priority, ClusterIdCheck ignoreClusterId, EpochCheck ignoreEpoch);

    /**
     * Complete a given outstanding request with a completion value.
     *
     * @param requestID  The request to complete.
     * @param completion The value to complete the request with
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
     * Reestablish the connection.
     */
    void reconnect();

    /**
     * The host that this router is routing requests for.
     */
    String getHost();

    /**
     * The port that this router is routing requests for.
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
