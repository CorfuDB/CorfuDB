package org.corfudb.infrastructure.logreplication.transport;

import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.proto.service.CorfuMessage;
import javax.annotation.Nonnull;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


public interface IClientServerRouter {

    /**
     * Exceptionally complete a request with a given cause.
     *
     * @param requestID The request to complete.
     * @param cause     The cause to give for the exceptional completion.
     */
    void completeExceptionally(LogReplication.LogReplicationSession session, long requestID, Throwable cause);

    /**
     * Complete a given outstanding request with a completion value.
     *
     * @param session    The session the request belongs to
     * @param requestID  The request to complete.
     * @param completion The value to complete the request with
     */
    <T> void completeRequest(LogReplication.LogReplicationSession session, long requestID, T completion);

    /**
     * Receive a request message from the transport layer
     *
     * @param request The request received.
     */
    void receive(CorfuMessage.RequestMsg request);

    /**
     * Receive a response message from the transport layer
     *
     * @param response The response received
     */
    void receive(CorfuMessage.ResponseMsg response);

    /**
     * Send a request message and get a completable future to be fulfilled by the reply.
     *
     * @param payload
     * @param <T> The type of completable to return.
     * @return A completable future which will be fulfilled by the reply,
     * or a timeout in the case there is no response.
     */
    <T> CompletableFuture<T> sendRequestAndGetCompletable(
            @Nonnull LogReplication.LogReplicationSession session,
            @Nonnull CorfuMessage.RequestPayloadMsg payload,
            @Nonnull String endpoint);

    /**
     * Send a response message
     *
     * @param response Log replication response message
     */
    void sendResponse(CorfuMessage.ResponseMsg response);

    /**
     * Set the Response timeout
     *
     * @param timeoutResponse Response timeout in milliseconds.
     */
    void setTimeoutResponse(long timeoutResponse);

    /**
     * Stops routing requests.
     */
    void stop(Set<LogReplication.LogReplicationSession> session);

}
