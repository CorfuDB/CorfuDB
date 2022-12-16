package org.corfudb.infrastructure.logreplication.runtime;

import org.corfudb.runtime.proto.service.CorfuMessage;

public interface LogReplicationClientRouter {

    /**
     * Connect to remote cluster through the specified channel adapter
     */
    void connect();

    /**
     * Connection Up Callback.
     *
     * @param nodeId id of the remote node to which connection was established.
     */
    void onConnectionUp(String nodeId);

    /**
     * Connection Down Callback.
     *
     * @param nodeId id of the remote node to which connection came down.
     */
    void onConnectionDown(String nodeId);

    /**
     * on error Callback.
     *
     */
    void onError(Throwable t);

    /**
     * Forward the msg received to a source router
     *
     * @param msg, response msg sent by SINK
     */
    void receive(CorfuMessage.ResponseMsg msg);

    /**
     * Forward the msg received to a sink router
     *
     * @param msg, request msg sent by SOURCE
     */
    void receive(CorfuMessage.RequestMsg msg);

    /**
     * When an error occurs in the Channel Adapter, trigger
     * exceptional completeness of all pending requests.
     *
     * @param e exception
     */
    void completeAllExceptionally(Exception e);
}
