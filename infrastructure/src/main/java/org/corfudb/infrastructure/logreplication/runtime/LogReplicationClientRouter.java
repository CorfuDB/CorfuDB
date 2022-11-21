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

    void  onError(Throwable t);

    void receive(CorfuMessage.ResponseMsg msg);

    void receive(CorfuMessage.RequestMsg msg);

    void completeAllExceptionally(Exception e);
}
