package org.corfudb.infrastructure.logreplication.transport.client;

import lombok.Getter;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;
import org.corfudb.infrastructure.logreplication.transport.IChannelContext;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;

import javax.annotation.Nonnull;
import java.util.Optional;


/**
 * Client Transport Adapter.
 *
 * Log Replication allows the definition of a custom transport layer for communication across clusters.
 * This interface must be extended by the client-side adapter to implement a custom channel.
 *
 * @author annym 05/15/2020
 */
public abstract class IClientChannelAdapter {

    @Getter
    private final String localClusterId;

    @Getter
    private final LogReplicationClientServerRouter router;

    @Getter
    private IChannelContext channelContext;

    /**
     * Default Constructor
     *
     * @param localClusterId local cluster unique identifier
     * @param router interface between LR and the transport layer
     */
    public IClientChannelAdapter(@Nonnull String localClusterId,
                                 @Nonnull LogReplicationClientServerRouter router) {
        this.localClusterId = localClusterId;
        this.router = router;
    }

    /**
     * Connect Asynchronously to all endpoints specified in the Cluster Descriptor.
     */
    public abstract void connectAsync(ClusterDescriptor remoteCluster, LogReplicationSession sessionMsg) throws Exception;

    /**
     * If connection is lost to a specific endpoint, attempt to reconnect to the specific node.
     */
    public abstract void connectAsync(ClusterDescriptor remoteCluster, String nodeId, LogReplicationSession sessionMsg);

    /**
     * Stop communication across all remote clusters.
     */
    public void stop() {}

    /**
     * Set shared context
     */
    public void setChannelContext(IChannelContext channelContext) {
        this.channelContext = channelContext;
    }

    /**
     * Send a message across the channel to a specific endpoint.
     *
     * @param nodeId remote node id
     * @param request corfu message to be sent
     */
    public abstract void send(String nodeId, RequestMsg request);

    /**
     * Send a message across the channel to a specific endpoint.
     *
     * @param nodeId remote node id
     * @param request corfu message to be sent
     */
    public abstract void send(String nodeId, ResponseMsg request);

    /**
     * Notify adapter of cluster change or reconfiguration.
     *
     * Since the adapter manages the connections to the remote site it must close or open
     * connections accordingly.
     *
     * @param remoteClusterDescriptor new descriptor for remote (sink) cluster
     */
    public void clusterChangeNotification(ClusterDescriptor remoteClusterDescriptor) {}

    /**
     * Receive a message from Server.
     * The adapter will forward this message to the router for further processing.
     *
     * @param msg received corfu message
     */
    public void receive(ResponseMsg msg) {
        router.receive(msg);
    }

    public void receive(RequestMsg msg) {
        router.receive(msg);
    }

    /**
     * Callback upon connectivity.
     *
     * The implementer of the adapter must notify back on a connection being established.
     *
     * @param nodeId remote node id for which the connection was established.
     */
    public void onConnectionUp(String nodeId, LogReplicationSession session) {
        router.onConnectionUp(nodeId, session);
    }

    /**
     * Callback upon connectivity loss.
     *
     * The implementer of the adapter must notify back on a connection being lost.
     *
     * @param nodeId remote node id for which the connection was lost.
     */
    public void onConnectionDown(String nodeId, LogReplicationSession session) {
        router.onConnectionDown(nodeId, session);
    }

    /**
     * Callback upon fatal error.
     *
     * The implementer of the adapter must notify back on a fatal error.
     *
     * @param t
     */
    public void onError(Throwable t, LogReplicationSession session) {
        router.onError(t, session);
    }

    /**
     * Retrieve remote leader.
     *
     * @return leader in remote cluster
     */
    public Optional<String> getRemoteLeader(LogReplicationSession session) {
        return router.getRemoteLeaderNodeId();
    }

    public abstract void resetRemoteLeader();

    public void processLeadershipLoss(LogReplicationSession session) { }
}
