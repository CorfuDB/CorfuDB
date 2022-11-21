package org.corfudb.infrastructure.logreplication.transport.client;

import lombok.Getter;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSinkClientRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSourceClientRouter;
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
    private final ClusterDescriptor remoteClusterDescriptor;

    @Getter
    private final LogReplicationSourceClientRouter sourceRouter;

    @Getter
    private final LogReplicationSinkClientRouter sinkRouter;

    /**
     * Default Constructor
     *
     * @param localClusterId local cluster unique identifier
     * @param remoteClusterDescriptor descriptor of the remote cluster (sink)
     * @param sourceRouter interface to forward
     */
    public IClientChannelAdapter(@Nonnull String localClusterId,
                                 @Nonnull ClusterDescriptor remoteClusterDescriptor,
                                 @Nonnull LogReplicationSourceClientRouter sourceRouter,
                                 @Nonnull LogReplicationSinkClientRouter sinkRouter) {
        this.localClusterId = localClusterId;
        this.remoteClusterDescriptor = remoteClusterDescriptor;
        this.sourceRouter = sourceRouter;
        this.sinkRouter = sinkRouter;
    }

    /**
     * Connect Asynchronously to all endpoints specified in the Cluster Descriptor.
     */
    public void connectAsync() {}

    /**
     * If connection is lost to a specific endpoint, attempt to reconnect to the specific node.
     */
    public void connectAsync(String nodeId) {}

    /**
     * Stop communication across Clusters.
     */
    public void stop() {}

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
        if (getSinkRouter() != null) {
            getSinkRouter().receive(msg);
        } else {
            getSourceRouter().receive(msg);
        }
    }

    public void receive(RequestMsg msg) {
        if (getSinkRouter() != null) {
            getSinkRouter().receive(msg);
        } else {
            getSourceRouter().receive(msg);
        }
    }

    /**
     * Callback upon connectivity.
     *
     * The implementer of the adapter must notify back on a connection being established.
     *
     * @param nodeId remote node id for which the connection was established.
     */
    public void onConnectionUp(String nodeId) {
        if (getSinkRouter() != null) {
            getSinkRouter().onConnectionUp(nodeId);
        } else {
            getSourceRouter().onConnectionUp(nodeId);
        }
    }

    /**
     * Callback upon connectivity loss.
     *
     * The implementer of the adapter must notify back on a connection being lost.
     *
     * @param nodeId remote node id for which the connection was lost.
     */
    public void onConnectionDown(String nodeId) {
        if (getSinkRouter() != null) {
            getSinkRouter().onConnectionDown(nodeId);
        } else {
            getSourceRouter().onConnectionDown(nodeId);
        }
    }

    /**
     * Callback upon fatal error.
     *
     * The implementer of the adapter must notify back on a fatal error.
     *
     * @param t
     */
    public void onError(Throwable t) {
        if (getSinkRouter() != null) {
            getSinkRouter().onError(t);
        } else {
            getSourceRouter().onError(t);
        }
    }

    /**
     * Retrieve remote leader.
     *
     * @return leader in remote cluster
     */
    public Optional<String> getRemoteLeader() {
        if(getSourceRouter() != null) {
            return getSourceRouter().getRemoteLeaderNodeId();
        }
        return Optional.empty();
    }

    public abstract void resetRemoteLeader();
}
