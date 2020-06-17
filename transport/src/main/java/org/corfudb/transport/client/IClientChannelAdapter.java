package org.corfudb.transport.client;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.logreplication.cluster.ClusterDescriptor;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.transport.logreplication.LogReplicationClientRouter;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.UUID;


/**
 * Client Transport Adapter.
 *
 * Log Replication allows the definition of a custom transport layer for communication across clusters.
 * This interface must be extended by the client-side adapter to implement a custom channel.
 *
 * @author annym 05/15/2020
 */
@Slf4j
public abstract class IClientChannelAdapter {

    @Getter
    private final String localClusterId;

    @Getter
    private final ClusterDescriptor remoteClusterDescriptor;

    @Getter
    private final LogReplicationClientRouter router;

    /**
     * Default Constructor
     *
     * @param localClusterId local cluster unique identifier
     * @param remoteClusterDescriptor descriptor of the remote cluster (standby)
     * @param router interface to forward
     */
    public IClientChannelAdapter(@Nonnull String localClusterId,
                                 @Nonnull ClusterDescriptor remoteClusterDescriptor,
                                 @Nonnull LogReplicationClientRouter router) {
        this.localClusterId = localClusterId;
        this.remoteClusterDescriptor = remoteClusterDescriptor;
        this.router = router;
    }

    /**
     * Connect Asynchronously to all endpoints specified in the Cluster Descriptor.
     */
    public void connectAsync() {}

    /**
     * If connection is lost to a specific endpoint, attempt to reconnect to the specific endpoint.
     */
    public void connectAsync(String endpoint) {}

    /**
     * Stop communication across Clusters.
     */
    public void stop() {}

    /**
     * Send a message across the channel to a specific endpoint.
     *
     * @param endpoint remote endpoint
     * @param msg corfu message to be sent
     */
    public abstract void send(String endpoint, CorfuMessage msg);

    /**
     * Notify adapter of cluster change or reconfiguration.
     *
     * Since the adapter manages the connections to the remote site it must close or open
     * connections accordingly.
     *
     * @param remoteClusterDescriptor new descriptor for remote (standby) cluster
     */
    public void clusterChangeNotification(ClusterDescriptor remoteClusterDescriptor) {}

    /**
     * Receive a message from Server.
     * The adapter will forward this message to the router for further processing.
     *
     * @param msg received corfu message
     */
    public void receive(CorfuMessage msg) {
        getRouter().receive(msg);
    }

    /**
     * Callback upon connectivity.
     *
     * The implementer of the adapter must notify back on a connection being stablished.
     *
     * @param endpoint remote endpoint for which the connection was established.
     */
    public void onConnectionUp(String endpoint) {
        getRouter().onConnectionUp(endpoint);
    }

    /**
     * Callback upon connectivity loss.
     *
     * The implementer of the adapter must notify back on a connection being lost.
     *
     * @param endpoint remote endpoint for which the connection was lost.
     */
    public void onConnectionDown(String endpoint) {
        getRouter().onConnectionDown(endpoint);
    }

    /**
     * Callback upon fatal error.
     *
     * The implementer of the adapter must notify back on a fatal error.
     *
     * @param t
     */
    public void onError(Throwable t) {}

    /**
     * Retrieve remote leader.
     *
     * @return leader in remote cluster
     */
    public Optional<String> getRemoteLeader() {
        return getRouter().getRemoteLeaderEndpoint();
    }
}
