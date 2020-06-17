package org.corfudb.transport.client;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.logreplication.cluster.ClusterDescriptor;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.transport.logreplication.LogReplicationClientRouter;

import javax.annotation.Nonnull;

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
    private final ClusterDescriptor remoteClusterDescriptor;

    @Getter
    private final LogReplicationClientRouter router;

    /**
     * Default Constructor
     *
     * @param remoteClusterDescriptor descriptor of the remote cluster (standby)
     * @param router interface to forward
     */
    public IClientChannelAdapter(@Nonnull ClusterDescriptor remoteClusterDescriptor,
                                 @Nonnull LogReplicationClientRouter router) {
        this.remoteClusterDescriptor = remoteClusterDescriptor;
        this.router = router;
    }

    /**
     * Send a message across the channel.
     *
     * For this API, the endpoint is not specified, therefore its the transport implementation which will
     * decide to which remote node to send the request. Leader node is provided as part of the Router utilities.
     *
     * @param msg corfu message to be sent
     */
    public abstract void send(CorfuMessage msg);

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
     *
     * @param msg received corfu message
     */
    public void receive(CorfuMessage msg) {
        getRouter().receive(msg);
    }

    /**
     * Adapter initialization.
     *
     * If any setup is required in advance, this will be triggered upon adapter creation.
     */
    public void start() {}

    /**
     * Stop communication across Clusters.
     */
    public void stop() {}

    /**
     * Return remote leader.
     *
     * @return leader in remote cluster
     */
    public String getRemoteLeader() {
        return getRouter().getRemoteLeaderEndpoint();
    }
}
