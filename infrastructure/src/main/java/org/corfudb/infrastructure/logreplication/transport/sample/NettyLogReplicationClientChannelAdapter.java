package org.corfudb.infrastructure.logreplication.transport.sample;

import lombok.NonNull;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientRouter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class NettyLogReplicationClientChannelAdapter extends IClientChannelAdapter {

    /**
     * Map of remote endpoint to Channel
     */
    private volatile Map<String, CorfuNettyClientChannel> channels;

    private ExecutorService executorService;

    /**
     * Constructor
     *
     * @param remoteClusterDescriptor
     * @param router
     */
    public NettyLogReplicationClientChannelAdapter(@NonNull String localClusterId,
                                                   @NonNull ClusterDescriptor remoteClusterDescriptor,
                                                   @NonNull LogReplicationClientRouter router) {
        super(localClusterId, remoteClusterDescriptor, router);
        this.channels = new ConcurrentHashMap<>();
        this.executorService = Executors.newSingleThreadExecutor();
    }

    @Override
    public void connectAsync() {
        executorService.submit(() -> {
            ClusterDescriptor remoteCluster = getRemoteClusterDescriptor();
            for (NodeDescriptor node : remoteCluster.getNodesDescriptors()) {
                log.info("Create Corfu Channel to remote {}", node.getEndpoint());
                CorfuNettyClientChannel channel = new CorfuNettyClientChannel(node, getRouter().getParameters().getNettyEventLoop(), this);
                this.channels.put(node.getEndpoint(), channel);
            }
        });
    }

    @Override
    public void stop() {
        channels.values().forEach(ch -> ch.close());
    }

    @Override
    public void send(String endpoint, CorfuMessage msg) {
        // Check the connection future. If connected, continue with sending the message.
        // If timed out, return a exceptionally completed with the timeout.
        if (channels.containsKey(endpoint)) {
            log.info("Sending message to {} on cluster {}, type={}", endpoint, getRemoteClusterDescriptor().getClusterId(), msg.getType());
            channels.get(endpoint).send(msg);
        } else {
            log.warn("Channel to {} does not exist, message of type={} is dropped", endpoint, msg.getType());
        }
    }

    @Override
    public void onConnectionUp(String endpoint) {
        executorService.submit(() -> super.onConnectionUp(endpoint));
    }

    private String getLeaderEndpoint() {
        if(getRemoteLeader().isPresent()) {
            return getRemoteLeader().get();
        } else {
            log.warn("No remote leader on cluster id={}", getRemoteClusterDescriptor().getClusterId());
            throw new NetworkException("No connection to leader.", getRemoteClusterDescriptor().getClusterId());
        }
    }

    public void completeExceptionally(Exception exception) {
        getRouter().completeAllExceptionally(exception);
    }
}
