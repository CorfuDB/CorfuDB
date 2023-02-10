package org.corfudb.infrastructure.logreplication.transport.sample;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;

import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientRouter;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class NettyLogReplicationClientChannelAdapter extends IClientChannelAdapter {

    /**
     * Map of remote node id to Channel
     */
    private volatile Map<String, CorfuNettyClientChannel> channels;

    private final ExecutorService executorService;

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
            for (NodeDescriptor node : remoteCluster.getNodeDescriptors()) {
                log.info("Create Netty Channel to remote node {}@{}:{}", node.getNodeId(), node.getHost(), node.getPort());
                CorfuNettyClientChannel channel = new CorfuNettyClientChannel(node, getRouter().getParameters().getNettyEventLoop(), this);
                this.channels.put(node.getNodeId(), channel);
            }
        });
    }

    @Override
    public void stop() {
        channels.values().forEach(ch -> ch.close());
    }

    @Override
    public void send(@Nonnull String nodeId, @NonNull RequestMsg request) {
        // Check the connection future. If connected, continue with sending the message.
        // If timed out, return a exceptionally completed with the timeout.
        if (channels.containsKey(nodeId)) {
            log.info("Sending message to {} on cluster {}, type={}",
                    nodeId, getRemoteClusterDescriptor().getClusterId(),
                    request.getPayload().getPayloadCase());
            channels.get(nodeId).send(request);
        } else {
            log.warn("Channel to node {}@{} does not exist, message of type={} is dropped",
                    nodeId, getRemoteClusterDescriptor().getEndpointByNodeId(nodeId),
                    request.getPayload().getPayloadCase());
        }
    }

    @Override
    public void onConnectionUp(String nodeId) {
        executorService.submit(() -> super.onConnectionUp(nodeId));
    }

    private String getLeaderEndpoint() {
        if (getRemoteLeader().isPresent()) {
            return getRemoteLeader().get();
        } else {
            log.warn("No remote leader on cluster id={}", getRemoteClusterDescriptor().getClusterId());
            throw new NetworkException("No connection to leader.", getRemoteClusterDescriptor().getClusterId());
        }
    }

    public void completeExceptionally(Exception exception) {
        getRouter().completeAllExceptionally(exception);
    }

    @Override
    public void resetRemoteLeader() {
        // No-op
    }
}
