package org.corfudb.transport.channel;

import lombok.NonNull;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.cluster.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.cluster.NodeDescriptor;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.transport.client.IClientChannelAdapter;
import org.corfudb.transport.logreplication.LogReplicationClientRouter;
import org.corfudb.util.Utils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public class NettyLogReplicationClientChannelAdapter extends IClientChannelAdapter {

    /**
     * Map of remote endpoint to Channel
     */
    private volatile Map<String, CorfuNettyClientChannel> channels;

    /**
     * Constructor
     *
     * @param remoteClusterDescriptor
     * @param router
     */
    public NettyLogReplicationClientChannelAdapter(ClusterDescriptor remoteClusterDescriptor,
                                                   @NonNull LogReplicationClientRouter router) {
        super(remoteClusterDescriptor, router);
        this.channels = new ConcurrentHashMap<>();
    }

    @Override
    public void start() {
        ClusterDescriptor remoteCluster = getRemoteClusterDescriptor();
        for (NodeDescriptor node : remoteCluster.getNodesDescriptors()) {
            log.info("Create Corfu Channel to remote {}", node.getEndpoint());
            CorfuNettyClientChannel channel = new CorfuNettyClientChannel(node, getRouter().getParameters().getNettyEventLoop(), this);
            this.channels.put(node.getEndpoint(), channel);
        }
    }

    @Override
    public void stop() {
        channels.values().forEach(ch -> ch.close());
    }

    @Override
    public void send(CorfuMessage msg) {
        send(getLeaderEndpoint(), msg);
    }

    @Override
    public void send(String endpoint, CorfuMessage msg) {
        // Check the connection future. If connected, continue with sending the message.
        // If timed out, return a exceptionally completed with the timeout.
        if (channels.containsKey(endpoint)) {
            log.trace("Sending message to {} on cluster {}, type={}", endpoint, getRemoteClusterDescriptor().getClusterId(), msg.getType());
            sendOnChannel(channels.get(endpoint), msg);
        } else {
            log.warn("Channel to {} does not exist, message of type={} is dropped", endpoint, msg.getType());
        }
    }

    private void sendOnChannel(CorfuNettyClientChannel channel, CorfuMessage msg) {
        try {
            log.info("Send on Channel message type={}", msg.getType());

            // Check the connection future. If connected, continue with sending the message.
            // If timed out, return a exceptionally completed with the timeout.
            try {
                channel.getConnectionFuture()
                        .get(getRouter().getParameters().getConnectionTimeout().toMillis(), TimeUnit.MILLISECONDS);
                channel.send(msg);
            } catch (InterruptedException e) {
                throw new UnrecoverableCorfuInterruptedError(e);
            } catch (TimeoutException te) {
                getRouter().completeExceptionally(msg.getRequestID(), te);
            } catch (ExecutionException ee) {
                getRouter().completeExceptionally(msg.getRequestID(), Utils.extractCauseWithCompleteStacktrace(ee));
            }
        } catch (Exception e) {
            log.error("Exception caught while attempting to sendOnChannel", e);
            throw new RuntimeException(e);
        }
    }

    private String getLeaderEndpoint() {
        String remoteLeaderEndpoint = getRemoteLeader();
        if (remoteLeaderEndpoint == null || remoteLeaderEndpoint.length() == 0) {
            log.warn("No remote leader on cluster id={}", getRemoteClusterDescriptor().getClusterId());
            return null;
        }

        return remoteLeaderEndpoint;
    }

    public void completeExceptionally(Exception exception) {
        getRouter().completeAllExceptionally(exception);
    }
}
