//package org.corfudb.infrastructure.logreplication.transport.sample;
//
//import lombok.NonNull;
//import lombok.extern.slf4j.Slf4j;
//import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
//import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
//
//import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSinkClientRouter;
//import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
//import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSourceClientRouter;
//import org.corfudb.runtime.LogReplication;
//import org.corfudb.runtime.exceptions.NetworkException;
//import org.corfudb.runtime.proto.service.CorfuMessage;
//import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
//
//import javax.annotation.Nonnull;
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
//@Slf4j
//public class NettyLogReplicationClientChannelAdapter extends IClientChannelAdapter {
//
//    /**
//     * Map of remote node id to Channel
//     */
//    private volatile Map<String, CorfuNettyClientChannel> channels;
//
//    private final ExecutorService executorService;
//
//    /**
//     * Constructor
//     *
//     * @param remoteClusterDescriptor
//     * @param sourceRouter not null when the connection initiator is the source
//     * @param sinkRouter not null when the connection initiator is the sink
//     */
//    public NettyLogReplicationClientChannelAdapter(@NonNull String localClusterId,
//                                                   @NonNull ClusterDescriptor remoteClusterDescriptor,
//                                                    LogReplicationSourceClientRouter sourceRouter,
//                                                   LogReplicationSinkClientRouter sinkRouter ) {
//        super(localClusterId, remoteClusterDescriptor, sourceRouter, sinkRouter);
//        this.channels = new ConcurrentHashMap<>();
//        this.executorService = Executors.newSingleThreadExecutor();
//    }
//
//    @Override
//    public void connectAsync(LogReplication.ReplicationSessionMsg session) {
//        executorService.submit(() -> {
//            ClusterDescriptor remoteCluster = getRemoteClusterDescriptor();
//            for (NodeDescriptor node : remoteCluster.getNodesDescriptors()) {
//                log.info("Create Netty Channel to remote node {}@{}:{}", node.getNodeId(), node.getHost(), node.getPort());
//                CorfuNettyClientChannel channel = null;
//                if (getSourceRouter() != null ) {
//                    log.trace("initializing the channel with existing nettyEventLoop");
//                    channel = new CorfuNettyClientChannel(node,
//                            getSourceRouter().getRuntimeFSM().getSourceManager().getParameters().getNettyEventLoop(),
//                            this);
//                } else {
//                    log.trace("Creating a new netty even loop and initializing the channel");
//                    channel = new CorfuNettyClientChannel(node, null, this);
//                }
//                this.channels.put(node.getNodeId(), channel);
//            }
//        });
//    }
//
//    @Override
//    public void stop() {
//        channels.values().forEach(ch -> ch.close());
//    }
//
//    @Override
//    public void send(@Nonnull String nodeId, @NonNull RequestMsg request) {
//        // Check the connection future. If connected, continue with sending the message.
//        // If timed out, return a exceptionally completed with the timeout.
//        if (channels.containsKey(nodeId)) {
//            log.info("Sending message to {} on cluster {}, type={}",
//                    nodeId, getRemoteClusterDescriptor().getClusterId(),
//                    request.getPayload().getPayloadCase());
//            channels.get(nodeId).send(request);
//        } else {
//            log.warn("Channel to node {}@{} does not exist, message of type={} is dropped",
//                    nodeId, getRemoteClusterDescriptor().getEndpointByNodeId(nodeId),
//                    request.getPayload().getPayloadCase());
//        }
//    }
//
//    @Override
//    public void send(String nodeId, CorfuMessage.ResponseMsg response) {}
//
//    @Override
//    public void onConnectionUp(String nodeId) {
//        executorService.submit(() -> {
//            super.onConnectionUp(nodeId);
//        });
//    }
//
//    private String getLeaderEndpoint() {
//        if (getRemoteLeader().isPresent()) {
//            return getRemoteLeader().get();
//        } else {
//            log.warn("No remote leader on cluster id={}", getRemoteClusterDescriptor().getClusterId());
//            throw new NetworkException("No connection to leader.", getRemoteClusterDescriptor().getClusterId());
//        }
//    }
//
//    public void completeExceptionally(Exception exception) {
//        if (getSourceRouter() != null) {
//            getSourceRouter().completeAllExceptionally(exception);
//        } else {
//            getSinkRouter().completeAllExceptionally(exception);
//        }
//    }
//
//    @Override
//    public void resetRemoteLeader() {
//        // No-op
//    }
//}
