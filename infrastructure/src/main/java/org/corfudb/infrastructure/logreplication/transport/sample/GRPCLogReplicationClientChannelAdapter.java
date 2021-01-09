package org.corfudb.infrastructure.logreplication.transport.sample;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.LogReplicationChannelGrpc;
import org.corfudb.infrastructure.logreplication.LogReplicationChannelGrpc.LogReplicationChannelStub;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientRouter;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.infrastructure.logreplication.LogReplicationChannelGrpc.LogReplicationChannelBlockingStub;
import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.util.NodeLocator;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This is a default implementation of a custom channel for Log Replication Servers inter-communication
 * which relies on the standard GRPC.
 *
 * It is used for testing purposes.
 *
 * @author amartinezman
 *
 */
@Slf4j
public class GRPCLogReplicationClientChannelAdapter extends IClientChannelAdapter {

    private final Map<String, ManagedChannel> channelMap;
    private final Map<String, LogReplicationChannelBlockingStub> blockingStubMap;
    private final Map<String, LogReplicationChannelStub> asyncStubMap;

    private StreamObserver<CorfuMessage> requestObserver;
    private StreamObserver<CorfuMessage> responseObserver;

    private final ExecutorService executorService;

    /** A {@link CompletableFuture} which is completed when a connection to a remote leader is set,
     * and  messages can be sent to the remote node.
     */
    @Getter
    volatile CompletableFuture<Void> connectionFuture;

    /** Construct client for accessing LogReplicationService server using the existing channel. */
    public GRPCLogReplicationClientChannelAdapter(String localClusterId, ClusterDescriptor remoteClusterDescriptor, LogReplicationClientRouter adapter) {
        super(localClusterId, remoteClusterDescriptor, adapter);

        this.channelMap = new HashMap<>();
        this.blockingStubMap = new HashMap<>();
        this.asyncStubMap = new HashMap<>();
        this.executorService = Executors.newSingleThreadExecutor();
        this.connectionFuture = new CompletableFuture<>();
    }

    @Override
    public void connectAsync() {
        this.executorService.submit(() ->
        getRemoteClusterDescriptor().getNodesDescriptors().forEach(node -> {
            try {
                NodeLocator nodeLocator = NodeLocator.parseString(node.getEndpoint());
                log.info("GRPC create connection to node{}@{}:{}", node.getNodeId(), nodeLocator.getHost(), nodeLocator.getPort());
                ManagedChannel channel = ManagedChannelBuilder.forAddress(nodeLocator.getHost(), nodeLocator.getPort())
                        .usePlaintext()
                        .build();
                channelMap.put(node.getNodeId(), channel);
                blockingStubMap.put(node.getNodeId(), LogReplicationChannelGrpc.newBlockingStub(channel));
                asyncStubMap.put(node.getNodeId(), LogReplicationChannelGrpc.newStub(channel));
                onConnectionUp(node.getNodeId());
            } catch (Exception e) {
                onConnectionDown(node.getNodeId());
            }
        }));
    }

    @Override
    public void connectAsync(String nodeId) {
        Optional<String> endpoint = getRemoteClusterDescriptor().getNodesDescriptors()
                .stream()
                .filter(nodeDescriptor -> nodeDescriptor.getNodeId().toString().equals(nodeId))
                .map(NodeDescriptor::getEndpoint)
                .collect(Collectors.toList())
                .stream()
                .findFirst();
        NodeLocator nodeLocator;
        if (endpoint.isPresent()) {
            nodeLocator = NodeLocator.parseString(endpoint.get());
        } else {
            throw new IllegalStateException("No endpoint found for node:" + nodeId);
        }
        this.executorService.submit(() -> {
            try {
                log.info("GRPC create connection to node {}@{}:{}", nodeId, nodeLocator.getHost(), nodeLocator.getPort());
                ManagedChannel channel = ManagedChannelBuilder.forAddress(nodeLocator.getHost(), nodeLocator.getPort()).usePlaintext().build();
                channelMap.put(nodeId, channel);
                blockingStubMap.put(nodeId, LogReplicationChannelGrpc.newBlockingStub(channel));
                asyncStubMap.put(nodeId, LogReplicationChannelGrpc.newStub(channel));
                onConnectionUp(nodeId);
            } catch (Exception e) {
                onConnectionDown(nodeId);
            }
        });
    }

    @Override
    public void send(String nodeId, CorfuMessage msg) {
        // Check the connection future. If connected, continue with sending the message.
        // If timed out, return a exceptionally completed with the timeout.
        switch (msg.getType()) {
            case LOG_REPLICATION_ENTRY:
                replicate(nodeId, msg);
                break;
            case LOG_REPLICATION_QUERY_LEADERSHIP:
                queryLeadership(nodeId, msg);
                break;
            case LOG_REPLICATION_METADATA_REQUEST:
                requestMetadata(nodeId, msg);
                break;
            default:
                break;
        }
    }

    private void queryLeadership(String nodeId, CorfuMessage msg) {
        try {
            if(blockingStubMap.containsKey(nodeId)) {
                CorfuMessage response = blockingStubMap.get(nodeId).withWaitForReady().queryLeadership(msg);
                receive(response);
            } else {
                log.warn("Stub not found for remote node {}@{}. Dropping message of type {}", nodeId,
                        getRemoteClusterDescriptor().getEndpointByNodeId(nodeId), msg.getType());
            }
        } catch (Exception e) {
            log.error("Caught exception while sending message to query leadership status id {}", msg.getRequestID(), e);
            getRouter().completeExceptionally(msg.getRequestID(), e);
        }
    }

    private void requestMetadata(String nodeId, CorfuMessage msg) {
        try {
            if(blockingStubMap.containsKey(nodeId)) {
                CorfuMessage response = blockingStubMap.get(nodeId).withWaitForReady().negotiate(msg);
                receive(response);
            } else {
                log.warn("Stub not found for remote node {}@{}. Dropping message of type {}", nodeId,
                        getRemoteClusterDescriptor().getNode(nodeId), msg.getType());
            }
        } catch (Exception e) {
            log.error("Caught exception while sending message to query metadata id={}", msg.getRequestID(), e);
            getRouter().completeExceptionally(msg.getRequestID(), e);
        }
    }

    private void replicate(String nodeId, CorfuMessage msg) {
        if(requestObserver == null) {
            responseObserver = new StreamObserver<CorfuMessage>() {
                @Override
                public void onNext(CorfuMessage response) {
                    try {
                        log.info("Received ACK for {}", response.getRequestID());
                        receive(response);
                    } catch (Exception e) {
                        log.error("Caught exception while receiving ACK", e);
                        getRouter().completeExceptionally(response.getRequestID(), e);
                        requestObserver = null;
                    }
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Error from response observer", t);
                    getRouter().completeExceptionally(msg.getRequestID(), t);
                    requestObserver = null;
                }

                @Override
                public void onCompleted() {
                    log.info("Completed");
                    requestObserver = null;
                }
            };

            log.info("Initiate stub for replication");

            if(asyncStubMap.containsKey(nodeId)) {
                requestObserver = asyncStubMap.get(nodeId).replicate(responseObserver);
            } else {
                log.error("No stub found for remote node {}@{}. Message dropped type={}", nodeId,
                        getRemoteClusterDescriptor().getEndpointByNodeId(nodeId), msg.getType());
            }
        }

        log.info("Send replication entry: {} to node {}@{}", msg.getRequestID(), nodeId,
                getRemoteClusterDescriptor().getEndpointByNodeId(nodeId));
        if (responseObserver != null) {
            // Send log replication entries across channel
            requestObserver.onNext(msg);
        }
    }

    @Override
    public void stop() {
        channelMap.values().forEach(channel -> {
            try {
                channel.shutdownNow();
                channel.awaitTermination(10, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                log.error("Caught exception when waiting to shutdown channel {}", channel.toString());
            }
        });
    }

}
