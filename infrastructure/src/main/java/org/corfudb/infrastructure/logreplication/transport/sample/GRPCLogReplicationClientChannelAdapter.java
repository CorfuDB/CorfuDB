package org.corfudb.infrastructure.logreplication.transport.sample;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationChannelGrpc;
import org.corfudb.infrastructure.logreplication.LogReplicationChannelGrpc.LogReplicationChannelBlockingStub;
import org.corfudb.infrastructure.logreplication.LogReplicationChannelGrpc.LogReplicationChannelStub;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientRouter;
import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.util.NodeLocator;

import javax.annotation.Nonnull;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
    private final ExecutorService executorService;

    private final ConcurrentMap<Long, StreamObserver<RequestMsg>> requestObserverMap;
    private final ConcurrentMap<Long, StreamObserver<ResponseMsg>> responseObserverMap;

    /** A {@link CompletableFuture} which is completed when a connection to a remote leader is set,
     * and  messages can be sent to the remote node.
     */
    @Getter
    volatile CompletableFuture<Void> connectionFuture;

    /** Construct client for accessing LogReplicationService server using the existing channel. */
    public GRPCLogReplicationClientChannelAdapter(
            String localClusterId,
            ClusterDescriptor remoteClusterDescriptor,
            LogReplicationClientRouter adapter) {
        super(localClusterId, remoteClusterDescriptor, adapter);

        this.channelMap = new HashMap<>();
        this.blockingStubMap = new HashMap<>();
        this.asyncStubMap = new HashMap<>();
        this.executorService = Executors.newSingleThreadExecutor();
        this.connectionFuture = new CompletableFuture<>();
        this.requestObserverMap = new ConcurrentHashMap<>();
        this.responseObserverMap = new ConcurrentHashMap<>();
    }

    @Override
    public void connectAsync() {
        this.executorService.submit(() ->
        getRemoteClusterDescriptor().getNodeDescriptors().forEach(node -> {
            try {
                NodeLocator nodeLocator = NodeLocator.parseString(node.getEndpoint());
                log.info("GRPC create connection to node{}@{}:{}", node.getNodeId(), nodeLocator.getHost(), nodeLocator.getPort());
                ManagedChannel channel = NettyChannelBuilder.forAddress(new InetSocketAddress(nodeLocator.getHost(), nodeLocator.getPort()))
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
        Optional<String> endpoint = getRemoteClusterDescriptor().getNodeDescriptors()
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
    public void send(@Nonnull String nodeId, @Nonnull RequestMsg request) {
        // Check the connection future. If connected, continue with sending the message.
        // If timed out, return a exceptionally completed with the timeout.
        switch (request.getPayload().getPayloadCase()) {
            case LR_ENTRY:
                replicate(nodeId, request);
                break;
            case LR_LEADERSHIP_QUERY:
                queryLeadership(nodeId, request);
                break;
            case LR_METADATA_REQUEST:
                requestMetadata(nodeId, request);
                break;
            default:
                break;
        }
    }

    private void queryLeadership(String nodeId, RequestMsg request) {
        try {
            if (blockingStubMap.containsKey(nodeId)) {
                ResponseMsg response = blockingStubMap.get(nodeId).withDeadlineAfter(10, TimeUnit.SECONDS).queryLeadership(request);
                receive(response);
            } else {
                log.warn("Stub not found for remote endpoint {}. Dropping message of type {}",
                        nodeId, request.getPayload().getPayloadCase());
            }
        } catch (Exception e) {
            log.error("Caught exception while sending message to query leadership status id {}",
                    request.getHeader().getRequestId(), e);
            getRouter().completeExceptionally(request.getHeader().getRequestId(), e);
        }
    }

    private void requestMetadata(String nodeId, RequestMsg request) {
        try {
            if (blockingStubMap.containsKey(nodeId)) {
                ResponseMsg response = blockingStubMap.get(nodeId).withDeadlineAfter(10, TimeUnit.SECONDS).negotiate(request);
                receive(response);
            } else {
                log.warn("Stub not found for remote endpoint {}. Dropping message of type {}",
                        nodeId, request.getPayload().getPayloadCase());
            }
        } catch (Exception e) {
            log.error("Caught exception while sending message to query metadata id={}",
                    request.getHeader().getRequestId(), e);
            getRouter().completeExceptionally(request.getHeader().getRequestId(), e);
        }
    }

    private void replicate(String nodeId, RequestMsg request) {
        long requestId = request.getHeader().getRequestId();
        if (!requestObserverMap.containsKey(requestId)) {
            StreamObserver<ResponseMsg> responseObserver = new StreamObserver<ResponseMsg>() {
                @Override
                public void onNext(ResponseMsg response) {
                    try {
                        log.info("Received ACK for {}", response.getHeader().getRequestId());
                        receive(response);
                    } catch (Exception e) {
                        log.error("Caught exception while receiving ACK", e);
                        getRouter().completeExceptionally(response.getHeader().getRequestId(), e);
                        requestObserverMap.remove(requestId);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Error from response observer", t);
                    getRouter().completeExceptionally(requestId, t);
                    requestObserverMap.remove(requestId);
                }

                @Override
                public void onCompleted() {
                    log.info("Completed");
                    requestObserverMap.remove(requestId);
                }
            };

            responseObserverMap.put(requestId, responseObserver);

            log.info("Initiate stub for replication");

            if(asyncStubMap.containsKey(nodeId)) {
                StreamObserver<RequestMsg> requestObserver = asyncStubMap.get(nodeId).replicate(responseObserver);
                requestObserverMap.put(requestId, requestObserver);
            } else {
                log.error("No stub found for remote node {}@{}. Message dropped type={}",
                        nodeId, getRemoteClusterDescriptor().getEndpointByNodeId(nodeId),
                        request.getPayload().getPayloadCase());
            }
        }

        log.info("Send replication entry: {} to node {}@{}", request.getHeader().getRequestId(),
                nodeId, getRemoteClusterDescriptor().getEndpointByNodeId(nodeId));
        if (responseObserverMap.containsKey(requestId)) {
            // Send log replication entries across channel
            requestObserverMap.get(requestId).onNext(request);
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

    @Override
    public void resetRemoteLeader() {
        // No-op
    }
}
