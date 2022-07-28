package org.corfudb.infrastructure.logreplication.transport.sample;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
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

    private static final Map<String, ManagedChannel> fsmIdTochannelMap = new ConcurrentHashMap<>();
    private static final Map<ManagedChannel, Integer> channelToNumClients = new ConcurrentHashMap<>();

    // fsmId to stub
    private final Map<String, LogReplicationChannelBlockingStub> fsmIdToblockingStub;
    private final Map<String, LogReplicationChannelStub> fsmIdToAsyncStubMap;
    private final ExecutorService executorService;

    private final ConcurrentMap<Long, StreamObserver<RequestMsg>> requestObserverMap;
    private final ConcurrentMap<Long, StreamObserver<ResponseMsg>> responseObserverMap;

    private int MAX_CLIENTS_THRESHOLD = 4;

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
        this.fsmIdToblockingStub = new HashMap<>();
        this.fsmIdToAsyncStubMap = new HashMap<>();
        this.executorService = Executors.newSingleThreadExecutor();
        this.connectionFuture = new CompletableFuture<>();
        this.requestObserverMap = new ConcurrentHashMap<>();
        this.responseObserverMap = new ConcurrentHashMap<>();
    }

    // Shama: every FSM uses this. We need to figure out if we need a new channel or if we can use the existing channel.
    @Override
    public void connectAsync() {
        // find if there exists a channel which can be multiplexed.
        this.executorService.submit(() ->
        getRemoteClusterDescriptor().getNodesDescriptors().forEach(node -> {
            try {
                NodeLocator nodeLocator = NodeLocator.parseString(node.getEndpoint());
                log.info("GRPC create connection to node{}@{}:{}", node.getNodeId(), nodeLocator.getHost(), nodeLocator.getPort());
                ManagedChannel channel = ManagedChannelBuilder.forAddress(nodeLocator.getHost(), nodeLocator.getPort())
                        .usePlaintext()
                        .build();
                fsmIdTochannelMap.put(node.getNodeId(), channel);
                fsmIdToblockingStub.put(node.getNodeId(), LogReplicationChannelGrpc.newBlockingStub(channel));
                fsmIdToAsyncStubMap.put(node.getNodeId(), LogReplicationChannelGrpc.newStub(channel));
                onConnectionUp(node.getNodeId());
            } catch (Exception e) {
                onConnectionDown(node.getNodeId());
            }
        }));
    }

//    Shama : the 2nd param is just the place holder
    private void connectAsync(String fsmId, boolean removeThis) {


        Optional<Map.Entry<ManagedChannel, Integer>> channelToIntegerEntry = channelToNumClients.entrySet().stream()
                .filter(e -> e.getValue() < MAX_CLIENTS_THRESHOLD).findFirst();

        ManagedChannel channel;
        if (channelToIntegerEntry.isPresent()) {
            channel = channelToIntegerEntry.get().getKey();
            //multiplexing
            fsmIdToblockingStub.put(fsmId, LogReplicationChannelGrpc.newBlockingStub(channel));
            fsmIdToAsyncStubMap.put(fsmId, LogReplicationChannelGrpc.newStub(channel));

            channelToNumClients.put(channel, channelToNumClients.get(channel) + 1);
            fsmIdTochannelMap.put(fsmId, channel);
        } else {
            createChannel(fsmId);
        }

    }

    private void createChannel(String fsmId) {
        getRemoteClusterDescriptor().getNodesDescriptors().forEach(node -> {
            try {
                NodeLocator nodeLocator = NodeLocator.parseString(node.getEndpoint());
                log.info("GRPC create connection to node{}@{}:{}", node.getNodeId(), nodeLocator.getHost(), nodeLocator.getPort());
                ManagedChannel channel = ManagedChannelBuilder.forAddress(nodeLocator.getHost(), nodeLocator.getPort())
                        .useTransportSecurity()  //This is TLS
                        .build();
                channelToNumClients.put(channel, channelToNumClients.get(channel) + 1);
                fsmIdTochannelMap.put(fsmId, channel);
                fsmIdToblockingStub.put(fsmId, LogReplicationChannelGrpc.newBlockingStub(channel));
                fsmIdToAsyncStubMap.put(fsmId, LogReplicationChannelGrpc.newStub(channel));
                onConnectionUp(node.getNodeId());
            } catch (Exception e) {
                onConnectionDown(node.getNodeId());
            }
        });
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
                fsmIdTochannelMap.put(nodeId, channel);
                fsmIdToblockingStub.put(nodeId, LogReplicationChannelGrpc.newBlockingStub(channel));
                fsmIdToAsyncStubMap.put(nodeId, LogReplicationChannelGrpc.newStub(channel));
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
            if (fsmIdToblockingStub.containsKey(nodeId)) {
                ResponseMsg response = fsmIdToblockingStub.get(nodeId).withWaitForReady().queryLeadership(request);
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
            if (fsmIdToblockingStub.containsKey(nodeId)) {
                ResponseMsg response = fsmIdToblockingStub.get(nodeId).withWaitForReady().negotiate(request);
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

            if(fsmIdToAsyncStubMap.containsKey(nodeId)) {
                StreamObserver<RequestMsg> requestObserver = fsmIdToAsyncStubMap.get(nodeId).replicate(responseObserver);
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
        fsmIdTochannelMap.values().forEach(channel -> {
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
