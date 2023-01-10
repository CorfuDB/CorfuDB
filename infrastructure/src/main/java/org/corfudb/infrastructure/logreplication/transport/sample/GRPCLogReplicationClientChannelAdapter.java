package org.corfudb.infrastructure.logreplication.transport.sample;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import io.grpc.ConnectivityState;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.corfudb.infrastructure.logreplication.LogReplicationChannelGrpc;
import org.corfudb.infrastructure.logreplication.LogReplicationChannelGrpc.LogReplicationChannelBlockingStub;
import org.corfudb.infrastructure.logreplication.LogReplicationChannelGrpc.LogReplicationChannelStub;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSinkClientRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSourceClientRouter;
import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.util.NodeLocator;

import javax.annotation.Nonnull;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;

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

    private final int MAX_STREAMS_IN_CHANNEL = 100;

    private static Map<ManagedChannel, Set<LogReplication.ReplicationSessionMsg>> channelToStreamsMap = new HashMap<>();
    private static final ReentrantLock lock = new ReentrantLock();

    private final Map<String, Set<ManagedChannel>> nodeIdToChannelMap;
    private final Map<LogReplication.ReplicationSessionMsg, LogReplicationChannelBlockingStub> blockingStubMap;
    private final Map<LogReplication.ReplicationSessionMsg, LogReplicationChannelStub> asyncStubMap;
    private final ExecutorService executorService;

    private final ConcurrentMap<LogReplication.ReplicationSessionMsg, StreamObserver<RequestMsg>> requestObserverMap;
    private final ConcurrentMap<LogReplication.ReplicationSessionMsg, StreamObserver<ResponseMsg>> responseObserverMap;
    private final ConcurrentMap<Pair<LogReplication.ReplicationSessionMsg, Long>, StreamObserver<RequestMsg>> replicationReqObserverMap;
    private final ConcurrentMap<Pair<LogReplication.ReplicationSessionMsg, Long>, StreamObserver<ResponseMsg>> replicationResObserverMap;

    /** A {@link CompletableFuture} which is completed when a connection to a remote leader is set,
     * and  messages can be sent to the remote node.
     */
    @Getter
    volatile CompletableFuture<Void> connectionFuture;

    Context context;

    /** Construct client for accessing LogReplicationService server using the existing channel. */
    public GRPCLogReplicationClientChannelAdapter(
            String localClusterId,
            ClusterDescriptor remoteClusterDescriptor,
            LogReplicationSourceClientRouter sourceRouter, LogReplicationSinkClientRouter sinkRouter ) {
        super(localClusterId, remoteClusterDescriptor, sourceRouter, sinkRouter);

        this.nodeIdToChannelMap = new HashMap<>();
        this.blockingStubMap = new HashMap<>();
        this.asyncStubMap = new HashMap<>();
        this.executorService = Executors.newSingleThreadExecutor();
        this.connectionFuture = new CompletableFuture<>();
        this.requestObserverMap = new ConcurrentHashMap<>();
        this.responseObserverMap = new ConcurrentHashMap<>();
        this.replicationReqObserverMap = new ConcurrentHashMap<>();
        this.replicationResObserverMap = new ConcurrentHashMap<>();
        context = Context.current();
    }

    @Override
    public void connectAsync(LogReplication.ReplicationSessionMsg session) {
        this.executorService.submit(() ->
        getRemoteClusterDescriptor().getNodesDescriptors().forEach(node -> {
            try {
                NodeLocator nodeLocator = NodeLocator.parseString(node.getEndpoint());
                ManagedChannel channel;

                lock.lock();
                Pair<Boolean, ManagedChannel> reuseChannel = canReuseChannel(node.getNodeId());
                if(!nodeIdToChannelMap.containsKey(node.getNodeId()) || !reuseChannel.getLeft()) {
                    log.info("GRPC create new channel to node{}@{}:{}", node.getNodeId(), nodeLocator.getHost(), nodeLocator.getPort());
                    channel = ManagedChannelBuilder.forAddress(nodeLocator.getHost(), nodeLocator.getPort())
                            .usePlaintext()
                            .build();
                    nodeIdToChannelMap.putIfAbsent(node.getNodeId(), new HashSet<>());
                    nodeIdToChannelMap.get(node.getNodeId()).add(channel);
                } else {
                    channel = reuseChannel.getRight();
                }
                channelToStreamsMap.putIfAbsent(channel, new HashSet<>());
                channelToStreamsMap.get(channel).add(session);
                lock.unlock();

                blockingStubMap.put(session, LogReplicationChannelGrpc.newBlockingStub(channel));
                asyncStubMap.put(session, LogReplicationChannelGrpc.newStub(channel));
                onConnectionUp(node.getNodeId());
            } catch (Exception e) {
                log.error("Error: {} :::: {}", e.getCause(), e.getStackTrace());
                onConnectionDown(node.getNodeId());
            }
        }));
    }

    private Pair<Boolean, ManagedChannel> canReuseChannel(String nodeId) {
        if(!nodeIdToChannelMap.isEmpty()) {
            Set<ManagedChannel> channels = nodeIdToChannelMap.get(nodeId);
            if (!channels.isEmpty()) {
                for (ManagedChannel channel : channels) {
                    if (channelToStreamsMap.get(channel).size() < MAX_STREAMS_IN_CHANNEL && !channel.getState(false).equals(ConnectivityState.SHUTDOWN)) {
                        return Pair.of(true, channel);
                    }
                }
            }
        }

        return Pair.of(false, null);
    }

    @Override
    public void connectAsync(String nodeId, LogReplication.ReplicationSessionMsg session) {
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
                ManagedChannel channel;

                lock.lock();
                Pair<Boolean, ManagedChannel> reuseChannel = canReuseChannel(nodeId);
                if(!nodeIdToChannelMap.containsKey(nodeId) || !reuseChannel.getLeft()) {
                    log.info("GRPC create new channel to node{}@{}:{}", nodeId, nodeLocator.getHost(), nodeLocator.getPort());
                    channel = ManagedChannelBuilder.forAddress(nodeLocator.getHost(), nodeLocator.getPort())
                            .usePlaintext()
//                            .defaultServiceConfig(getRetryingServiceConfig())
//                            .enableRetry()
                            .build();
                    nodeIdToChannelMap.putIfAbsent(nodeId, new HashSet<>());
                    nodeIdToChannelMap.get(nodeId).add(channel);
                } else {
                    channel = reuseChannel.getRight();
                }
                channelToStreamsMap.putIfAbsent(channel, new HashSet<>());
                channelToStreamsMap.get(channel).add(session);
                lock.unlock();

                blockingStubMap.put(session, LogReplicationChannelGrpc.newBlockingStub(channel));
                asyncStubMap.put(session, LogReplicationChannelGrpc.newStub(channel));
                onConnectionUp(nodeId);
            } catch (Exception e) {
                log.error("Error: {} :::: {}", e.getCause(), e.getStackTrace());
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

    // Used when connection is triggered from SINK
    @Override
    public void send(String nodeId, ResponseMsg response) {
        if(nodeId == null) {
            nodeId = this.getSinkRouter().getRemoteLeaderNodeId().get();
        }

        LogReplication.ReplicationSessionMsg sessionMsg;
        //SINK sends subscribe, Negotiation and ACKs
        if(response.getPayload().getPayloadCase().equals(CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_METADATA_RESPONSE)) {
            sessionMsg = response.getPayload().getLrMetadataResponse().getSessionInfo();
        } else if(response.getPayload().getPayloadCase().equals(CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_ENTRY_ACK)) {
            sessionMsg = response.getPayload().getLrEntryAck().getMetadata().getSessionInfo();
        } else if(response.getPayload().getPayloadCase().equals(CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_SUBSCRIBE_REQUEST)) {
            sessionMsg = response.getPayload().getLrSubscribeRequest().getSessionInfo();
        } else {
            sessionMsg = null;
            log.info("Unexpected payloadType {}", response.getPayload().getPayloadCase());
        }

        if (!responseObserverMap.containsKey(sessionMsg)) {
            String finalNodeId = nodeId;
            StreamObserver<RequestMsg> requestObserver = new StreamObserver<RequestMsg>() {
                @Override
                public void onNext(RequestMsg request) {
                    try {
                        log.info("Received request {}", request.getHeader().getRequestId());
                        receive(request);
                    } catch (Exception e) {
                        log.error("Caught exception while receiving Requests", e);
                        getSinkRouter().completeExceptionally(request.getHeader().getRequestId(), e);
                        responseObserverMap.remove(sessionMsg);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Error from request observer", t);
                    long requestId = response.getHeader().getRequestId();
                    getSinkRouter().completeExceptionally(requestId, t);
                    responseObserverMap.remove(sessionMsg);
                    onServiceUnavailable(t, finalNodeId, sessionMsg);
                }

                @Override
                public void onCompleted() {
                    responseObserverMap.remove(sessionMsg);
                }
            };

            requestObserverMap.put(sessionMsg, requestObserver);

            if(asyncStubMap.containsKey(sessionMsg)) {
                StreamObserver<ResponseMsg> responseObserver = asyncStubMap.get(sessionMsg).subscribeAndStartreplication(requestObserver);
                responseObserverMap.put(sessionMsg, responseObserver);
            } else {
                log.error("No stub found for remote node {}@{}. Message dropped type={}",
                        nodeId, getRemoteClusterDescriptor().getEndpointByNodeId(nodeId),
                        response.getPayload().getPayloadCase());
            }
        }

        if (requestObserverMap.containsKey(sessionMsg)) {
            // Send negotiation and log replication ACKs across channel
            responseObserverMap.get(sessionMsg).onNext(response);
        }
    }

    private void queryLeadership(String nodeId, RequestMsg request) {
        LogReplication.ReplicationSessionMsg session = request.getPayload().getLrLeadershipQuery().getSessionInfo();
        try {
            log.info("queryLeadership for session {}", session);
            if (blockingStubMap.containsKey(session)) {
                ResponseMsg response = blockingStubMap.get(session).withWaitForReady().queryLeadership(request);
                receive(response);
            } else {
                log.warn("Stub not found for remote endpoint {}. Dropping message of type {}",
                        nodeId, request.getPayload().getPayloadCase());
            }
        } catch (Exception e) {
            log.error("Caught exception while sending message to query leadership status id {}",
                    request.getHeader().getRequestId(), e);
            onServiceUnavailable(e, nodeId, session);
            getSourceRouter().completeExceptionally(request.getHeader().getRequestId(), e);
        }
    }

    private void requestMetadata(String nodeId, RequestMsg request) {
        LogReplication.ReplicationSessionMsg session = request.getPayload().getLrMetadataRequest().getSessionInfo();
        try {
            if (blockingStubMap.containsKey(session)) {
                ResponseMsg response = blockingStubMap.get(session).withWaitForReady().negotiate(request);
                receive(response);
            } else {
                log.warn("Stub not found for remote endpoint {}. Dropping message of type {}",
                        nodeId, request.getPayload().getPayloadCase());
            }
        } catch (Exception e) {
            log.error("Caught exception while sending message to query metadata id={}",
                    request.getHeader().getRequestId(), e);
            onServiceUnavailable(e, nodeId, session);
            getSourceRouter().completeExceptionally(request.getHeader().getRequestId(), e);
        }
    }

    private void replicate(String nodeId, RequestMsg request) {
        LogReplication.ReplicationSessionMsg sessionMsg = request.getPayload().getLrEntry().getMetadata().getSessionInfo();
        long requestId = request.getHeader().getRequestId();

        if (!replicationReqObserverMap.containsKey(Pair.of(sessionMsg, requestId))) {
            StreamObserver<ResponseMsg> responseObserver = new StreamObserver<ResponseMsg>() {
                @Override
                public void onNext(ResponseMsg response) {
                    try {
                        log.info("Received ACK for {}", response.getHeader().getRequestId());
                        receive(response);
                    } catch (Exception e) {
                        log.error("Caught exception while receiving ACK", e);
                        getSourceRouter().completeExceptionally(response.getHeader().getRequestId(), e);
                        replicationReqObserverMap.remove(Pair.of(sessionMsg, requestId));
                    }
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Error from response observer", t);
                    long requestId = request.getHeader().getRequestId();
                    onServiceUnavailable(t, nodeId, sessionMsg);
                    getSourceRouter().completeExceptionally(requestId, t);
                    replicationReqObserverMap.remove(Pair.of(sessionMsg, requestId));
                }

                @Override
                public void onCompleted() {
                    replicationReqObserverMap.remove(Pair.of(sessionMsg, requestId));
                }
            };
            replicationResObserverMap.put(Pair.of(sessionMsg, requestId), responseObserver);

            if(asyncStubMap.containsKey(sessionMsg)) {
                StreamObserver<RequestMsg> requestObserver = asyncStubMap.get(sessionMsg).replicate(responseObserver);
                replicationReqObserverMap.put(Pair.of(sessionMsg, requestId), requestObserver);
            } else {
                log.error("No stub found for remote node {}@{}. Message dropped type={}",
                        nodeId, getRemoteClusterDescriptor().getEndpointByNodeId(nodeId),
                        request.getPayload().getPayloadCase());
            }
        }

        log.info("Send replication entry: {} to node {}@{}", request.getHeader().getRequestId(),
                nodeId, getRemoteClusterDescriptor().getEndpointByNodeId(nodeId));
        if (replicationResObserverMap.containsKey(Pair.of(sessionMsg, requestId))) {
            // Send log replication entries across channel
            replicationReqObserverMap.get(Pair.of(sessionMsg, requestId)).onNext(request);
        }
    }

    @Override
    public void stop() {
        lock.lock();
        nodeIdToChannelMap.values().stream().forEach(channelSet -> {
            if(!channelSet.isEmpty()) {
                channelSet.stream().forEach(channel -> {
                    try {
                        channel.shutdownNow();
                        channel.awaitTermination(10, TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                        log.error("Caught exception when waiting to shutdown channel {}", channel.toString());
                    }
                });
            }
        });
        lock.unlock();
    }

    @Override
    public void resetRemoteLeader() {
        // No-op
    }

    private void onServiceUnavailable(Throwable t, String nodeId, LogReplication.ReplicationSessionMsg sessionMsg) {
        Set<ManagedChannel> allChannelsToNode = nodeIdToChannelMap.get(nodeId);

        allChannelsToNode.stream().forEach(channel -> {
            ConnectivityState channelState = channel.getState(false);
            lock.lock();
            channelToStreamsMap.remove(channel);
            nodeIdToChannelMap.remove(nodeId);
            lock.unlock();

            if (!channelState.equals(ConnectivityState.SHUTDOWN) && t instanceof StatusRuntimeException &&
                    ((StatusRuntimeException) t).getStatus().getCode().equals(Status.UNAVAILABLE.getCode())) {
                //generally a transient issue, retry connection...
                log.info("prob channel {}", channel.hashCode());
                // no retry logic for sample, so create a new channel
                onConnectionDown(nodeId);
            } else if (channelState.equals(ConnectivityState.SHUTDOWN)) {
                log.debug("GRPC channel to node {} is shutdown", nodeId);
            }
        });

    }

    private Map<String, ?> getRetryingServiceConfig() {
        return new Gson()
                .fromJson(
                        new JsonReader(new InputStreamReader(
                                Objects.requireNonNull(this.getClass()
                                        .getResourceAsStream("lr_service_config.json")),
                                UTF_8)),
                        Map.class);
    }

}
