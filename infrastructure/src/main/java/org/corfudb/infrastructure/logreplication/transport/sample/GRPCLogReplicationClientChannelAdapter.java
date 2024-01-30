package org.corfudb.infrastructure.logreplication.transport.sample;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import io.grpc.ConnectivityState;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.corfudb.infrastructure.logreplication.LogReplicationGrpc;
import org.corfudb.infrastructure.logreplication.LogReplicationGrpc.LogReplicationBlockingStub;
import org.corfudb.infrastructure.logreplication.LogReplicationGrpc.LogReplicationStub;
import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.infrastructure.NodeDescriptor;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;
import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.util.NodeLocator;

import javax.annotation.Nonnull;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
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
import java.util.concurrent.TimeoutException;
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

    private static final Map<ManagedChannel, Set<LogReplicationSession>> channelToStreamsMap = new HashMap<>();
    private static final ReentrantLock lock = new ReentrantLock();

    private static final Map<String, Set<ManagedChannel>> nodeIdToChannelMap = new HashMap<>();
    private final Map<LogReplicationSession, LogReplicationBlockingStub> sessionToBlockingStubMap;
    private final Map<LogReplicationSession, LogReplicationStub> sessionToAsyncStubMap;
    private final ExecutorService executorService;

    private final ConcurrentMap<LogReplicationSession, CorfuStreamObserver<ResponseMsg>> responseObserverMap;
    private final ConcurrentMap<Pair<LogReplicationSession, Long>, CorfuStreamObserver<RequestMsg>> replicationReqObserverMap;


    /** A {@link CompletableFuture} which is completed when a connection to a remote leader is set,
     * and  messages can be sent to the remote node.
     */
    @Getter
    volatile CompletableFuture<Void> connectionFuture;

    Context context;

    /** Construct client for accessing LogReplicationService server using the existing channel. */
    public GRPCLogReplicationClientChannelAdapter(
            String localClusterId,
            LogReplicationClientServerRouter router) {
        super(localClusterId, router);

        this.sessionToBlockingStubMap = new HashMap<>();
        this.sessionToAsyncStubMap = new HashMap<>();
        this.executorService = Executors.newSingleThreadExecutor();
        this.connectionFuture = new CompletableFuture<>();
        this.responseObserverMap = new ConcurrentHashMap<>();
        this.replicationReqObserverMap = new ConcurrentHashMap<>();
        context = Context.current();
    }

    @Override
    public void connectAsync(ClusterDescriptor remoteClusterDescriptor, LogReplicationSession session) throws Exception {
        this.executorService.submit(() -> remoteClusterDescriptor.getNodeDescriptors().forEach(node -> {
            try {
                NodeLocator nodeLocator = NodeLocator.parseString(node.getEndpoint());
                ManagedChannel channel;

                lock.lock();
                Pair<Boolean, ManagedChannel> reuseChannel = canReuseChannel(node.getNodeId());
                if(!nodeIdToChannelMap.containsKey(node.getNodeId()) || !reuseChannel.getLeft()) {
                    log.info("GRPC create new channel to node{}@{}:{}", node.getNodeId(), nodeLocator.getHost(), nodeLocator.getPort());
                    channel = NettyChannelBuilder.forAddress(new InetSocketAddress(nodeLocator.getHost(), nodeLocator.getPort()))
                            .usePlaintext()
                            .defaultServiceConfig(getRetryingServiceConfig())
                            .enableRetry()
                            .build();
                    nodeIdToChannelMap.putIfAbsent(node.getNodeId(), new HashSet<>());
                    nodeIdToChannelMap.get(node.getNodeId()).add(channel);
                } else {
                    channel = reuseChannel.getRight();
                }
                channelToStreamsMap.putIfAbsent(channel, new HashSet<>());
                channelToStreamsMap.get(channel).add(session);
                lock.unlock();

                sessionToBlockingStubMap.put(session, LogReplicationGrpc.newBlockingStub(channel));
                sessionToAsyncStubMap.put(session, LogReplicationGrpc.newStub(channel));
                onConnectionUp(node.getNodeId(), session);
            } catch (Exception e) {
                log.error("Error: {} :::: {}", e.getCause(), e.getStackTrace());
                onConnectionDown(node.getNodeId(), session);
            }
        })).get();
    }

    private Pair<Boolean, ManagedChannel> canReuseChannel(String nodeId) {
        if(!nodeIdToChannelMap.isEmpty()) {
            Set<ManagedChannel> channels = nodeIdToChannelMap.get(nodeId);
            if (channels != null && !channels.isEmpty()) {
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
    public void connectAsync(ClusterDescriptor remoteCluster, String nodeId, LogReplicationSession session) throws Exception {
        Optional<String> endpoint = remoteCluster.getNodeDescriptors()
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
                    channel = NettyChannelBuilder.forAddress(new InetSocketAddress(nodeLocator.getHost(), nodeLocator.getPort()))
                            .usePlaintext()
                            .defaultServiceConfig(getRetryingServiceConfig())
                            .enableRetry()
                            .build();
                    nodeIdToChannelMap.putIfAbsent(nodeId, new HashSet<>());
                    nodeIdToChannelMap.get(nodeId).add(channel);
                } else {
                    channel = reuseChannel.getRight();
                }
                channelToStreamsMap.putIfAbsent(channel, new HashSet<>());
                channelToStreamsMap.get(channel).add(session);
                lock.unlock();

                sessionToBlockingStubMap.put(session, LogReplicationGrpc.newBlockingStub(channel));
                sessionToAsyncStubMap.put(session, LogReplicationGrpc.newStub(channel));
                onConnectionUp(nodeId, session);
            } catch (Exception e) {
                log.error("Error: {} :::: {}", e.getCause(), e.getStackTrace());
                onConnectionDown(nodeId, session);
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
            nodeId = this.getRouter().getRemoteLeaderNodeId().get();
        }

        LogReplicationSession sessionMsg = response.getHeader().getSession();

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
                        getRouter().completeExceptionally(sessionMsg, request.getHeader().getRequestId(), e);
                        responseObserverMap.remove(sessionMsg);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Error from request observer", t);
                    long requestId = response.getHeader().getRequestId();
                    getRouter().completeExceptionally(sessionMsg, requestId, t);
                    responseObserverMap.remove(sessionMsg);
                    onServiceUnavailable(t, finalNodeId, sessionMsg);
                    getRouter().inputRemoteSourceLeaderLoss(sessionMsg);
                }

                @Override
                public void onCompleted() {
                    responseObserverMap.remove(sessionMsg);
                }
            };

            if(sessionToAsyncStubMap.containsKey(sessionMsg)) {
                StreamObserver<ResponseMsg> responseObserver = sessionToAsyncStubMap.get(sessionMsg).reverseReplicate(requestObserver);
                responseObserverMap.put(sessionMsg, new CorfuStreamObserver<>(responseObserver));
            } else {
                log.error("No stub found for remote node {}. Message dropped type={}",
                        nodeId, response.getPayload().getPayloadCase());
            }
        }

        responseObserverMap.get(sessionMsg).onNext(response);
    }

    public void processLeadershipLoss(LogReplicationSession session) {
        responseObserverMap.remove(session);
    }

    private void queryLeadership(String nodeId, RequestMsg request) {
        LogReplicationSession session = request.getHeader().getSession();

        // StreamObserver which will observe the async response
        StreamObserver<ResponseMsg> responseObserver = new StreamObserver<ResponseMsg>() {
            @Override
            public void onNext(ResponseMsg responseMsg) {
                log.info("Received leadership response from node {}", nodeId);
                receive(responseMsg);
            }

            @Override
            public void onError(Throwable throwable) {
                log.warn("Error encountered while receiving leadership response msg {}", throwable);
            }

            @Override
            public void onCompleted() {
                log.info("Finished queryLeadership RPC");
            }
        };

        try {
            log.info("queryLeadership for session {}", session);
            if (sessionToAsyncStubMap.containsKey(session)) {
                sessionToAsyncStubMap.get(session).withDeadlineAfter(10, TimeUnit.SECONDS)
                        .queryLeadership(request, responseObserver);
            } else {
                log.warn("Stub not found for session {}. Dropping message of type {}",
                        session, request.getPayload().getPayloadCase());
            }
        } catch (Exception e) {
            log.error("Caught exception while sending message to query leadership status id {} on channel {}",
                    request.getHeader().getRequestId(), nodeIdToChannelMap.get(nodeId).hashCode(), e);
            onServiceUnavailable(e, nodeId, session);
            getRouter().completeExceptionally(session, request.getHeader().getRequestId(), e);
        }
    }

    private void requestMetadata(String nodeId, RequestMsg request) {
        LogReplicationSession session = request.getHeader().getSession();
        try {
            log.info("Metadata request for session {}", session);
            if (sessionToBlockingStubMap.containsKey(session)) {
                ResponseMsg response = sessionToBlockingStubMap.get(session)
                        .withDeadlineAfter(5000, TimeUnit.MILLISECONDS)
                        .withWaitForReady()
                        .negotiate(request);
                receive(response);
            } else {
                log.warn("Stub not found for remote endpoint {}. Dropping message of type {}",
                        nodeId, request.getPayload().getPayloadCase());
            }
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode().equals(Status.DEADLINE_EXCEEDED.getCode())) {
                log.error("DeadLine exceeded for requestMetadata requestID {}", request.getHeader().getRequestId());
                getRouter().completeExceptionally(session, request.getHeader().getRequestId(), new TimeoutException());
            }
        } catch (Exception e) {
            log.error("Caught exception while sending message to query metadata id={} on channel {}",
                    request.getHeader().getRequestId(), nodeIdToChannelMap.get(nodeId).hashCode(), e);
            onServiceUnavailable(e, nodeId, session);
            getRouter().completeExceptionally(session, request.getHeader().getRequestId(), e);
        }
    }

    private void replicate(String nodeId, RequestMsg request) {
        LogReplicationSession sessionMsg = request.getHeader().getSession();
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
                        getRouter().completeExceptionally(sessionMsg, response.getHeader().getRequestId(), e);
                        replicationReqObserverMap.remove(Pair.of(sessionMsg, requestId));
                    }
                }

                @Override
                public void onError(Throwable t) {
                    log.error("Error from response observer", t);
                    long requestId = request.getHeader().getRequestId();
                    onServiceUnavailable(t, nodeId, sessionMsg);
                    getRouter().completeExceptionally(sessionMsg, requestId, t);
                    replicationReqObserverMap.remove(Pair.of(sessionMsg, requestId));
                }

                @Override
                public void onCompleted() {
                    replicationReqObserverMap.remove(Pair.of(sessionMsg, requestId));
                }
            };

            if(sessionToAsyncStubMap.containsKey(sessionMsg)) {
                StreamObserver<RequestMsg> requestObserver = sessionToAsyncStubMap.get(sessionMsg).replicate(responseObserver);
                replicationReqObserverMap.put(Pair.of(sessionMsg, requestId), new CorfuStreamObserver<>(requestObserver));
            } else {
                log.error("No stub found for remote node {}. Message dropped type={}",
                        nodeId, request.getPayload().getPayloadCase());
            }
        }

        log.info("Send replication entry: {} to node {}", request.getHeader().getRequestId(), nodeId);

        // Send log replication entries across channel
        replicationReqObserverMap.get(Pair.of(sessionMsg, requestId)).onNext(request);
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
        responseObserverMap.clear();
        replicationReqObserverMap.clear();
        nodeIdToChannelMap.keySet().forEach(node -> log.debug("Channel is closed for node {}", node));
        lock.unlock();
    }

    @Override
    public void resetRemoteLeader() {
        // No-op
    }

    private synchronized void onServiceUnavailable(Throwable t, String nodeId, LogReplicationSession sesssion) {
        log.info("Service unavailable, clear cached information about connection to node {}", nodeId);
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
                onConnectionDown(nodeId, sesssion);
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