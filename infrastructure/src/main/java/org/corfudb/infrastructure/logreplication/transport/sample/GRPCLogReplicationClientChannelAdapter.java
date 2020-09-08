package org.corfudb.infrastructure.logreplication.transport.sample;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.logreplication.infrastructure.ClusterDescriptor;
import org.corfudb.infrastructure.logreplication.LogReplicationChannelGrpc;
import org.corfudb.infrastructure.logreplication.LogReplicationChannelGrpc.LogReplicationChannelStub;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientRouter;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.infrastructure.logreplication.LogReplicationChannelGrpc.LogReplicationChannelBlockingStub;
import org.corfudb.infrastructure.logreplication.transport.client.IClientChannelAdapter;
import org.corfudb.util.NodeLocator;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

    private ExecutorService executorService;

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
                log.info("GRPC create connection to {}:{}", nodeLocator.getHost(), nodeLocator.getPort());
                ManagedChannel channel = ManagedChannelBuilder.forAddress(nodeLocator.getHost(), nodeLocator.getPort())
                        .usePlaintext()
                        .build();
                channelMap.put(node.getEndpoint(), channel);
                blockingStubMap.put(node.getEndpoint(), LogReplicationChannelGrpc.newBlockingStub(channel));
                asyncStubMap.put(node.getEndpoint(), LogReplicationChannelGrpc.newStub(channel));
                onConnectionUp(node.getEndpoint());
            } catch (Exception e) {
                onConnectionDown(node.getEndpoint());
            }
        }));
    }

    @Override
    public void connectAsync(String endpoint) {
        this.executorService.submit(() -> {
            try {
                NodeLocator nodeLocator = NodeLocator.parseString(endpoint);
                log.info("GRPC create connection to {}:{}", nodeLocator.getHost(), nodeLocator.getPort());
                ManagedChannel channel = ManagedChannelBuilder.forAddress(nodeLocator.getHost(), nodeLocator.getPort()).usePlaintext().build();
                channelMap.put(endpoint, channel);
                blockingStubMap.put(endpoint, LogReplicationChannelGrpc.newBlockingStub(channel));
                asyncStubMap.put(endpoint, LogReplicationChannelGrpc.newStub(channel));
                onConnectionUp(endpoint);
            } catch (Exception e) {
                onConnectionDown(endpoint);
            }
        });
    }

    @Override
    public void send(String endpoint, CorfuMessage msg) {
        // Check the connection future. If connected, continue with sending the message.
        // If timed out, return a exceptionally completed with the timeout.
        switch (msg.getType()) {
            case LOG_REPLICATION_ENTRY:
                replicate(endpoint, msg);
                break;
            case LOG_REPLICATION_QUERY_LEADERSHIP:
                queryLeadership(endpoint, msg);
                break;
            case LOG_REPLICATION_METADATA_REQUEST:
                requestMetadata(endpoint, msg);
                break;
            default:
                break;
        }
    }

    private void queryLeadership(String endpoint, CorfuMessage msg) {
        try {
            if(blockingStubMap.containsKey(endpoint)) {
                CorfuMessage response = blockingStubMap.get(endpoint).withWaitForReady().queryLeadership(msg);
                receive(response);
            } else {
                log.warn("Stub not found for remote endpoint {}. Dropping message of type {}", endpoint, msg.getType());
            }
        } catch (Exception e) {
            log.error("Caught exception while sending message to query leadership status id {}", msg.getRequestID(), e);
            getRouter().completeExceptionally(msg.getRequestID(), e);
        }
    }

    private void requestMetadata(String endpoint, CorfuMessage msg) {
        try {
            if(blockingStubMap.containsKey(endpoint)) {
                CorfuMessage response = blockingStubMap.get(endpoint).withWaitForReady().negotiate(msg);
                receive(response);
            } else {
                log.warn("Stub not found for remote endpoint {}. Dropping message of type {}", endpoint, msg.getType());
            }
        } catch (Exception e) {
            log.error("Caught exception while sending message to query metadata id={}", msg.getRequestID(), e);
            getRouter().completeExceptionally(msg.getRequestID(), e);
        }
    }

    private void replicate(String endpoint, CorfuMessage msg) {
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

            if(asyncStubMap.containsKey(endpoint)) {
                requestObserver = asyncStubMap.get(endpoint).replicate(responseObserver);
            } else {
                log.error("No stub found for remote endpoint {}. Message dropped type={}", endpoint, msg.getType());
            }
        }

        log.info("Send replication entry: {}", msg.getRequestID());
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
