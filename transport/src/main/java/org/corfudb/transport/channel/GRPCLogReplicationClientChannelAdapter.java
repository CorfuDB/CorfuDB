package org.corfudb.transport.channel;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.logreplication.cluster.ClusterDescriptor;
import org.corfudb.runtime.LogReplicationChannelGrpc;
import org.corfudb.runtime.LogReplicationChannelGrpc.LogReplicationChannelStub;
import org.corfudb.transport.logreplication.LogReplicationClientRouter;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.runtime.LogReplicationChannelGrpc.LogReplicationChannelBlockingStub;
import org.corfudb.transport.client.IClientChannelAdapter;
import org.corfudb.util.NodeLocator;

import java.util.HashMap;
import java.util.Map;
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

    /** Construct client for accessing LogReplicationService server using the existing channel. */
    public GRPCLogReplicationClientChannelAdapter(ClusterDescriptor clusterDescriptor, LogReplicationClientRouter adapter) {
        super(clusterDescriptor, adapter);

        this.channelMap = new HashMap<>();
        this.blockingStubMap = new HashMap<>();
        this.asyncStubMap = new HashMap<>();

        clusterDescriptor.getNodesDescriptors().forEach(node -> {
            NodeLocator nodeLocator = NodeLocator.parseString(node.getEndpoint());
            log.info("GRPC create connection to {}:{}", nodeLocator.getHost(), nodeLocator.getPort());
            ManagedChannel channel = ManagedChannelBuilder.forAddress(nodeLocator.getHost(), nodeLocator.getPort()).usePlaintext().build();
            channelMap.put(node.getEndpoint(), channel);
            blockingStubMap.put(node.getEndpoint(), LogReplicationChannelGrpc.newBlockingStub(channel));
            asyncStubMap.put(node.getEndpoint(), LogReplicationChannelGrpc.newStub(channel));
        });
    }

    @Override
    public void send(CorfuMessage msg) {
        send(getRemoteLeader(), msg);
    }

    @Override
    public void send(String endpoint, CorfuMessage msg) {

        switch (msg.getType()) {
            case LOG_REPLICATION_ENTRY:
                replicate(endpoint, msg);
                break;
            case LOG_REPLICATION_QUERY_LEADERSHIP:
                queryLeadership(endpoint, msg);
                break;
            case LOG_REPLICATION_NEGOTIATION_REQUEST:
                negotiate(endpoint, msg);
                break;
            default:
                break;
        }
    }

    private void queryLeadership(String endpoint, CorfuMessage msg) {
        try {
            if(blockingStubMap.containsKey(endpoint)) {
                CorfuMessage response = blockingStubMap.get(endpoint).withWaitForReady().queryLeadership(msg);
                getRouter().receive(response);
            } else {
                log.warn("Stub not found for remote endpoint {}. Dropping message of type {}", endpoint, msg.getType());
            }
        } catch (Exception e) {
            log.error("Caught exception while sending message to query leadership status id {}", msg.getRequestID(), e);
            getRouter().completeExceptionally(msg.getRequestID(), e);
        }
    }

    private void negotiate(String endpoint, CorfuMessage msg) {
        try {
            if(blockingStubMap.containsKey(endpoint)) {
                CorfuMessage response = blockingStubMap.get(endpoint).withWaitForReady().negotiate(msg);
                getRouter().receive(response);
            } else {
                log.warn("Stub not found for remote endpoint {}. Dropping message of type {}", endpoint, msg.getType());
            }
        } catch (Exception e) {
            log.error("Caught exception while sending message to query leadership status id {}", msg.getRequestID(), e);
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
                        getRouter().receive(response);
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
