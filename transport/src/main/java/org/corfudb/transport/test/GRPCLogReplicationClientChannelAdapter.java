package org.corfudb.transport.test;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.transport.logreplication.LogReplicationClientRouter;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.runtime.LogReplicationChannelGrpc;
import org.corfudb.transport.client.IClientChannelAdapter;

/**
 * This is a default implementation of a custom channel for Log Replication Servers inter-communication
 * which relies on the standard GRPC.
 *
 * It is used for testing purposes.
 *
 */
@Slf4j
public class GRPCLogReplicationClientChannelAdapter extends IClientChannelAdapter {

    private final Channel channel;
    private final LogReplicationChannelGrpc.LogReplicationChannelBlockingStub blockingStub;
    private final LogReplicationChannelGrpc.LogReplicationChannelStub asyncStub;

    private StreamObserver<CorfuMessage> requestObserver;
    private StreamObserver<CorfuMessage> responseObserver;

    /** Construct client for accessing LogReplicationService server using the existing channel. */
    public GRPCLogReplicationClientChannelAdapter(Integer port, String host, String localSiteId, LogReplicationClientRouter adapter) {
        super(port, host, localSiteId, adapter);
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        blockingStub = LogReplicationChannelGrpc.newBlockingStub(channel);
        asyncStub = LogReplicationChannelGrpc.newStub(channel);
    }

    @Override
    public void send(String remoteSiteId, CorfuMessage msg) {

        CorfuMessage response = null;

        switch (msg.getType()) {
            case LOG_REPLICATION_ENTRY:
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
                            log.error("Completed");
                            requestObserver = null;
                        }
                    };

                    log.info("Initiate stub for replication");
                    requestObserver = asyncStub.replicate(responseObserver);
                }

                log.info("Send replication entry: {}", msg.getRequestID());
                if (responseObserver != null) {
                    // Send log replication entries across channel
                    requestObserver.onNext(msg);
                }
                break;
            case LOG_REPLICATION_NEGOTIATION_REQUEST:
                try {
                    response = blockingStub.withWaitForReady().negotiate(msg);
                    getRouter().receive(response);
                } catch (Exception e) {
                    log.error("Caught exception while sending message to query leadership status id {}", msg.getRequestID(), e);
                    getRouter().completeExceptionally(msg.getRequestID(), e);
                }
                break;
            case LOG_REPLICATION_QUERY_LEADERSHIP:
                try {
                    response = blockingStub.withWaitForReady().queryLeadership(msg);
                    getRouter().receive(response);
                } catch (Exception e) {
                    log.error("Caught exception while sending message to query leadership status id {}", msg.getRequestID(), e);
                    getRouter().completeExceptionally(msg.getRequestID(), e);
                }
                break;
             default:
                 break;
        }

        if (response != null) {
            log.info("Received[{}] response {}", response.getRequestID(), response.getType().name());
        }
    }

}
