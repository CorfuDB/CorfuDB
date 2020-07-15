package org.corfudb.infrastructure.logreplication.transport.sample;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationServerRouter;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.runtime.Messages;
import org.corfudb.runtime.Messages.CorfuMessage;
import org.corfudb.infrastructure.logreplication.LogReplicationChannelGrpc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * GRPC Log Replication Service Stub Implementation.
 *
 * Note: GRPC is used to channel the plugin-based transport architecture for log replication.
 *
 * @author annym 05/15/20
 */
@Slf4j
public class GRPCLogReplicationServerHandler extends LogReplicationChannelGrpc.LogReplicationChannelImplBase {

    /*
     * Corfu Message Router (internal to Corfu)
     */
    LogReplicationServerRouter router;

    /*
     * Map of Request ID to Stream Observer to send responses back to the client. Used for blocking calls.
     */
    Map<Long, StreamObserver<CorfuMessage>> streamObserverMap;

    /*
     * Map of Sync Request ID to Stream Observer to send responses back to the client. Used for async calls.
     *
     * Note: we cannot rely on the request ID, because for client streaming APIs this will change for each
     * message, despite being part of the same stream.
     */
    Map<Long, StreamObserver<CorfuMessage>> replicationStreamObserverMap;

    public GRPCLogReplicationServerHandler(LogReplicationServerRouter router) {
        this.router = router;
        this.streamObserverMap = new ConcurrentHashMap<>();
        this.replicationStreamObserverMap = new ConcurrentHashMap<>();
    }

    @Override
    public void negotiate(CorfuMessage request, StreamObserver<CorfuMessage> responseObserver) {
        log.trace("Received[{}]: {}", request.getRequestID(), request.getType().name());
        router.receive(request);
        streamObserverMap.put(request.getRequestID(), responseObserver);
    }

    @Override
    public void queryLeadership(CorfuMessage request, StreamObserver<CorfuMessage> responseObserver) {
        log.info("Received[{}]: {}", request.getRequestID(), request.getType().name());
        streamObserverMap.put(request.getRequestID(), responseObserver);
        router.receive(request);
    }

    @Override
    public StreamObserver<CorfuMessage> replicate(StreamObserver<CorfuMessage> responseObserver) {

        return new StreamObserver<CorfuMessage>() {
            @Override
            public void onNext(CorfuMessage replicationCorfuMessage) {
                log.trace("Received[{}]: {}", replicationCorfuMessage.getRequestID(), replicationCorfuMessage.getType().name());

                // Register at the observable first.
                try {
                    replicationStreamObserverMap.putIfAbsent(replicationCorfuMessage.getRequestID(), responseObserver);
                } catch (Exception e) {
                    log.error("Exception caught when unpacking log replication entry {}. Skipping message.",
                            replicationCorfuMessage.getRequestID(), e);
                }

                // Forward the received message to the router
                router.receive(replicationCorfuMessage);
            }

            @Override
            public void onError(Throwable t) {
                log.error("Encountered error while attempting replication.", t);
            }

            @Override
            public void onCompleted() {
                log.trace("Client has completed snapshot replication.");
            }
        };
    }

    public void send(CorfuMessage msg) {
        // Case: message to send is an ACK (async observers)
        if (msg.getType().equals(Messages.CorfuMessageType.LOG_REPLICATION_ENTRY)) {
            try {
                long requestId = msg.getRequestID();

                if (!replicationStreamObserverMap.containsKey(msg.getRequestID())) {
                    log.warn("Corfu Message {} has no pending observer. Message {} will not be sent.", msg.getRequestID(), msg.getType().name());
                    log.info("Stream observers in map: {}", replicationStreamObserverMap.keySet());
                    return;
                }

                StreamObserver<CorfuMessage> observer = replicationStreamObserverMap.get(requestId);
                log.info("Sending[{}]: {}", msg.getRequestID(), msg.getType().name());
                observer.onNext(msg);
                observer.onCompleted();

                // Remove observer as response was already sent
                // Since we send summarized ACKs (to avoid memory leaks) remove all observers lower or equal than
                // the one for which a response is being sent.
                replicationStreamObserverMap.keySet().removeIf(id -> id <= requestId);
            } catch (Exception e) {
                log.error("Caught an exception while trying to send message {} ", msg.getRequestID(), e);
            }

        } else {

            if (!streamObserverMap.containsKey(msg.getRequestID())) {
                log.warn("Corfu Message {} has no pending observer. Message {} will not be sent.", msg.getRequestID(), msg.getType().name());
                log.info("Stream observers in map: {}", streamObserverMap.keySet());
                return;
            }

            StreamObserver<CorfuMessage> observer = streamObserverMap.get(msg.getRequestID());
            log.info("Sending[{}]: {}", msg.getRequestID(), msg.getType().name());
            observer.onNext(msg);
            observer.onCompleted();

            // Remove observer as response was already sent
            streamObserverMap.remove(msg.getRequestID());
        }
    }

}
