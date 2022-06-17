package org.corfudb.infrastructure.logreplication.transport.sample;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationChannelGrpc;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationServerRouter;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;

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
    Map<Long, StreamObserver<ResponseMsg>> streamObserverMap;

    /*
     * Map of Sync Request ID to Stream Observer to send responses back to the client. Used for async calls.
     *
     * Note: we cannot rely on the request ID, because for client streaming APIs this will change for each
     * message, despite being part of the same stream.
     */
    Map<Long, StreamObserver<ResponseMsg>> replicationStreamObserverMap;

    public GRPCLogReplicationServerHandler(LogReplicationServerRouter router) {
        this.router = router;
        this.streamObserverMap = new ConcurrentHashMap<>();
        this.replicationStreamObserverMap = new ConcurrentHashMap<>();
    }

    @Override
    public void negotiate(RequestMsg request, StreamObserver<ResponseMsg> responseObserver) {
        log.trace("Received[{}]: {}", request.getHeader().getRequestId(),
                request.getPayload().getPayloadCase().name());
        router.receive(request);
        streamObserverMap.put(request.getHeader().getRequestId(), responseObserver);
    }

    @Override
    public void queryLeadership(RequestMsg request, StreamObserver<ResponseMsg> responseObserver) {
        log.trace("Received[{}]: {}", request.getHeader().getRequestId(),
                request.getPayload().getPayloadCase().name());
        streamObserverMap.put(request.getHeader().getRequestId(), responseObserver);
        router.receive(request);
    }

    @Override
    public StreamObserver<RequestMsg> replicate(StreamObserver<ResponseMsg> responseObserver) {

        return new StreamObserver<RequestMsg>() {
            @Override
            public void onNext(RequestMsg replicationCorfuMessage) {
                long requestId = replicationCorfuMessage.getHeader().getRequestId();
                String name = replicationCorfuMessage.getPayload().getPayloadCase().name();
                log.trace("Received[{}]: {}", requestId, name);

                // Register at the observable first.
                try {
                    replicationStreamObserverMap.putIfAbsent(requestId, responseObserver);
                } catch (Exception e) {
                    log.error("Exception caught when unpacking log replication entry {}. Skipping message.",
                            requestId, e);
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

    public void send(ResponseMsg msg) {
        // Case: message to send is an ACK (async observers)
        if (msg.getPayload().getPayloadCase().equals(ResponsePayloadMsg.PayloadCase.LR_ENTRY_ACK)) {
            try {
                long requestId = msg.getHeader().getRequestId();

                if (!replicationStreamObserverMap.containsKey(requestId)) {
                    log.warn("Corfu Message {} has no pending observer. Message {} will not be sent.",
                            msg.getHeader().getRequestId(), msg.getPayload().getPayloadCase().name());
                    log.info("Stream observers in map: {}", replicationStreamObserverMap.keySet());
                    return;
                }

                StreamObserver<ResponseMsg> observer = replicationStreamObserverMap.get(requestId);
                log.info("Sending[{}]: {}", requestId, msg.getPayload().getPayloadCase().name());
                observer.onNext(msg);
                observer.onCompleted();

                // Remove observer as response was already sent
                // Since we send summarized ACKs (to avoid memory leaks) remove all observers lower or equal than
                // the one for which a response is being sent.
                replicationStreamObserverMap.keySet().removeIf(id -> id <= requestId);
            } catch (Exception e) {
                log.error("Caught exception while trying to send message {}", msg.getHeader().getRequestId(), e);
            }

        } else {

            if (!streamObserverMap.containsKey(msg.getHeader().getRequestId())) {
                log.warn("Corfu Message {} has no pending observer. Message {} will not be sent.",
                        msg.getHeader().getRequestId(), msg.getPayload().getPayloadCase().name());
                log.info("Stream observers in map: {}", streamObserverMap.keySet());
                return;
            }

            StreamObserver<ResponseMsg> observer = streamObserverMap.get(msg.getHeader().getRequestId());
            log.info("Sending[{}]: {}", msg.getHeader().getRequestId(), msg.getPayload().getPayloadCase().name());
            observer.onNext(msg);
            observer.onCompleted();

            // Remove observer as response was already sent
            streamObserverMap.remove(msg.getHeader().getRequestId());
        }
    }

}
