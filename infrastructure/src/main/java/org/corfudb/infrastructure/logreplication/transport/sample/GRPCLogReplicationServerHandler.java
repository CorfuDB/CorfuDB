package org.corfudb.infrastructure.logreplication.transport.sample;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.corfudb.infrastructure.logreplication.LogReplicationChannelGrpc;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationServerRouter;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;

import java.util.Map;
import java.util.Objects;
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
     * Map of (Remote Cluster Id, Request Id) pair to Stream Observer to send responses back to the client. Used for
     * blocking calls.
     */
    Map<Pair<UuidMsg, Long>, StreamObserver<ResponseMsg>> streamObserverMap;

    /*
     * Map of (Remote Cluster Id, Sync Request Id) pair to Stream Observer to send responses back to the client. Used
     * for async calls.
     *
     * Note: we cannot rely on the request ID, because for client streaming APIs this will change for each
     * message, despite being part of the same stream.
     */
    Map<Pair<UuidMsg, Long>, StreamObserver<ResponseMsg>> replicationStreamObserverMap;

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
        streamObserverMap.put(Pair.of(request.getHeader().getClusterId(), request.getHeader().getRequestId()),
            responseObserver);
    }

    @Override
    public void queryLeadership(RequestMsg request, StreamObserver<ResponseMsg> responseObserver) {
        log.trace("Received[{}]: {}", request.getHeader().getRequestId(),
                request.getPayload().getPayloadCase().name());
        streamObserverMap.put(Pair.of(request.getHeader().getClusterId(), request.getHeader().getRequestId()),
            responseObserver);
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
                    replicationStreamObserverMap.putIfAbsent(
                        Pair.of(replicationCorfuMessage.getHeader().getClusterId(), requestId), responseObserver);
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
        long requestId = msg.getHeader().getRequestId();
        UuidMsg clusterId = msg.getHeader().getClusterId();

        // Case: message to send is an ACK (async observers)
        if (msg.getPayload().getPayloadCase().equals(ResponsePayloadMsg.PayloadCase.LR_ENTRY_ACK)) {
            try {
                if (!replicationStreamObserverMap.containsKey(
                    Pair.of(clusterId, requestId))) {
                    log.warn("Corfu Message {} has no pending observer. Message {} will not be sent.",
                        msg.getHeader().getRequestId(), msg.getPayload().getPayloadCase().name());
                    log.info("Stream observers in map: {}", replicationStreamObserverMap.keySet());
                    return;
                }

                StreamObserver<ResponseMsg> observer = replicationStreamObserverMap.get(Pair.of(clusterId, requestId));
                log.info("Sending[{}:{}]: {}", clusterId, requestId,
                    msg.getPayload().getPayloadCase().name());
                observer.onNext(msg);
                observer.onCompleted();

                // Remove observer as response was already sent.
                // Since we send summarized ACKs (to avoid memory leaks) remove all observers lower or equal than
                // the one for which a response is being sent.
                replicationStreamObserverMap.keySet().removeIf(id ->
                    id.getRight() <= requestId &&
                    Objects.equals(id.getLeft(), clusterId));
            } catch (Exception e) {
                log.error("Caught exception while trying to send message {}", msg.getHeader().getRequestId(), e);
            }

        } else {

            if (!streamObserverMap.containsKey(Pair.of(clusterId, requestId))) {
                log.warn("Corfu Message {} has no pending observer. Message {} will not be sent.",
                    requestId, msg.getPayload().getPayloadCase().name());
                log.info("Stream observers in map: {}", streamObserverMap.keySet());
                return;
            }

            StreamObserver<ResponseMsg> observer = streamObserverMap.get(Pair.of(clusterId, requestId));
            log.info("Sending[{}]: {}", requestId, msg.getPayload().getPayloadCase().name());
            observer.onNext(msg);
            observer.onCompleted();

            // Remove observer as response was already sent
            streamObserverMap.remove(Pair.of(clusterId, requestId));
        }
    }

}
