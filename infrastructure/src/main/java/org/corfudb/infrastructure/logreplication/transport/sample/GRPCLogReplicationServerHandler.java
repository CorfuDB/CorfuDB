package org.corfudb.infrastructure.logreplication.transport.sample;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.corfudb.infrastructure.logreplication.LogReplicationGrpc;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientServerRouter;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.proto.service.CorfuMessage;
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
public class GRPCLogReplicationServerHandler extends LogReplicationGrpc.LogReplicationImplBase {


    /*
     * LogReplication router that handles the incoming/outgoing messages
     */
    final LogReplicationClientServerRouter router;

    /*
     * Map of (Remote Cluster Id, Request Id) pair to Stream Observer to send responses back to the client. Used for
     * unary RPCs.
     */
    Map<Pair<LogReplicationSession, Long>, CorfuStreamObserver<ResponseMsg>> unaryCallStreamObserverMap;

    /*
     * Used when Source is the connection starter.
     * Map of session to Stream Observer to send responses back to the client. Used for async calls.
     *
     */
    Map<Pair<LogReplicationSession, Long>, CorfuStreamObserver<ResponseMsg>>  replicationStreamObserverMap;

    /*
     * Used when Sink is the connection starter. Source drives the replication using the streamObserver.
     * Map of session to StreamObserver to send requests to the clients.
     *
     */
    Map<LogReplicationSession, CorfuStreamObserver<RequestMsg>> reverseReplicationStreamObserverMap;

    public GRPCLogReplicationServerHandler(LogReplicationClientServerRouter router) {
        this.router = router;
        this.unaryCallStreamObserverMap = new ConcurrentHashMap<>();
        this.replicationStreamObserverMap = new ConcurrentHashMap<>();
        this.reverseReplicationStreamObserverMap = new ConcurrentHashMap<>();
    }

    @Override
    public void negotiate(RequestMsg request, StreamObserver<ResponseMsg> responseObserver) {
        log.info("Received[{}]: {}", request.getHeader().getRequestId(),
                request.getPayload().getPayloadCase().name());
        unaryCallStreamObserverMap.put(Pair.of(request.getHeader().getSession(), request.getHeader().getRequestId()),
                new CorfuStreamObserver<>(responseObserver));
        router.receive(request);
    }

    @Override
    public void queryLeadership(RequestMsg request, StreamObserver<ResponseMsg> responseObserver) {
        log.info("Received[{}]: {}", request.getHeader().getRequestId(),
                request.getPayload().getPayloadCase().name());
        unaryCallStreamObserverMap.put(Pair.of(request.getHeader().getSession(), request.getHeader().getRequestId()),
            new CorfuStreamObserver<>(responseObserver));
        router.receive(request);
    }

    @Override
    public StreamObserver<RequestMsg> replicate(StreamObserver<ResponseMsg> responseObserver) {

        return new StreamObserver<RequestMsg>() {
            @Override
            public void onNext(RequestMsg replicationCorfuMessage) {
                long requestId = replicationCorfuMessage.getHeader().getRequestId();
                String name = replicationCorfuMessage.getPayload().getPayloadCase().name();
                log.info("Received[{}]: {}", requestId, name);

                // Register at the observable first.
                try {
                    replicationStreamObserverMap.putIfAbsent(Pair.of(replicationCorfuMessage.getHeader().getSession(), requestId),
                            new CorfuStreamObserver<>(responseObserver));
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

    @Override
    public StreamObserver<ResponseMsg> reverseReplicate(StreamObserver<RequestMsg> responseObserver) {

        return new StreamObserver<ResponseMsg>() {
            LogReplicationSession session;
            @Override
            public void onNext(ResponseMsg lrResponseMsg) {
                long requestId = lrResponseMsg.getHeader().getRequestId();

                session = lrResponseMsg.getHeader().getSession();

                try {
                    reverseReplicationStreamObserverMap.putIfAbsent(session, new CorfuStreamObserver<>(responseObserver));
                } catch (Exception e) {
                    log.error("Exception caught when unpacking log replication entry {}. Skipping message.",
                            requestId, e);
                }

                router.receive(lrResponseMsg);
            }

            @Override
            public void onError(Throwable t) {
                log.error("Encountered error in the long living reverse replicate RPC for {}...", session, t);
                reverseReplicationStreamObserverMap.remove(session);
                router.onConnectionDown(session);
            }

            @Override
            public void onCompleted() {
                log.info("Client has completed snapshot replication.");
            }
        };
    }



    public void send(ResponseMsg msg) {
        long requestId = msg.getHeader().getRequestId();
        LogReplicationSession session = msg.getHeader().getSession();

        // Case: message to send is an ACK (async observers)
        if (msg.getPayload().getPayloadCase().equals(ResponsePayloadMsg.PayloadCase.LR_ENTRY_ACK)) {
            try {
                if (!replicationStreamObserverMap.containsKey(Pair.of(session, requestId))) {
                    log.warn("Corfu Message {} has no pending observer. Message {} will not be sent.",
                        msg.getHeader().getRequestId(), msg.getPayload().getPayloadCase().name());
                    return;
                }

                StreamObserver<ResponseMsg> observer = replicationStreamObserverMap.get(Pair.of(session, requestId));
                log.trace("Sending[{}:{}]: {}", session, requestId, msg.getPayload().getPayloadCase().name());
                observer.onNext(msg);
                observer.onCompleted();

                // Remove observer as response was already sent.
                // Since we send summarized ACKs (to avoid memory leaks) remove all observers lower or equal than
                // the one for which a response is being sent.
                replicationStreamObserverMap.keySet().removeIf(id ->
                        id.getRight() <= requestId &&
                                Objects.equals(id.getLeft(), session));
            } catch (Exception e) {
                log.error("Caught exception while trying to send message {}", msg.getHeader().getRequestId(), e);
            }

        } else {

            if (!unaryCallStreamObserverMap.containsKey(Pair.of(session, requestId))) {
                log.warn("Corfu Message {} has no pending observer. Message {} will not be sent.",
                    requestId, msg.getPayload().getPayloadCase().name());
                return;
            }

            CorfuStreamObserver<ResponseMsg> observer = unaryCallStreamObserverMap.get(Pair.of(session, requestId));
            observer.onNext(msg);
            observer.onCompleted();

            // Remove observer as response was already sent
            unaryCallStreamObserverMap.remove(Pair.of(session, requestId));
        }
    }

    public void send(RequestMsg msg) {

        if (msg.getPayload().getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_METADATA_REQUEST) ||
                msg.getPayload().getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_ENTRY)) {
            try {
                LogReplicationSession session = msg.getHeader().getSession();

                if (!reverseReplicationStreamObserverMap.containsKey(session)) {
                    log.warn("Corfu Message {} has no pending observer. Message {} will not be sent.",
                            msg.getHeader().getRequestId(), msg.getPayload().getPayloadCase().name());

                    return;
                }

                CorfuStreamObserver<RequestMsg> observer = reverseReplicationStreamObserverMap.get(session);
                observer.onNext(msg);

            } catch(StatusRuntimeException e) {
                if (e.getStatus().getCode().equals(Status.Code.CANCELLED)) {
                    log.error("StreamObserver is cancelled for session {} with exception", msg.getHeader().getSession(), e);
                    router.onConnectionDown(msg.getHeader().getSession());
                }
            } catch (Exception e) {
                log.error("Caught exception while trying to send message {}", msg.getHeader().getRequestId(), e);
            }

        } else {
            log.error("UnExpected request msg {}", msg);
        }

    }

}
