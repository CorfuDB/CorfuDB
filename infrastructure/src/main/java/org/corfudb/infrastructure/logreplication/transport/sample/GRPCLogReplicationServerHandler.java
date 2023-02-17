package org.corfudb.infrastructure.logreplication.transport.sample;

import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.corfudb.infrastructure.logreplication.LogReplicationGrpc;
import org.corfudb.infrastructure.logreplication.LogReplicationGrpc;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSinkServerRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSourceServerRouter;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
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
     * Map of session to SINK Router (internal to Corfu)
     */
    final Map<LogReplicationSession, LogReplicationSinkServerRouter> sessionToSinkRouter;

    /*
     * Map of session to SOURCE Router (internal to Corfu)
     */
    final Map<LogReplicationSession, LogReplicationSourceServerRouter> sessionToSourceServer;

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

    /*
     * Map of session to StreamObserver to send requests to the clients.
     */
    Map<LogReplicationSession, StreamObserver<RequestMsg>> sessionToStreamObserverRequestMap;

    public GRPCLogReplicationServerHandler(Map<LogReplicationSession, LogReplicationSourceServerRouter> sessionToSourceServer,
                                           Map<LogReplicationSession, LogReplicationSinkServerRouter> sessionToSinkRouter) {
        this.sessionToSourceServer = sessionToSourceServer;
        this.sessionToSinkRouter = sessionToSinkRouter;
        this.streamObserverMap = new ConcurrentHashMap<>();
        this.replicationStreamObserverMap = new ConcurrentHashMap<>();
        this.sessionToStreamObserverRequestMap = new ConcurrentHashMap<>();
    }

    @Override
    public void negotiate(RequestMsg request, StreamObserver<ResponseMsg> responseObserver) {
        log.info("Received[{}]: {}", request.getHeader().getRequestId(),
                request.getPayload().getPayloadCase().name());
        LogReplicationSession session = request.getHeader().getSession();
        if (sessionToSinkRouter.containsKey(session)) {
            sessionToSinkRouter.get(session).receive(request);
            streamObserverMap.put(Pair.of(request.getHeader().getClusterId(), request.getHeader().getRequestId()),
                    responseObserver);
        } else {
            log.info("Dropping msg as the cluster is not SINK for the session {}", session);
        }
    }

    @Override
    public void queryLeadership(RequestMsg request, StreamObserver<ResponseMsg> responseObserver) {
        log.info("Received[{}]: {}", request.getHeader().getRequestId(),
                request.getPayload().getPayloadCase().name());

        LogReplicationSession session = request.getHeader().getSession();
        if(sessionToSinkRouter.containsKey(session)) {
            sessionToSinkRouter.get(session).receive(request);
        } else if(sessionToSourceServer.containsKey(session)) {
            sessionToSourceServer.get(session).receive(request);
        } else {
            log.info("Dropping msg as the cluster is not SINK for the session {}", session);
        }
        streamObserverMap.put(Pair.of(request.getHeader().getClusterId(), request.getHeader().getRequestId()),
            responseObserver);
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
                    replicationStreamObserverMap.putIfAbsent(
                        Pair.of(replicationCorfuMessage.getHeader().getClusterId(), requestId), responseObserver);
                } catch (Exception e) {
                    log.error("Exception caught when unpacking log replication entry {}. Skipping message.",
                            requestId, e);
                }

                // Forward the received message to the router
                LogReplicationSession session = replicationCorfuMessage.getHeader().getSession();

                if(sessionToSinkRouter.containsKey(session)) {
                    sessionToSinkRouter.get(session).receive(replicationCorfuMessage);
                } else if(sessionToSourceServer.containsKey(session)) {
                    sessionToSourceServer.get(session).receive(replicationCorfuMessage);
                }
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
    public StreamObserver<ResponseMsg> subscribeAndStartreplication(StreamObserver<RequestMsg> responseObserver) {

        return new StreamObserver<ResponseMsg>() {
            LogReplicationSession session;
            @Override
            public void onNext(ResponseMsg lrResponseMsg) {
                long requestId = lrResponseMsg.getHeader().getRequestId();

                session = lrResponseMsg.getHeader().getSession();

                try {
                    sessionToStreamObserverRequestMap.putIfAbsent(session, responseObserver);
                } catch (Exception e) {
                    log.error("Exception caught when unpacking log replication entry {}. Skipping message.",
                            requestId, e);
                }

                if(sessionToSourceServer.containsKey(session)) {
                    sessionToSourceServer.get(session).receive(lrResponseMsg);
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("Encountered error in the long living subscribe RPC for {}...",session, t);
                if(sessionToSourceServer.containsKey(session)) {
                    sessionToSourceServer.get(session).connectionDown();
                }
            }

            @Override
            public void onCompleted() {
                log.info("Client has completed snapshot replication.");
            }
        };
    }



    public void send(ResponseMsg msg) {
        long requestId = msg.getHeader().getRequestId();
        UuidMsg clusterId = msg.getHeader().getClusterId();

        // Case: message to send is an ACK (async observers)
        if (msg.getPayload().getPayloadCase().equals(ResponsePayloadMsg.PayloadCase.LR_ENTRY_ACK)) {
            try {
                if (!replicationStreamObserverMap.containsKey(Pair.of(clusterId, requestId))) {
                    log.warn("Corfu Message {} has no pending observer. Message {} will not be sent.",
                        msg.getHeader().getRequestId(), msg.getPayload().getPayloadCase().name());
                    return;
                }

                StreamObserver<ResponseMsg> observer = replicationStreamObserverMap.get(Pair.of(clusterId, requestId));
                log.info("Sending[{}:{}]: {}", clusterId, requestId, msg.getPayload().getPayloadCase().name());
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
                return;
            }

            StreamObserver<ResponseMsg> observer = streamObserverMap.get(Pair.of(clusterId, requestId));
            observer.onNext(msg);
            observer.onCompleted();

            // Remove observer as response was already sent
            streamObserverMap.remove(Pair.of(clusterId, requestId));
        }
    }

    public void send(RequestMsg msg) {

        if (msg.getPayload().getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_METADATA_REQUEST) ||
                msg.getPayload().getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_ENTRY)) {
            try {
                LogReplicationSession session = msg.getHeader().getSession();

                if (!sessionToStreamObserverRequestMap.containsKey(session)) {
                    log.warn("Corfu Message {} has no pending observer. Message {} will not be sent.",
                            msg.getHeader().getRequestId(), msg.getPayload().getPayloadCase().name());

                    return;
                }

                StreamObserver<RequestMsg> observer = sessionToStreamObserverRequestMap.get(session);

                observer.onNext(msg);

            } catch (Exception e) {
                log.error("Caught exception while trying to send message {}", msg.getHeader().getRequestId(), e);
            }

        } else {
            log.error("UnExpected request msg {}", msg);
        }
    }

    public void updateRouterInfo(Map<LogReplicationSession, LogReplicationSourceServerRouter> sesionToSourceServerRouter,
                                 Map<LogReplicationSession, LogReplicationSinkServerRouter> sessionToSinkServerRouter) {
        this.sessionToSourceServer.putAll(sesionToSourceServerRouter);
        this.sessionToSinkRouter.putAll(sessionToSinkServerRouter);

    }

}
