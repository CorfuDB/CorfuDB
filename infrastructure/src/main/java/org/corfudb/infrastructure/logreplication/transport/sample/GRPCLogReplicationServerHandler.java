package org.corfudb.infrastructure.logreplication.transport.sample;

import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.corfudb.infrastructure.logreplication.LogReplicationChannelGrpc;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSubscriber;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSinkServerRouter;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationSourceServerRouter;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_ENTRY_ACK;
import static org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_METADATA_REQUEST;
import static org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_METADATA_RESPONSE;
import static org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_SUBSCRIBE_REQUEST;

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
     * Map of session to SINK Router (internal to Corfu)
     */
    final Map<ReplicationSession, LogReplicationSinkServerRouter> sessionToSinkServer;

    /*
     * Map of session to SOURCE Router (internal to Corfu)
     */
    final Map<ReplicationSession, LogReplicationSourceServerRouter> sessionToSourceServer;

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

    Map<ReplicationSession, StreamObserver<RequestMsg>> sessionToStreamObserverRequestMap;

    public GRPCLogReplicationServerHandler(Map<ReplicationSession, LogReplicationSourceServerRouter> sessionToSourceServer,
                                           Map<ReplicationSession, LogReplicationSinkServerRouter> sessionToSinkServer) {
        this.sessionToSourceServer = sessionToSourceServer;
        this.sessionToSinkServer = sessionToSinkServer;
        this.streamObserverMap = new ConcurrentHashMap<>();
        this.replicationStreamObserverMap = new ConcurrentHashMap<>();
        this.sessionToStreamObserverRequestMap = new ConcurrentHashMap<>();
    }

    @Override
    public void negotiate(RequestMsg request, StreamObserver<ResponseMsg> responseObserver) {
        log.info("Received[{}]: {}", request.getHeader().getRequestId(),
                request.getPayload().getPayloadCase().name());
        ReplicationSession session = convertSessionMsg(request, null);
        if (sessionToSinkServer.containsKey(session)) {
            sessionToSinkServer.get(session).receive(request);
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

        ReplicationSession session = convertSessionMsg(request, null);
        log.info("Received: {}", request);
//        log.info("sessionToSinkServer {}", sessionToSinkServer);
//        log.info("sessionToSourceServer {}", sessionToSourceServer);
//        log.info("session {}",session);
        if(sessionToSinkServer.containsKey(session)) {
            sessionToSinkServer.get(session).receive(request);
        } else if(sessionToSourceServer.containsKey(session)) {
            sessionToSourceServer.get(session).receive(request);
        }
        streamObserverMap.put(Pair.of(request.getHeader().getClusterId(), request.getHeader().getRequestId()),
            responseObserver);

//        log.info("in LRS, send. streamObserverMap hash {}", streamObserverMap.hashCode());
//        log.info("requestID {}", request.getHeader().getRequestId());
//        log.info("in LRS, send. streamObserverMap {}", streamObserverMap);
    }

    @Override
    public StreamObserver<RequestMsg> replicate(StreamObserver<ResponseMsg> responseObserver) {

        return new StreamObserver<RequestMsg>() {
            @Override
            public void onNext(RequestMsg replicationCorfuMessage) {
                long requestId = replicationCorfuMessage.getHeader().getRequestId();
                String name = replicationCorfuMessage.getPayload().getPayloadCase().name();
                log.info("Received[{}]: {}", requestId, name);

//                log.info("#114 the request is {}", replicationCorfuMessage);
                // Register at the observable first.
                try {
                    replicationStreamObserverMap.putIfAbsent(
                        Pair.of(replicationCorfuMessage.getHeader().getClusterId(), requestId), responseObserver);
                } catch (Exception e) {
                    log.error("Exception caught when unpacking log replication entry {}. Skipping message.",
                            requestId, e);
                }

                // Forward the received message to the router
                ReplicationSession session = convertSessionMsg(replicationCorfuMessage, null);
//                log.info("lr entry session: {}", session);
                if(sessionToSinkServer.containsKey(session)) {
                    sessionToSinkServer.get(session).receive(replicationCorfuMessage);
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
            @Override
            public void onNext(ResponseMsg lrResponseMsg) {
                long requestId = lrResponseMsg.getHeader().getRequestId();
                String name = lrResponseMsg.getPayload().getPayloadCase().name();
//                log.info("#159 Received[{}]: {}", requestId, name);

//                log.info("#161 received is {}", lrResponseMsg);

                ReplicationSession sessionFromSink = convertSessionMsg(null, lrResponseMsg);
                ReplicationSession session = new ReplicationSession(sessionFromSink.getLocalClusterId(),
                        sessionFromSink.getRemoteClusterId(), sessionFromSink.getSubscriber());

                // For ACKs, the SINK copies the metadata of the lr_msg. Hence convert the incoming session to reflect session as seen by SINK
//                if(lrResponseMsg.getPayload().getPayloadCase().equals(LR_ENTRY_ACK)) {
//                    ReplicationSession ackSession =
//                    session = ackSession;
//                }
                // create an outgoing session from incoming session. This will be used to query the streamObserver in the BiDirectional Stream.
                ReplicationSession observerSession =  new ReplicationSession(session.getLocalClusterId(),
                        session.getRemoteClusterId(), session.getSubscriber());


                try {
//                    log.info("the senderClusterId  is {}, observerSession:: {}", observerSession.getRemoteClusterId(), observerSession);
                    sessionToStreamObserverRequestMap.putIfAbsent(observerSession, responseObserver);
//                    log.info("after putting in key: {}, value: {}, sessionToStreamObserverRequestMap {}", observerSession,
//                            responseObserver, sessionToStreamObserverRequestMap);
                } catch (Exception e) {
                    log.error("Exception caught when unpacking log replication entry {}. Skipping message.",
                            requestId, e);
                }

                // Forward the received message to the router
//                log.info("lr entry session: {}", observerSession);
//                log.info("looking for: {}", session);
//                log.info("sessionToSourceServer {}", sessionToSourceServer);
//                log.info("sessionToSinkServer {}", sessionToSinkServer);
                if(sessionToSourceServer.containsKey(session)) {
                    sessionToSourceServer.get(session).receive(lrResponseMsg);
                }
            }

            @Override
            public void onError(Throwable t) {
                log.error("Encountered error while attempting replication.", t);
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
//                log.info("Shama: in send response {}", msg);
                if (!replicationStreamObserverMap.containsKey(Pair.of(clusterId, requestId))) {
                    log.warn("Corfu Message {} has no pending observer. Message {} will not be sent.",
                        msg.getHeader().getRequestId(), msg.getPayload().getPayloadCase().name());
//                    log.info("Stream observers in map: {}", replicationStreamObserverMap.keySet());
                    return;
                }

                StreamObserver<ResponseMsg> observer = replicationStreamObserverMap.get(Pair.of(clusterId, requestId));
                log.info("Sending[{}:{}]: {}", clusterId, requestId, msg.getPayload().getPayloadCase().name());
                log.info("requestID {}", requestId);
                log.info("replicationStreamObserverMap {}", replicationStreamObserverMap);
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
//                log.info("Stream observers in map: {}", streamObserverMap.keySet());
                return;
            }

            StreamObserver<ResponseMsg> observer = streamObserverMap.get(Pair.of(clusterId, requestId));
//            log.info("#255 Sending[{}]: {}", requestId, msg.getPayload().getPayloadCase().name());
            observer.onNext(msg);
            observer.onCompleted();

            // Remove observer as response was already sent
            streamObserverMap.remove(Pair.of(clusterId, requestId));
        }
    }

    public void send(RequestMsg msg) {
        long requestId = msg.getHeader().getRequestId();
        UuidMsg clusterId = msg.getHeader().getClusterId();
        log.info("#267 requestId {}", requestId);
        // Case: message to send is an ACK (async observers)
        if (msg.getPayload().getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_METADATA_REQUEST) ||
                msg.getPayload().getPayloadCase().equals(CorfuMessage.RequestPayloadMsg.PayloadCase.LR_ENTRY)) {
            try {
//                log.info("Shama: in send request {}", msg);
//                log.info("sessionToStreamObserverRequestMap hash {}", sessionToStreamObserverRequestMap.hashCode());
//                log.info("contents of sessionToStreamObserverRequestMap {}", sessionToStreamObserverRequestMap);
                ReplicationSession session = convertSessionMsg(msg, null);
//                log.info("looking for {} ", session);
                if (!sessionToStreamObserverRequestMap.containsKey(session)) {
                    log.warn("Corfu Message {} has no pending observer. Message {} will not be sent.",
                            msg.getHeader().getRequestId(), msg.getPayload().getPayloadCase().name());
//                    log.info("Stream observers in map: {}", replicationStreamObserverMap.keySet());
                    return;
                }

                StreamObserver<RequestMsg> observer = sessionToStreamObserverRequestMap.get(session);
                log.info("#285 Sending[{}:{}]: {}", clusterId, requestId, msg.getPayload().getPayloadCase().name());
//                log.info("#285 observer from the map {}", observer);
                observer.onNext(msg);

                // Remove observer as request was already sent.
                // Since we send summarized ACKs (to avoid memory leaks) remove all observers lower or equal than
                // the one for which a response is being sent.
//                log.info("removing the observer");
//                replicationStreamObserverMap.remove(session);
            } catch (Exception e) {
                log.error("Caught exception while trying to send message {}", msg.getHeader().getRequestId(), e);
            }

        } else {
            log.error("UnExpected request msg {}", msg);
        }
    }

    private ReplicationSession convertSessionMsg( RequestMsg requestMsg, ResponseMsg responseMsg) {
        String payloadCase;
        if(requestMsg != null) {
            payloadCase = requestMsg.getPayload().getPayloadCase().toString();
        } else {
            payloadCase = responseMsg.getPayload().getPayloadCase().toString();
        }

        LogReplication.ReplicationSessionMsg sessionMsg = null;
        switch(payloadCase) {
            case "LR_ENTRY" :
                if(requestMsg != null) {
                    sessionMsg = requestMsg.getPayload().getLrEntry().getMetadata().getSessionInfo();
                } else {
                    sessionMsg = responseMsg.getPayload().getLrEntryAck().getMetadata().getSessionInfo();
                }
                break;
            case "LR_METADATA_REQUEST" :
                sessionMsg = requestMsg.getPayload().getLrMetadataRequest().getSessionInfo();
                break;
            case "LR_LEADERSHIP_QUERY" :
                sessionMsg = requestMsg.getPayload().getLrLeadershipQuery().getSessionInfo();
                break;
            case "LR_METADATA_RESPONSE" :
                sessionMsg = responseMsg.getPayload().getLrMetadataResponse().getSessionInfo();
                break;
            case "LR_LEADERSHIP_RESPONSE" :
                sessionMsg = responseMsg.getPayload().getLrLeadershipResponse().getSessionInfo();
                break;
            case "LR_LEADERSHIP_LOSS" :
                sessionMsg = responseMsg.getPayload().getLrLeadershipLoss().getSessionInfo();
                break;
            case "LR_SUBSCRIBE_REQUEST" :
                sessionMsg = responseMsg.getPayload().getLrSubscribeRequest().getSessionInfo();
                break;
            case "LR_ENTRY_ACK" :
                sessionMsg = responseMsg.getPayload().getLrEntryAck().getMetadata().getSessionInfo();
                break;
            default:
                log.error("Unexpected payloadcase received : {}", payloadCase);
        }
        ReplicationSubscriber subscriber = new ReplicationSubscriber(sessionMsg.getReplicationModel(), sessionMsg.getClient());
        return new ReplicationSession(sessionMsg.getRemoteClusterId(),sessionMsg.getLocalClusterId(), subscriber);
    }

}
