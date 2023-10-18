package org.corfudb.infrastructure.logreplication.infrastructure.msghandlers;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.TextFormat;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.SessionManager;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationMetadata;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.transport.IClientServerRouter;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLeadershipLossResponseMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLeadershipResponse;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;
import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg.PayloadCase.LR_ENTRY;
import static org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg.PayloadCase.LR_LEADERSHIP_QUERY;
import static org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg.PayloadCase.LR_METADATA_REQUEST;
import static org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg.PayloadCase.LR_SINK_SESSION_INITIALIZATION;
import static org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_ENTRY_ACK;
import static org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_LEADERSHIP_RESPONSE;
import static org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_METADATA_RESPONSE;
import static org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg.PayloadCase.LR_SINK_SESSION_INITIALIZATION_ACK;


/**
 * This class represents the Log Replication Server, which is responsible of providing Log Replication across sites.
 *
 * The Log Replication Server, handles log replication messages :
 * Leadership messages, so source and sink know which node to send/receive messages
 * Negotiation messages, which allows the Source Replicator to get a view of the last synchronized point at the remote cluster.
 * Replication entry messages, which represent parts of a Snapshot (full) sync or a Log Entry (delta) sync and also
 * handles negotiation messages.
 *
 * The incoming messages are currently handled by a single thread.This would change in the subsequent PR to be configurable.
 *
 */
@Slf4j
public class LogReplicationServer extends LogReplicationAbstractServer {

    // Unique and immutable identifier of a server node (UUID)
    // Note: serverContext.getLocalEndpoint() can return an IP or FQDN, which is mutable (for this we
    // should have a unique way the identify a node in the  topology
    private final String localNodeId;

    // Cluster Id of the local node.
    private final String localClusterId;

    private final ExecutorService executor;

    private static final String EXECUTOR_NAME_PREFIX = "LogReplicationServer-";

    @Getter
    @VisibleForTesting
    private final Map<LogReplicationSession, LogReplicationSinkManager> sessionToSinkManagerMap = new ConcurrentHashMap<>();

    private final ServerContext serverContext;

    private final SessionManager sessionManager;

    private final Set<LogReplicationSession> allSessions;

    private final LogReplicationMetadataManager metadataManager;

    private final LogReplicationContext replicationContext;

    /**
     * RequestHandlerMethods for the LogReplication server
     */
    @Getter
    private final ReplicationHandlerMethods handlerMethods =
            ReplicationHandlerMethods.generateHandler(MethodHandles.lookup(), this);

    public LogReplicationServer(@Nonnull ServerContext context, Set<LogReplicationSession> sessions,
                                LogReplicationMetadataManager metadataManager, String localNodeId, String localClusterId,
                                LogReplicationContext replicationContext, SessionManager sessionManager) {
        this.serverContext = context;
        this.allSessions = sessions;
        this.metadataManager = metadataManager;
        this.localNodeId = localNodeId;
        this.localClusterId = localClusterId;
        this.replicationContext = replicationContext;
        this.sessionManager = sessionManager;
        // TODO V2: the number of threads will change in the follow up PR.
        this.executor = context.getExecutorService(1, EXECUTOR_NAME_PREFIX);
    }

    public LogReplicationSinkManager createSinkManager(LogReplicationSession session) {
        if(sessionToSinkManagerMap.containsKey(session)) {
            log.trace("Sink manager already exists for session {}", session);
            return sessionToSinkManagerMap.get(session);
        }
        LogReplicationSinkManager sinkManager = new LogReplicationSinkManager(metadataManager, serverContext, session,
                replicationContext);
        sessionToSinkManagerMap.put(session, sinkManager);
        log.info("Sink Manager created for session={}", session);
        return sinkManager;

    }

    public void updateTopologyConfigId(long topologyConfigId) {
        sessionToSinkManagerMap.values().forEach(sinkManager -> sinkManager.updateTopologyConfigId(topologyConfigId));
    }

    /* ************ Override Methods ************ */

    @Override
    protected void processRequest(RequestMsg req, ResponseMsg res, IClientServerRouter r) {
        // TODO V2: add metrics around the queue size
        executor.submit(() -> getHandlerMethods().handle(req, res, r));
    }

    @Override
    public void shutdown() {
        super.shutdown();
        executor.shutdown();
        sessionToSinkManagerMap.values().forEach(sinkManager -> sinkManager.shutdown());
        sessionToSinkManagerMap.clear();
    }

    /**
     * Get session associated to the received request.
     *
     * @param request
     * @return the session for the given request
     */
    private LogReplicationSession getSession(RequestMsg request) {
        LogReplicationSession session;

        if(!request.getHeader().hasSession()) {
            // Backward compatibility where 'session' field not present
            session = LogReplicationSession.newBuilder()
                    .setSourceClusterId(getUUID(request.getHeader().getClusterId()).toString())
                    .setSinkClusterId(localClusterId)
                    .setSubscriber(LogReplicationConfigManager.getDefaultSubscriber())
                    .build();
        } else {
            session = request.getHeader().getSession();
        }

        return session;
    }

    private ResponseMsg getMetadataResponse(RequestMsg request, ReplicationMetadata metadata) {

        LogReplicationMetadataResponseMsg metadataMsg = LogReplicationMetadataResponseMsg.newBuilder()
                .setTopologyConfigID(metadata.getTopologyConfigId())
                .setVersion(metadata.getVersion())
                .setSnapshotStart(metadata.getLastSnapshotStarted())
                .setSnapshotTransferred(metadata.getLastSnapshotTransferred())
                .setSnapshotApplied(metadata.getLastSnapshotApplied())
                .setLastLogEntryTimestamp(metadata.getLastLogEntryBatchProcessed())
                .build();
        ResponsePayloadMsg payload = ResponsePayloadMsg.newBuilder()
                .setLrMetadataResponse(metadataMsg)
                .build();

        return getResponseMsg(getHeaderMsg(request.getHeader()), payload);
    }

    /* ************ Server Handlers ************ */

    /**
     * Given a log-entry request message, send back an acknowledgement
     * after processing the message.
     *
     * @param request leadership query
     * @param router  router used for sending back the response
     */
    @LogReplicationRequestHandler(requestType = LR_ENTRY)
    private void handleLrEntryRequest(RequestMsg request, ResponseMsg res,
                                      @Nonnull IClientServerRouter router) {
        log.trace("Log Replication Entry received by Server.");

        if (replicationContext.getIsLeader().get()) {
            LogReplicationSession session = getSession(request);

            LogReplicationSinkManager sinkManager = sessionToSinkManagerMap.get(session);

            // We create a sinkManager for sessions that are discovered while bootstrapping LR. But as topology changes,
            // we may discover new sessions. At the same time, its possible that the remote Source cluster finds a new
            // session before the local cluster and sends a request to the local cluster.
            // Since the two events are async, we wait to receive a new session in the incoming request.
            // If the incoming session is not known to sessionManager drop the message (until session is discovered by
            // local cluster), otherwise create a corresponding sinkManager.
            if (sinkManager == null || sinkManager.isSinkManagerShutdown()) {
                if(!allSessions.contains(session)) {
                    log.error("SessionManager does not know about incoming session {}, total={}, current sessions={}",
                            session, sessionToSinkManagerMap.size(), sessionToSinkManagerMap.keySet());
                    return;
                }
            }

            LogReplicationEntryMsg ack = null;
            try {
                // Forward the received message to the Sink Manager for apply
                ack = sinkManager.receive(request.getPayload().getLrEntry());
            } catch (NullPointerException npe) {
                log.warn("Dropping LrEntryRequest with requestId {} as sink manager was destroyed for session {}",
                        request.getHeader().getRequestId(), session);
            }

            if (ack != null) {
                long ts = ack.getMetadata().getEntryType().equals(LogReplicationEntryType.LOG_ENTRY_REPLICATED) ?
                        ack.getMetadata().getTimestamp() : ack.getMetadata().getSnapshotTimestamp();
                log.info("Sending ACK {} on {} to Client ", TextFormat.shortDebugString(ack.getMetadata()), ts);

                ResponsePayloadMsg payload = ResponsePayloadMsg.newBuilder().setLrEntryAck(ack).build();
                HeaderMsg responseHeader = getHeaderMsg(request.getHeader());
                ResponseMsg response = getResponseMsg(responseHeader, payload);
                router.sendResponse(response);
            }
        } else {
            LogReplicationEntryMsg entryMsg = request.getPayload().getLrEntry();
            LogReplicationEntryType entryType = entryMsg.getMetadata().getEntryType();
            log.warn("Dropping received message of type {} while NOT LEADER. snapshotSyncSeqNumber={}, ts={}," +
                            "syncRequestId={}", entryType, entryMsg.getMetadata().getSnapshotSyncSeqNum(),
                    entryMsg.getMetadata().getTimestamp(), entryMsg.getMetadata().getSyncRequestId());
            sendLeadershipLoss(request, router);
        }
    }

    /**
     * Given a metadata request message, send back a response signaling
     * current log-replication status (snapshot related information).
     *
     * @param request leadership query
     * @param router  router used for sending back the response
     */
    @LogReplicationRequestHandler(requestType = LR_METADATA_REQUEST)
    private void handleMetadataRequest(RequestMsg request, ResponseMsg res,
                                       @Nonnull IClientServerRouter router) {
        log.info("Log Replication Metadata Request received by Server.");

        if (replicationContext.getIsLeader().get()) {

            LogReplicationSession session = getSession(request);

            LogReplicationSinkManager sinkManager = sessionToSinkManagerMap.get(session);

            // We create a sinkManager for sessions that are discovered while bootstrapping LR. But as topology changes,
            // we may discover new sessions. At the same time, its possible that the remote Source cluster finds a new
            // session before the local cluster and sends a request to the local cluster.
            // Since the two events are async, we wait to receive a new session in the incoming request.
            // If the incoming session is not known to sessionManager drop the message (until session is discovered by
            // local cluster), otherwise create a corresponding sinkManager.
            // TODO[V2] : We still have a case where the cluster does not ever discover a session on its own.
            //  To resolve this, we need to have a long living RPC from the connectionInitiator cluster which will query
            //  for sessions from the other cluster
            if (sinkManager == null) {
                if(!allSessions.contains(session)) {
                    log.error("SessionManager does not know about incoming session {}, total={}, current sessions={}",
                            session, sessionToSinkManagerMap.size(), sessionToSinkManagerMap.keySet());
                    return;
                } else {
                    sinkManager = createSinkManager(session);
                }
            }

            ReplicationMetadata metadata = metadataManager.getReplicationMetadata(session);
            ResponseMsg response = getMetadataResponse(request, metadata);

            log.info("Send Metadata response for session {}: :: {}", session.hashCode(), TextFormat.shortDebugString(response.getPayload()));
            router.sendResponse(response);

            try {
                // If a snapshot apply is pending, start (if not started already)
                sinkManager.startPendingSnapshotApply();
            } catch (NullPointerException npe) {
                log.warn("Not resuming any pending replication as the sink manager was destroyed for session {}", session);
            }
        } else {
            log.warn("Dropping metadata request as this node is not the leader.  Request id = {}",
                    request.getHeader().getRequestId());
            sendLeadershipLoss(request, router);
        }
    }

    /**
     * Given a leadership request message, send back a
     * response indicating our current leadership status.
     *
     * @param request the leadership request message
     * @param router  router used for sending back the response
     */
    @LogReplicationRequestHandler(requestType = LR_LEADERSHIP_QUERY)
    private void  handleLeadershipQuery(RequestMsg request, ResponseMsg res,
                                                     @Nonnull IClientServerRouter router) {
        log.debug("Log Replication Query Leadership Request received by Server.");
        HeaderMsg responseHeader = getHeaderMsg(request.getHeader());
        ResponseMsg response = getLeadershipResponse(responseHeader, replicationContext.getIsLeader().get(), localNodeId);
        router.sendResponse(response);
    }

    @LogReplicationRequestHandler(requestType = LR_SINK_SESSION_INITIALIZATION)
    private void handleSinkSessionCreationRequest(RequestMsg request, ResponseMsg res,
                                              @Nonnull IClientServerRouter router) {
        log.debug("Log Replication Sink Side Session Initialization Request received by Server.");
        LogReplicationSession session = request.getPayload().getLrSinkSessionInitialization().getSession();
        createSinkManager(session);
        this.sessionManager.refreshForSinkSideInitialization(session);
        LogReplication.LogReplicationSinkSessionInitializationAck ack =
                LogReplication.LogReplicationSinkSessionInitializationAck.newBuilder().build();
        ResponsePayloadMsg payload = ResponsePayloadMsg.newBuilder().setLrSinkSessionInitializationAck(ack).build();
        HeaderMsg responseHeader = getHeaderMsg(request.getHeader());
        ResponseMsg response = getResponseMsg(responseHeader, payload);
        router.sendResponse(response);
    }

    /**
     * Handle an ACK from Log Replication server.
     *
     * @param response The ack message
     * @param router   A reference to the router
     */
    @LogReplicationResponseHandler(responseType = LR_ENTRY_ACK)
    private void handleAck(RequestMsg req, ResponseMsg response,
                                                  @Nonnull IClientServerRouter router) {
        log.debug("Handle log replication ACK {}", response);
        router.completeRequest(response.getHeader().getSession(), response.getHeader().getRequestId(),
                response.getPayload().getLrEntryAck());
    }

    @LogReplicationResponseHandler(responseType = LR_METADATA_RESPONSE)
    private void handleMetadataResponse(RequestMsg req,  ResponseMsg response,
                                                       @Nonnull IClientServerRouter router) {
        log.debug("Handle log replication Metadata Response");
        router.completeRequest(response.getHeader().getSession(), response.getHeader().getRequestId(),
                response.getPayload().getLrMetadataResponse());
    }

    @LogReplicationResponseHandler(responseType = LR_LEADERSHIP_RESPONSE)
    private void handleLeadershipResponse(RequestMsg req, ResponseMsg response,
                                                                      @Nonnull IClientServerRouter router) {
        log.debug("Handle log replication query leadership response msg {}", TextFormat.shortDebugString(response));
        router.completeRequest(response.getHeader().getSession(), response.getHeader().getRequestId(),
                response.getPayload().getLrLeadershipResponse());
    }

    @LogReplicationResponseHandler(responseType = LR_SINK_SESSION_INITIALIZATION_ACK)
    private void handleSinkSessionCreationResponse(RequestMsg req, ResponseMsg response,
                                          @Nonnull IClientServerRouter router) {
        log.debug("Handle log replication sink side session initialization response msg {}", TextFormat.shortDebugString(response));
        router.completeRequest(response.getHeader().getSession(), response.getHeader().getRequestId(),
                response.getPayload().getLrSinkSessionInitializationAck());
    }


    /**
     * Send a leadership loss response.  This will re-trigger leadership discovery on the Source.
     *
     * @param request   the incoming request message
     * @param router    the client router for sending the NACK
     */
    private void sendLeadershipLoss(@Nonnull RequestMsg request, @Nonnull IClientServerRouter router) {
        HeaderMsg responseHeader = getHeaderMsg(request.getHeader());
        ResponseMsg response = getLeadershipLossResponseMsg(responseHeader, localNodeId);
        router.sendResponse(response);
    }

    public void leadershipChanged() {

        if (replicationContext.getIsLeader().get()) {
            // Reset the Sink Managers on acquiring leadership
            sessionToSinkManagerMap.values().forEach(sinkManager -> sinkManager.reset());
        } else {
            // Stop the Sink Managers if leadership is lost
            sessionToSinkManagerMap.values().forEach(sinkManager -> sinkManager.stopOnLeadershipLoss());
        }
    }

    public void stopSinkManagerForSession(LogReplicationSession session) {
        log.info("Stopping Sink manager for session: {}", session);
        try {
            if (sessionToSinkManagerMap.containsKey(session)) {
                sessionToSinkManagerMap.get(session).shutdown();
                sessionToSinkManagerMap.remove(session);
            }
        } catch (NullPointerException npe) {
            log.warn("SinkManger was either shutdown or wasnot created for session {}", session);
        }
    }
}
