package org.corfudb.infrastructure.logreplication.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.TextFormat;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.AbstractServer;
import org.corfudb.infrastructure.IServerRouter;
import org.corfudb.infrastructure.RequestHandler;
import org.corfudb.infrastructure.RequestHandlerMethods;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationMetadata;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLeadershipLoss;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLeadershipResponse;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;
import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;

/**
 * This class represents the Log Replication Server, which is responsible of providing Log Replication across sites.
 *
 * The Log Replication Server, handles log replication entries--which represent parts of a Snapshot (full) sync or a
 * Log Entry (delta) sync and also handles negotiation messages, which allows the Source Replicator to get a view of
 * the last synchronized point at the remote cluster.
 */
@Slf4j
public class LogReplicationServer extends AbstractServer {

    // Unique and immutable identifier of a server node (UUID)
    // Note: serverContext.getLocalEndpoint() can return an IP or FQDN, which is mutable (for this we
    // should have a unique way the identify a node in the  topology
    private String localNodeId;

    // Cluster Id of the local node.
    private String localClusterId;

    private final ExecutorService executor;

    private static final String EXECUTOR_NAME_PREFIX = "LogReplicationServer-";

    private Map<LogReplicationSession, LogReplicationSinkManager> sessionToSinkManagerMap = new HashMap<>();

    private final AtomicBoolean isLeader = new AtomicBoolean(false);

    @Getter
    private SessionManager sessionManager;

    /**
     * RequestHandlerMethods for the LogReplication server
     */
    @Getter
    private final RequestHandlerMethods handlerMethods = createHandlerMethods();

    protected RequestHandlerMethods createHandlerMethods() {
        return RequestHandlerMethods.generateHandler(MethodHandles.lookup(), this);
    }

    public LogReplicationServer(@Nonnull ServerContext context, @Nonnull SessionManager sessionManager,
                                String localEndpoint) {
        this.localNodeId = sessionManager.getTopology().getLocalNodeDescriptor().getNodeId();
        this.localClusterId = sessionManager.getTopology().getLocalClusterDescriptor().getClusterId();
        this.sessionManager = sessionManager;
        createSinkManagers(localEndpoint, context);
        this.executor = context.getExecutorService(1, EXECUTOR_NAME_PREFIX);
    }

    @VisibleForTesting
    public LogReplicationServer(@Nonnull ServerContext context, LogReplicationSinkManager sinkManager,
        String localNodeId, String localClusterId, SessionManager sessionManager) {
        sessionToSinkManagerMap.put(sinkManager.getSession(), sinkManager);
        this.localNodeId = localNodeId;
        this.localClusterId = localClusterId;
        this.sessionManager = sessionManager;
        this.executor = context.getExecutorService(1, EXECUTOR_NAME_PREFIX);
    }

     private void createSinkManagers(String localEndpoint, ServerContext serverContext) {
        for (LogReplicationSession session : sessionManager.getIncomingSessions()) {
            LogReplicationSinkManager sinkManager = new LogReplicationSinkManager(localEndpoint,
                    sessionManager.getMetadataManager(), serverContext, session, sessionManager.getReplicationContext());
            sessionToSinkManagerMap.put(session, sinkManager);
            log.info("Sink Manager created for session={}", session);
        }
    }

    public void updateTopologyConfigId(long topologyConfigId) {
        sessionToSinkManagerMap.values().forEach(sinkManager -> sinkManager.updateTopologyConfigId(topologyConfigId));
    }

    /* ************ Override Methods ************ */

    @Override
    protected void processRequest(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        executor.submit(() -> getHandlerMethods().handle(req, ctx, r));
    }

    @Override
    public void shutdown() {
        super.shutdown();
        executor.shutdown();
        sessionToSinkManagerMap.values().forEach(sinkManager -> sinkManager.shutdown());
        sessionToSinkManagerMap.clear();
    }

    /* ************ Server Handlers ************ */

    /**
     * Given a log-entry request message, send back an acknowledgement
     * after processing the message.
     *
     * @param request leadership query
     * @param ctx     enables a {@link ChannelHandler} to interact with its
     *                {@link ChannelPipeline} and other handlers
     * @param router  router used for sending back the response
     */
    @RequestHandler(type = PayloadCase.LR_ENTRY)
    private void handleLrEntryRequest(@Nonnull RequestMsg request,
                                      @Nonnull ChannelHandlerContext ctx,
                                      @Nonnull IServerRouter router) {
        log.trace("Log Replication Entry received by Server.");

        if (isLeader.get()) {
            LogReplicationSession session = getSession(request);

            LogReplicationSinkManager sinkManager = sessionToSinkManagerMap.get(session);

            // If no sink Manager is found, drop the message and log an error
            if (sinkManager == null) {
                log.error("Sink Manager not found for session {}, total={}, sessions={}", session,
                        sessionToSinkManagerMap.size(), sessionToSinkManagerMap.keySet());
                return;
            }

            // Forward the received message to the Sink Manager for apply
            LogReplicationEntryMsg ack = sinkManager.receive(request.getPayload().getLrEntry());

            if (ack != null) {
                long ts = ack.getMetadata().getEntryType().equals(LogReplicationEntryType.LOG_ENTRY_REPLICATED) ?
                    ack.getMetadata().getTimestamp() : ack.getMetadata().getSnapshotTimestamp();
                log.info("Sending ACK {} on {} to Client ", TextFormat.shortDebugString(ack.getMetadata()), ts);

                ResponsePayloadMsg payload = ResponsePayloadMsg.newBuilder().setLrEntryAck(ack).build();
                HeaderMsg responseHeader = getHeaderMsg(request.getHeader());
                ResponseMsg response = getResponseMsg(responseHeader, payload);
                router.sendResponse(response, ctx);
            }
        } else {
            LogReplicationEntryMsg entryMsg = request.getPayload().getLrEntry();
            LogReplicationEntryType entryType = entryMsg.getMetadata().getEntryType();
            log.warn("Dropping received message of type {} while NOT LEADER. snapshotSyncSeqNumber={}, ts={}," +
                "syncRequestId={}", entryType, entryMsg.getMetadata().getSnapshotSyncSeqNum(),
                entryMsg.getMetadata().getTimestamp(), entryMsg.getMetadata().getSyncRequestId());
            sendLeadershipLoss(request, ctx, router);
        }
    }

    /**
     * Given a metadata request message, send back a response signaling
     * current log-replication status (snapshot related information).
     *
     * @param request leadership query
     * @param ctx     enables a {@link ChannelHandler} to interact with its
     *                {@link ChannelPipeline} and other handlers
     * @param router  router used for sending back the response
     */
    @RequestHandler(type = PayloadCase.LR_METADATA_REQUEST)
    private void handleMetadataRequest(@Nonnull RequestMsg request,
                                       @Nonnull ChannelHandlerContext ctx,
                                       @Nonnull IServerRouter router) {
        log.info("Log Replication Metadata Request received by Server.");

        if (isLeader.get()) {

            LogReplicationSession session = getSession(request);

            LogReplicationSinkManager sinkManager = sessionToSinkManagerMap.get(session);

            if (sinkManager == null) {
                log.error("Sink Manager not found for session {}, total={}, sessions={}", session,
                        sessionToSinkManagerMap.size(), sessionToSinkManagerMap.keySet());
                return;
            }

            ReplicationMetadata metadata = sessionManager.getMetadataManager().getReplicationMetadata(session);
            ResponseMsg response = getMetadataResponse(request, metadata);

            log.info("Send Metadata response: :: {}", TextFormat.shortDebugString(response.getPayload()));
            router.sendResponse(response, ctx);

            // If a snapshot apply is pending, start (if not started already)
            sinkManager.startPendingSnapshotApply();
        } else {
            log.warn("Dropping metadata request as this node is not the leader.  Request id = {}",
                request.getHeader().getRequestId());
            sendLeadershipLoss(request, ctx, router);
        }
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
                    .setSubscriber(SessionManager.getDefaultSubscriber())
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

    /**
     * Given a leadership request message, send back a
     * response indicating our current leadership status.
     *
     * @param request the leadership request message
     * @param ctx     the context which enables a {@link ChannelHandler} to interact with its
     *                {@link ChannelPipeline} and other handlers
     * @param router  router used for sending back the response
     */
    @RequestHandler(type = PayloadCase.LR_LEADERSHIP_QUERY)
    private void handleLogReplicationQueryLeadership(@Nonnull RequestMsg request,
                                                     @Nonnull ChannelHandlerContext ctx,
                                                     @Nonnull IServerRouter router) {
        log.debug("Log Replication Query Leadership Request received by Server.");
        HeaderMsg responseHeader = getHeaderMsg(request.getHeader());
        ResponseMsg response = getLeadershipResponse(responseHeader, isLeader.get(), localNodeId);
        router.sendResponse(response, ctx);
    }

    /**
     * Send a leadership loss response.  This will re-trigger leadership discovery on the Source.
     *
     * @param request   the incoming request message
     * @param ctx       the channel context
     * @param router    the client router for sending the NACK
     */
    private void sendLeadershipLoss(@Nonnull RequestMsg request,
        @Nonnull ChannelHandlerContext ctx, @Nonnull IServerRouter router) {
        HeaderMsg responseHeader = getHeaderMsg(request.getHeader());
        ResponseMsg response = getLeadershipLoss(responseHeader, localNodeId);
        router.sendResponse(response, ctx);
    }

    public void setLeadership(boolean leader) {
        isLeader.set(leader);

        if (isLeader.get()) {
            // Reset the Sink Managers on acquiring leadership
            sessionToSinkManagerMap.values().forEach(sinkManager -> sinkManager.reset());
        } else {
            // Stop the Sink Managers if leadership is lost
            sessionToSinkManagerMap.values().forEach(sinkManager -> sinkManager.stopOnLeadershipLoss());
        }
    }
}
