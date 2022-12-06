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
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationModel;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationSubscriber;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.LogReplicationSession;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
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

    private static final String DEFAULT_CLIENT_NAME = "Federation-Client";

    private static final UUID DEFAULT_CLIENT_ID = UUID.fromString(DEFAULT_CLIENT_NAME);

    // Unique and immutable identifier of a server node (UUID)
    // Note: serverContext.getLocalEndpoint() can return an IP or FQDN, which is mutable (for this we
    // should have a unique way the identify a node in the  topology
    private String localNodeId;

    private final ExecutorService executor;

    private static final String EXECUTOR_NAME_PREFIX = "LogReplicationServer-";

    private Map<LogReplicationSession, LogReplicationSinkManager> sourceSessionToSinkManagerMap = new HashMap<>();

    private final AtomicBoolean isLeader = new AtomicBoolean(false);

    private final LogReplicationMetadataManager metadataManager;

    /**
     * RequestHandlerMethods for the LogReplication server
     */
    @Getter
    private final RequestHandlerMethods handlerMethods = createHandlerMethods();

    protected RequestHandlerMethods createHandlerMethods() {
        return RequestHandlerMethods.generateHandler(MethodHandles.lookup(), this);
    }

    public LogReplicationServer(@Nonnull ServerContext context, String localNodeId,
                                LogReplicationConfigManager configManager, String localEndpoint,
                                long topologyConfigId, @Nonnull LogReplicationMetadataManager metadataManager) {
        this.localNodeId = localNodeId;
        this.metadataManager = metadataManager;
        createSinkManagers(configManager, localEndpoint, context, topologyConfigId);
        this.executor = context.getExecutorService(1, EXECUTOR_NAME_PREFIX);
    }

    @VisibleForTesting
    public LogReplicationServer(@Nonnull ServerContext context, LogReplicationSinkManager sinkManager,
        String localNodeId, LogReplicationMetadataManager metadataManager) {
        sourceSessionToSinkManagerMap.put(sinkManager.getSession(), sinkManager);
        this.localNodeId = localNodeId;
        this.metadataManager = metadataManager;
        this.executor = context.getExecutorService(1, EXECUTOR_NAME_PREFIX);
    }

     private void createSinkManagers(LogReplicationConfigManager configManager, String localEndpoint,
                                     ServerContext serverContext, long topologyConfigId) {
        for (LogReplicationSession session : metadataManager.getIncomingSessions()) {
            LogReplicationSinkManager sinkManager = new LogReplicationSinkManager(localEndpoint, configManager,
                    metadataManager, serverContext, topologyConfigId, session);
            sourceSessionToSinkManagerMap.put(session, sinkManager);
        }
    }

    public void updateTopologyConfigId(long topologyConfigId) {
        sourceSessionToSinkManagerMap.values().forEach(sinkManager -> sinkManager.updateTopologyConfigId(topologyConfigId));
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
        sourceSessionToSinkManagerMap.values().forEach(sinkManager -> sinkManager.shutdown());
        sourceSessionToSinkManagerMap.clear();
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
            // Get the Sink Manager corresponding to the remote cluster session
            String sourceClusterId = getUUID(request.getHeader().getClusterId()).toString();
            LogReplicationSinkManager sinkManager = sourceSessionToSinkManagerMap.get(
                ReplicationSession.getDefaultReplicationSessionForCluster(sourceClusterId));

            // If no sink Manager is found, drop the message and log an error
            if (sinkManager == null) {
                log.error("Sink Manager not found for remote cluster {}.  This could be due to a topology mismatch.",
                    getUUID(request.getHeader().getClusterId()).toString());
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
            String sourceClusterId = getUUID(request.getHeader().getClusterId()).toString();

            // TODO: after multi-model support is added instead of default session, this info will be
            //  part of the RequestMsg
            LogReplicationSession session = getDefaultSession(sourceClusterId, localNodeId);

            LogReplicationSinkManager sinkManager = sourceSessionToSinkManagerMap.get(session);

            if (sinkManager == null) {
                log.error("Sink Manager not found for session {}", session);
                return;
            }

            ResponseMsg response = getMetadataResponse(request, session);

            log.info("Send Metadata response: :: {}", TextFormat.shortDebugString(response.getPayload()));
            router.sendResponse(response, ctx);

            // If a snapshot apply is pending, start (if not started already)
            if (isSnapshotApplyPending(session) && !sinkManager.getOngoingApply().get()) {
                sinkManager.resumeSnapshotApply();
            }
        } else {
            log.warn("Dropping metadata request as this node is not the leader.  Request id = {}",
                request.getHeader().getRequestId());
            sendLeadershipLoss(request, ctx, router);
        }
    }

    private ResponseMsg getMetadataResponse(RequestMsg request, LogReplicationSession session) {
        ReplicationMetadata metadata = metadataManager.queryReplicationMetadata(session);
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

        return  getResponseMsg(getHeaderMsg(request.getHeader()), payload);
    }

    /**
     * Return default session. This method should be used during rolling upgrades
     * between versions V1 and V2. LR-V1 does not have the notion of sessions.
     */
    private LogReplicationSession getDefaultSession(String sourceClusterId, String sinkClusterId) {
        return  LogReplicationSession.newBuilder()
                .setSourceClusterId(sourceClusterId)
                .setSinkClusterId(sinkClusterId) // TODO: obtain cluster ID (instead of node)
                .setSubscriber(ReplicationSubscriber.newBuilder()
                        .setClientName(DEFAULT_CLIENT_NAME)
                        .setModel(ReplicationModel.TABLE_REPLICATION)
                        .setClientId(RpcCommon.UuidMsg.newBuilder()
                                .setMsb(DEFAULT_CLIENT_ID.getMostSignificantBits())
                                .setLsb(DEFAULT_CLIENT_ID.getLeastSignificantBits())
                                .build())
                        .build())
                .build();
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

    private boolean isSnapshotApplyPending(LogReplicationSession session) {
        ReplicationMetadata metadata = metadataManager.queryReplicationMetadata(session);

        return (metadata.getLastSnapshotStarted() == metadata.getLastSnapshotTransferred()) &&
                metadata.getLastSnapshotTransferred() > metadata.getLastSnapshotApplied();
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
            sourceSessionToSinkManagerMap.values().forEach(sinkManager -> sinkManager.reset());
        } else {
            // Stop the Sink Managers if leadership is lost
            sourceSessionToSinkManagerMap.values().forEach(sinkManager -> sinkManager.stopOnLeadershipLoss());
        }
    }
}
