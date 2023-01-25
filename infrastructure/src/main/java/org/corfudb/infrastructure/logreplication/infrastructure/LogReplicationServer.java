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
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.LogReplication;
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


    // unique and immutable identifier of server's node (UUID)
    // ServerContext.getLocalEndpoint() could be IP or FQDN, which is mutable
    // node id should be the only identifier for a node in the topology
    private String localNodeId;

    private final ExecutorService executor;

    private static final String EXECUTOR_NAME_PREFIX = "LogReplicationServer-";

    private Map<LogReplication.ReplicationSessionMsg, LogReplicationSinkManager> incomingSessionMsgToSinkManagerMap = new HashMap<>();

    private final AtomicBoolean isLeader = new AtomicBoolean(false);

    private final LogReplicationConfigManager configManager;

    private final String localEndpoint;

    private long topologyConfigId;

    private final Map<ReplicationSession, LogReplicationMetadataManager> localSessionToMetadataManagerMap;


    /**
     * RequestHandlerMethods for the LogReplication server
     */
    @Getter
    private final RequestHandlerMethods handlerMethods = createHandlerMethods();

    protected RequestHandlerMethods createHandlerMethods() {
        return RequestHandlerMethods.generateHandler(MethodHandles.lookup(), this);
    }

    public LogReplicationServer(@Nonnull ServerContext context, String localNodeId,
                                LogReplicationConfigManager configManager,
                                String localEndpoint, long topologyConfigId,
                                Map<ReplicationSession, LogReplicationMetadataManager> outgoingSessionToMetadataManagerMap) {
        this.localNodeId = localNodeId;
        this.configManager = configManager;
        this.localEndpoint = localEndpoint;
        this.topologyConfigId = topologyConfigId;
        this.localSessionToMetadataManagerMap = outgoingSessionToMetadataManagerMap;
        createSinkManagers();
        this.executor = context.getExecutorService(1, EXECUTOR_NAME_PREFIX);
    }

    @VisibleForTesting
    public LogReplicationServer(@Nonnull ServerContext context, LogReplicationSinkManager sinkManager,
        String localNodeId) {
        this.configManager = null;
        this.localEndpoint = null;
        this.topologyConfigId = 0L;
        this.localSessionToMetadataManagerMap = null;

        ReplicationSession outgoingSession = sinkManager.getSourceSession();
        LogReplication.ReplicationSessionMsg incomingSession = LogReplication.ReplicationSessionMsg.newBuilder()
                .setRemoteClusterId(outgoingSession.getLocalClusterId())
                .setLocalClusterId(outgoingSession.getRemoteClusterId())
                .setClient(outgoingSession.getSubscriber().getClient())
                .setReplicationModel(outgoingSession.getSubscriber().getReplicationModel())
                .build();
        incomingSessionMsgToSinkManagerMap.put(incomingSession, sinkManager);
        this.localNodeId = localNodeId;
        this.executor = context.getExecutorService(1, EXECUTOR_NAME_PREFIX);
    }

     private void createSinkManagers() {
        for (Map.Entry<ReplicationSession, LogReplicationMetadataManager> entry :
                localSessionToMetadataManagerMap.entrySet()) {
            ReplicationSession outgoingSession = entry.getKey();
            LogReplicationSinkManager sinkManager = new LogReplicationSinkManager(localEndpoint, configManager,
                entry.getValue(), configManager.getServerContext(), topologyConfigId, entry.getKey());
            LogReplication.ReplicationSessionMsg incomingSession = LogReplication.ReplicationSessionMsg.newBuilder()
                    .setRemoteClusterId(outgoingSession.getLocalClusterId())
                    .setLocalClusterId(outgoingSession.getRemoteClusterId())
                    .setClient(outgoingSession.getSubscriber().getClient())
                    .setReplicationModel(outgoingSession.getSubscriber().getReplicationModel())
                    .build();
            incomingSessionMsgToSinkManagerMap.put(incomingSession, sinkManager);
        }
    }

    private void createSinkManagers(LogReplication.ReplicationSessionMsg outgoingSessionMsg) {
        // TODO: this will change to msg after the sessionsManager PR
        ReplicationSession session = new ReplicationSession(outgoingSessionMsg.getRemoteClusterId(),
                outgoingSessionMsg.getLocalClusterId(),
                new ReplicationSubscriber(outgoingSessionMsg.getReplicationModel(), outgoingSessionMsg.getClient()));
        LogReplicationSinkManager sinkManager = new LogReplicationSinkManager(localEndpoint, configManager,
                localSessionToMetadataManagerMap.get(outgoingSessionMsg), configManager.getServerContext(), topologyConfigId, session);

        //Reverse sessionInfo while adding it to incomingSessionMsgToSinkManagerMap so the requests handlers don't have to.
        LogReplication.ReplicationSessionMsg incomingSession = LogReplication.ReplicationSessionMsg.newBuilder()
                .mergeFrom(outgoingSessionMsg)
                .setRemoteClusterId(outgoingSessionMsg.getLocalClusterId())
                .setLocalClusterId(outgoingSessionMsg.getRemoteClusterId())
                .build();
        incomingSessionMsgToSinkManagerMap.put(incomingSession, sinkManager);
    }

    public void updateTopologyConfigId(long topologyConfigId) {
        this.topologyConfigId = topologyConfigId;
        incomingSessionMsgToSinkManagerMap.values().forEach(sinkManager -> sinkManager.updateTopologyConfigId(topologyConfigId));
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
        incomingSessionMsgToSinkManagerMap.values().forEach(sinkManager -> sinkManager.shutdown());
        incomingSessionMsgToSinkManagerMap.clear();
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
            LogReplicationSinkManager sinkManager = incomingSessionMsgToSinkManagerMap.get(request.getHeader().getSessionInfo());

            // If no sink Manager is found and if the session is not seen by the discoveryService, drop the message and log an error. Else create Sink Manager
            if (sinkManager == null && !sessionValid(request.getHeader().getSessionInfo(), getUUID(request.getHeader().getClusterId()).toString())) {
                return;
            }

            // Forward the received message to the Sink Manager for apply
            LogReplicationEntryMsg ack = sinkManager.receive(request.getPayload().getLrEntry());

            if (ack != null) {
                long ts = ack.getMetadata().getEntryType().equals(LogReplicationEntryType.LOG_ENTRY_REPLICATED) ?
                    ack.getMetadata().getTimestamp() : ack.getMetadata().getSnapshotTimestamp();
                log.info("Sending ACK {} on {} to Client ", TextFormat.shortDebugString(ack.getMetadata()), ts);

                ResponsePayloadMsg payload = ResponsePayloadMsg.newBuilder().setLrEntryAck(ack).build();
                HeaderMsg responseHeader = getHeaderMsg(createLrResponseHeader(request.getHeader()));
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
     * When sink manager is not found, check if the session is valid and create sink managers.
     * @param incomingSession
     * @param requestId
     * @return true if session is valid, false if not valid
     */
    private boolean sessionValid(LogReplication.ReplicationSessionMsg incomingSession, String requestId) {
        LogReplication.ReplicationSessionMsg outgoingSession = LogReplication.ReplicationSessionMsg.newBuilder()
                .mergeFrom(incomingSession)
                .setLocalClusterId(incomingSession.getRemoteClusterId())
                .setRemoteClusterId(incomingSession.getRemoteClusterId())
                .build();
        // TODO: the ReplicationSession will be replaced by ReplicationSessionMsg
        ReplicationSession sessionObj = new ReplicationSession(outgoingSession.getRemoteClusterId(),
                outgoingSession.getLocalClusterId(),
                new ReplicationSubscriber(outgoingSession.getReplicationModel(), outgoingSession.getClient()));
        if(localSessionToMetadataManagerMap.containsKey(sessionObj)) {
            createSinkManagers(outgoingSession);
            return true;
        }

        log.error("Sink Manager not found for remote cluster {}.  This could be due to a topology mismatch.", requestId);
        return false;
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
            LogReplicationSinkManager sinkManager = incomingSessionMsgToSinkManagerMap.get(request.getHeader().getSessionInfo());

            // If no sink Manager is found, drop the message and log an error
            if (sinkManager == null && !sessionValid(request.getHeader().getSessionInfo(), getUUID(request.getHeader().getClusterId()).toString())) {
                return;
            }

            LogReplicationMetadataManager metadataMgr = sinkManager.getLogReplicationMetadataManager();

            ResponseMsg response = metadataMgr.getMetadataResponse(getHeaderMsg(createLrResponseHeader(request.getHeader())));
            log.info("Send Metadata response: :: {}", TextFormat.shortDebugString(response.getPayload()));
            router.sendResponse(response, ctx);

            // If a snapshot apply is pending, start (if not started already)
            if (isSnapshotApplyPending(metadataMgr) && !sinkManager.getOngoingApply().get()) {
                sinkManager.resumeSnapshotApply();
            }
        } else {
            log.warn("Dropping metadata request as this node is not the leader.  Request id = {}",
                request.getHeader().getRequestId());
            sendLeadershipLoss(request, ctx, router);
        }
    }

    /**
     * Given a leadership request message, send back a
     * response indicating our current leadership status.
     *
     * @param request leadership query
     * @param ctx     enables a {@link ChannelHandler} to interact with its
     *                {@link ChannelPipeline} and other handlers
     * @param router  router used for sending back the response
     */
    @RequestHandler(type = PayloadCase.LR_LEADERSHIP_QUERY)
    private void handleLogReplicationQueryLeadership(@Nonnull RequestMsg request,
                                                     @Nonnull ChannelHandlerContext ctx,
                                                     @Nonnull IServerRouter router) {
        log.debug("Log Replication Query Leadership Request received by Server.");
        HeaderMsg responseHeader = getHeaderMsg(createLrResponseHeader(request.getHeader()));
        ResponseMsg response = getLeadershipResponse(responseHeader, isLeader.get(), localNodeId);
        router.sendResponse(response, ctx);
    }

    private boolean isSnapshotApplyPending(LogReplicationMetadataManager metadataMgr) {
        return (metadataMgr.getLastStartedSnapshotTimestamp() == metadataMgr.getLastTransferredSnapshotTimestamp()) &&
                metadataMgr.getLastTransferredSnapshotTimestamp() > metadataMgr.getLastAppliedSnapshotTimestamp();
    }

    /**
     * Send a leadership loss response.  This will re-trigger leadership discovery on the Source.
     * @param request Incoming request message
     * @param ctx Channel context
     * @param router Client router for sending the NACK
     */
    private void sendLeadershipLoss(@Nonnull RequestMsg request,
        @Nonnull ChannelHandlerContext ctx, @Nonnull IServerRouter router) {
        HeaderMsg responseHeader = getHeaderMsg(createLrResponseHeader(request.getHeader()));
        ResponseMsg response = getLeadershipLoss(responseHeader, localNodeId);
        router.sendResponse(response, ctx);
    }

    private HeaderMsg createLrResponseHeader(HeaderMsg incomingHeader) {
        LogReplication.ReplicationSessionMsg incomingSession = incomingHeader.getSessionInfo();
        return HeaderMsg.newBuilder().mergeFrom(incomingHeader)
                .setSessionInfo(LogReplication.ReplicationSessionMsg.newBuilder()
                        .setRemoteClusterId(incomingSession.getLocalClusterId())
                        .setLocalClusterId(incomingSession.getRemoteClusterId())
                        .setReplicationModel(incomingSession.getReplicationModel())
                        .setClient(incomingSession.getClient())
                        .build())
                .build();
    }

    public void setLeadership(boolean leader) {
        isLeader.set(leader);

        if (isLeader.get()) {
            // Reset the Sink Managers on acquiring leadership
            incomingSessionMsgToSinkManagerMap.values().forEach(sinkManager -> sinkManager.reset());
        } else {
            // Stop the Sink Managers if leadership is lost
            incomingSessionMsgToSinkManagerMap.values().forEach(sinkManager -> sinkManager.stopOnLeadershipLoss());
        }
    }
}
