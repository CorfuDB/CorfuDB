package org.corfudb.infrastructure;

import com.google.protobuf.TextFormat;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLeadershipLoss;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLeadershipResponse;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getHeaderMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.getResponseMsg;

/**
 * This class represents the Log Replication Server, which is
 * responsible of providing Log Replication across sites.
 *
 * The Log Replication Server, handles log replication entries--which
 * represent parts of a Snapshot (full) sync or a Log Entry (delta) sync
 * and also handles negotiation messages, which allows the Source Replicator
 * to get a view of the last synchronized point at the remote cluster.
 */
@Slf4j
public class LogReplicationServer extends AbstractServer {


    // unique and immutable identifier of server's node (UUID)
    // ServerContext.getLocalEndpoint() could be IP or FQDN, which is mutable
    // node id should be the only identifier for a node in the topology
    private String localNodeId;

    private final ExecutorService executor;

    @Getter
    private final LogReplicationMetadataManager metadataManager;

    @Getter
    private final LogReplicationSinkManager sinkManager;

    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private final AtomicBoolean isActive = new AtomicBoolean(false);

    /**
     * RequestHandlerMethods for the LogReplication server
     */
    @Getter
    private final RequestHandlerMethods handlerMethods = createHandlerMethods();

    protected RequestHandlerMethods createHandlerMethods() {
        return RequestHandlerMethods.generateHandler(MethodHandles.lookup(), this);
    }

    public LogReplicationServer(@Nonnull ServerContext context, @Nonnull  LogReplicationConfig logReplicationConfig,
                                @Nonnull LogReplicationMetadataManager metadataManager, String corfuEndpoint,
                                long topologyConfigId, String localNodeId) {
        this(context, metadataManager, new LogReplicationSinkManager(corfuEndpoint, logReplicationConfig,
                metadataManager, context, topologyConfigId));
        this.localNodeId = localNodeId;
    }

    public LogReplicationServer(@Nonnull ServerContext context,
                                @Nonnull LogReplicationMetadataManager metadataManager,
                                @Nonnull LogReplicationSinkManager sinkManager) {
        this.metadataManager = metadataManager;
        this.sinkManager = sinkManager;
        this.executor = context.getExecutorService(1, "LogReplicationServer-");
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
    }

    /* ************ Server Handlers ************ */

    /**
     * Given a log-entry request message, send back the log-data entries.
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

        if (isLeader(request, ctx, router)) {
            // Forward the received message to the Sink Manager for apply
            LogReplicationEntryMsg ack =
                    sinkManager.receive(request.getPayload().getLrEntry());

            if (ack != null) {
                long ts = ack.getMetadata().getEntryType().equals(LogReplicationEntryType.LOG_ENTRY_REPLICATED) ?
                        ack.getMetadata().getTimestamp() : ack.getMetadata().getSnapshotTimestamp();
                log.info("Sending ACK {} on {} to Client ", TextFormat.shortDebugString(ack.getMetadata()), ts);

                ResponsePayloadMsg payload = ResponsePayloadMsg.newBuilder()
                        .setLrEntryAck(ack)
                        .build();
                HeaderMsg responseHeader = getHeaderMsg(request.getHeader());
                ResponseMsg response = getResponseMsg(responseHeader, payload);
                router.sendResponse(response, ctx);
            }
        } else {
            log.warn("Dropping log replication entry as this node is not the leader.");
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

        if (isLeader(request, ctx, router)) {
            LogReplicationMetadataManager metadataMgr = sinkManager.getLogReplicationMetadataManager();
            ResponseMsg response = metadataMgr.getMetadataResponse(getHeaderMsg(request.getHeader()));
            log.info("Send Metadata response :: {}", TextFormat.shortDebugString(response.getPayload()));
            router.sendResponse(response, ctx);

            // If a snapshot apply is pending, start (if not started already)
            if (isSnapshotApplyPending(metadataMgr) && !sinkManager.getOngoingApply().get()) {
                sinkManager.resumeSnapshotApply();
            }
        } else {
            log.warn("Dropping metadata request as this node is not the leader.");
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
        HeaderMsg responseHeader = getHeaderMsg(request.getHeader());
        ResponseMsg response = getLeadershipResponse(responseHeader, isLeader.get(), localNodeId);
        router.sendResponse(response, ctx);
    }

    /* ************ Private / Utility Methods ************ */

    private boolean isSnapshotApplyPending(LogReplicationMetadataManager metadataMgr) {
        return (metadataMgr.getLastStartedSnapshotTimestamp() == metadataMgr.getLastTransferredSnapshotTimestamp()) &&
                metadataMgr.getLastTransferredSnapshotTimestamp() > metadataMgr.getLastAppliedSnapshotTimestamp();
    }

    /**
     * Verify if current node is still the lead receiving node.
     *
     * @return true, if leader node.
     *         false, otherwise.
     */
    protected synchronized boolean isLeader(@Nonnull RequestMsg request,
                                            @Nonnull ChannelHandlerContext ctx,
                                            @Nonnull IServerRouter router) {
        // If the current cluster has switched to the active role (no longer the receiver) or it is no longer the leader,
        // skip message processing (drop received message) and nack on leadership (loss of leadership)
        // This will re-trigger leadership discovery on the sender.
        boolean lostLeadership = isActive.get() || !isLeader.get();

        if (lostLeadership) {
            log.warn("This node has changed, active={}, leader={}. Dropping message type={}, id={}", isActive.get(),
                    isLeader.get(), request.getPayload().getPayloadCase(), request.getHeader().getRequestId());
            HeaderMsg responseHeader = getHeaderMsg(request.getHeader());
            ResponseMsg response = getLeadershipLoss(responseHeader, localNodeId);
            router.sendResponse(response, ctx);
        }

        return !lostLeadership;
    }

    /* ************ Public Methods ************ */

    public synchronized void setLeadership(boolean leader) {
        isLeader.set(leader);
    }

    public synchronized void setActive(boolean active) {
        isActive.set(active);
    }
}
