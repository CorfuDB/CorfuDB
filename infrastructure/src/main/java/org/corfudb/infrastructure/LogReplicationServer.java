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
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.exceptions.LogReplicationBusyException;
import org.corfudb.runtime.proto.service.CorfuMessage.HeaderMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg.PayloadCase;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponseMsg;
import org.corfudb.runtime.proto.service.CorfuMessage.ResponsePayloadMsg;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
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

    /*
     * Size bounding LRs client RPC queue, set to be at least that of the sender buffer window
     * (LogReplicationConfig.DEFAULT_MAX_NUM_MSG_PER_BATCH).
     * Consider optimizations like de-duplication of resent messages and/or disk-backed receiver queue to avoid tuning
     * this parameter for higher scale.
     */
    private static final int MAX_EXECUTOR_QUEUE_SIZE = 5;
    private final ExecutorService executor;

    /*
     * Bounds the LR_ENTRY requests that are queued or running. A request beyond it is answered
     * with a typed BUSY reply instead of being dropped, so the source never has to infer
     * backpressure from a timeout.
     */
    private final Semaphore dataCapacity = new Semaphore(MAX_EXECUTOR_QUEUE_SIZE);

    private static final long BUSY_RETRY_AFTER_MS = 2000;

    // A source that predates the snapshot lease is told so once per episode, not on every poll.
    private final AtomicBoolean unsupportedSourceReported = new AtomicBoolean(false);

    @Getter
    private final LogReplicationMetadataManager metadataManager;

    @Getter
    private final LogReplicationSinkManager sinkManager;

    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private final AtomicBoolean isActive = new AtomicBoolean(false);
    private final AtomicBoolean isStandby = new AtomicBoolean(false);

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
                metadataManager, context, topologyConfigId), localNodeId);
    }

    public LogReplicationServer(@Nonnull ServerContext context,
                                @Nonnull LogReplicationMetadataManager metadataManager,
                                @Nonnull LogReplicationSinkManager sinkManager, String localNodeId) {
        this.localNodeId = localNodeId;
        this.metadataManager = metadataManager;
        this.sinkManager = sinkManager;
        this.executor = context.getExecutorService(1, "LogReplicationServer-");
    }

    /* ************ Override Methods ************ */

    /**
     * Control-plane requests (leadership and status) are answered on the calling I/O thread: both
     * handlers only read volatile state and a cached, already committed status, so they never block
     * and can never queue behind a slow write. Only LR_ENTRY, whose handler runs down to the actual
     * log write, is handed to the executor.
     */
    @Override
    protected void processRequest(RequestMsg req, ChannelHandlerContext ctx, IServerRouter r) {
        if (req.getPayload().getPayloadCase() != PayloadCase.LR_ENTRY) {
            getHandlerMethods().handle(req, ctx, r);
            return;
        }
        if (!dataCapacity.tryAcquire()) {
            sendBusy(req, ctx, r, busy(LogReplicationBusyResponseMsg.Reason.OVERLOADED));
            return;
        }
        try {
            executor.execute(() -> {
                try {
                    getHandlerMethods().handle(req, ctx, r);
                } finally {
                    dataCapacity.release();
                }
            });
        } catch (RejectedExecutionException e) {
            dataCapacity.release();
            sendBusy(req, ctx, r, busy(LogReplicationBusyResponseMsg.Reason.OVERLOADED));
        }
    }

    private LogReplicationBusyResponseMsg busy(LogReplicationBusyResponseMsg.Reason reason) {
        return LogReplicationBusyResponseMsg.newBuilder().setReason(reason)
                .setSnapshotLease(sinkManager.getSnapshotLease()).setRetryAfterMs(BUSY_RETRY_AFTER_MS).build();
    }

    private void sendBusy(RequestMsg request, ChannelHandlerContext ctx, IServerRouter router,
                          LogReplicationBusyResponseMsg busy) {
        router.sendResponse(getResponseMsg(getHeaderMsg(request.getHeader()),
                ResponsePayloadMsg.newBuilder().setLrBusyResponse(busy).build()), ctx);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        executor.shutdown();
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

        if (isStandby.get() && isLeader(request, ctx, router, true)) {
            // Forward the received message to the Sink Manager for apply
            LogReplicationEntryMsg ack;
            try {
                ack = sinkManager.receive(request.getPayload().getLrEntry());
            } catch (LogReplicationBusyException e) {
                sendBusy(request, ctx, router, e.getResponse());
                return;
            }

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
        } else if (!isStandby.get()) {
            log.warn("Dropping log replication entry as this cluster's role is not Standby");
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
        log.debug("Log Replication Metadata Request received by Server.");

        if (!isLeader(request, ctx, router, false)) {
            log.warn("Dropping metadata request as this node is not the leader.");
            return;
        }
        if (!request.getPayload().getLrMetadataRequest().getSupportsSnapshotLifecycle()) {
            // The source predates the snapshot lease. It cannot decode this reply and will keep
            // retrying its negotiation; replication resumes once the source cluster is upgraded.
            if (unsupportedSourceReported.compareAndSet(false, true)) {
                log.warn("Rejecting a source that does not support the snapshot lease protocol. Replication "
                        + "stays paused until the source cluster is upgraded.");
            }
            sendBusy(request, ctx, router, busy(LogReplicationBusyResponseMsg.Reason.UNSUPPORTED_PROTOCOL));
            return;
        }
        unsupportedSourceReported.set(false);
        // The lease coordinator publishes this committed view once per second, independently of
        // source polls, so this handler never reads the store.
        LogReplicationMetadataResponseMsg status = metadataManager.getCachedSnapshotStatus();
        if (sinkManager.getSnapshotLease().getPhase() == SnapshotSyncLeaseRecord.Phase.NOT_READY) {
            // This node has not (re)acquired the lease yet. What it cached while it last led may be
            // stale, because another node may have led in between: report the lease as not ready, so
            // that the source asks again instead of deciding on it.
            status = status.toBuilder().setSnapshotLease(SnapshotSyncLeaseRecord.getDefaultInstance()).build();
        }
        router.sendResponse(getResponseMsg(getHeaderMsg(request.getHeader()), ResponsePayloadMsg.newBuilder()
                .setLrMetadataResponse(status).build()), ctx);
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
        if (!isStandby.get() && isLeader.get()) {
            log.warn("This node is the leader but the current role of the cluster is not STANDBY");
        }
        HeaderMsg responseHeader = getHeaderMsg(request.getHeader());
        ResponseMsg response = getLeadershipResponse(responseHeader, isLeader.get(), localNodeId, isStandby.get());
        router.sendResponse(response, ctx);
    }

    /* ************ Private / Utility Methods ************ */

    /**
     * Verify if current node is still the lead receiving node.
     *
     * @return true, if leader node.
     *         false, otherwise.
     */
    protected synchronized boolean isLeader(@Nonnull RequestMsg request,
                                            @Nonnull ChannelHandlerContext ctx,
                                            @Nonnull IServerRouter router, boolean isLogEntry) {
        // If the current cluster has switched to the active role (no longer the receiver) or it is no longer the leader,
        // skip message processing (drop received message) and nack on leadership (loss of leadership)
        // This will re-trigger leadership discovery on the sender.
        boolean lostLeadership = isActive.get() || !isLeader.get();

        if (lostLeadership) {

            if (isLogEntry) {
                LogReplicationEntryMsg entryMsg = request.getPayload().getLrEntry();
                LogReplicationEntryType entryType = entryMsg.getMetadata().getEntryType();
                log.warn("Received message of type {} while NOT LEADER. snapshotSyncSeqNumber={}, ts={}, syncRequestId={}", entryType,
                        entryMsg.getMetadata().getSnapshotSyncSeqNum(), entryMsg.getMetadata().getTimestamp(),
                        entryMsg.getMetadata().getSyncRequestId());
            }

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
        sinkManager.setLeadership(leader);
        isLeader.set(leader);
    }

    public void stopSink() {
        sinkManager.stopOnLeadershipLoss();
    }

    public synchronized void setActive(boolean active) {
        isActive.set(active);
        updateSinkRole();
    }

    public synchronized void setStandby(boolean standby) {
        isStandby.set(standby);
        updateSinkRole();
    }

    /** At startup only the role a cluster has is set, so the sink role follows from both flags. */
    private void updateSinkRole() {
        sinkManager.setSinkRole(isStandby.get() && !isActive.get());
    }
}
