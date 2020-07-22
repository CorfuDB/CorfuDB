package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationLeadershipLoss;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryMetadataResponse;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

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

    private final ServerContext serverContext;

    // Used for receiving and applying messages.
    private final ExecutorService executor;

    @Getter
    private final LogReplicationMetadataManager metadataManager;

    @Getter
    private final LogReplicationSinkManager sinkManager;

    private final AtomicBoolean isLeader = new AtomicBoolean(false);

    private final AtomicBoolean isActive = new AtomicBoolean(false);

    @Getter
    private final HandlerMethods handler = HandlerMethods.generateHandler(MethodHandles.lookup(), this);

    public LogReplicationServer(@Nonnull ServerContext context, @Nonnull  LogReplicationConfig logReplicationConfig,
                                @Nonnull LogReplicationMetadataManager metadataManager, String corfuEndpoint,
                                long topologyConfigId) {
        this.serverContext = context;
        this.metadataManager = metadataManager;
        this.sinkManager = new LogReplicationSinkManager(corfuEndpoint, logReplicationConfig, metadataManager, serverContext, topologyConfigId);
        this.executor = Executors.newFixedThreadPool(1,
                new ServerThreadFactory("LogReplicationServer-", new ServerThreadFactory.ExceptionHandler()));
    }

    /* ************ Override Methods ************ */

    @Override
    public boolean isServerReadyToHandleMsg(CorfuMsg msg) {
        return getState() == ServerState.READY;
    }

    @Override
    protected void processRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        executor.submit(() -> getHandler().handle(msg, ctx, r));
    }

    @Override
    public void shutdown() {
        super.shutdown();
        executor.shutdown();
    }

    /* ************ Server Handlers ************ */

    @ServerHandler(type = CorfuMsgType.LOG_REPLICATION_ENTRY)
    private void handleLogReplicationEntry(CorfuPayloadMsg<LogReplicationEntry> msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.trace("Log Replication Entry received by Server.");

        if (isLeader(msg, r)) {
            // Forward the received message to the Sink Manager for apply
            LogReplicationEntry ack = sinkManager.receive(msg.getPayload());

            if (ack != null) {
                long ts = ack.getMetadata().getMessageMetadataType().equals(MessageType.LOG_ENTRY_REPLICATED) ?
                        ack.getMetadata().getTimestamp() : ack.getMetadata().getSnapshotTimestamp();
                log.info("Sending ACK {} on {} to Client ", ack.getMetadata(), ts);
                r.sendResponse(msg, CorfuMsgType.LOG_REPLICATION_ENTRY.payloadMsg(ack));
            }
        }
    }

    /**
     * Server handler to process log replication status queries.
     * It is used at the negotiation phase to decide to start a snapshot full sync or log entry sync.
     * It is also used during full snapshot sync while polling the receiver's status when the receiver is
     * applying the data to the real streams.
     * @param msg
     * @param ctx
     * @param r
     */
    @ServerHandler(type = CorfuMsgType.LOG_REPLICATION_QUERY_METADATA_REQUEST)
    private void handleLogReplicationQueryMetadataRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.info("Log Replication Negotiation Request received by Server.");

        if (isLeader(msg, r)) {
            LogReplicationMetadataManager metadata = sinkManager.getLogReplicationMetadataManager();

            // TODO (Xiaoqin Ma): That's 6 independent DB calls per one LOG_REPLICATION_NEGOTIATION_REQUEST.
            //  Can we do just one? Also, It does not look like we handle failures if one of them fails, for example.
            LogReplicationQueryMetadataResponse response = new LogReplicationQueryMetadataResponse(
                    metadata.getTopologyConfigId(),
                    metadata.getVersion(),
                    metadata.getLastSnapStartTimestamp(),
                    metadata.getLastSnapTransferDoneTimestamp(),
                    metadata.getLastSrcBaseSnapshotTimestamp(),
                    metadata.getLastProcessedLogTimestamp());
            log.info("Send Negotiation response");
            r.sendResponse(msg, CorfuMsgType.LOG_REPLICATION_QUERY_METADATA_RESPONSE.payloadMsg(response));
        } else {
            log.warn("Dropping negotiation request as this node is not the leader.");
        }
    }

    @ServerHandler(type = CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP)
    private void handleLogReplicationQueryLeadership(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.info("Log Replication Query Leadership Request received by Server.");
        LogReplicationQueryLeaderShipResponse resp = new LogReplicationQueryLeaderShipResponse(0,
                isLeader.get(), serverContext.getLocalEndpoint());
        r.sendResponse(msg, CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP_RESPONSE.payloadMsg(resp));
    }

    /* ************ Private / Utility Methods ************ */

    /**
     * Verify if current node is still the lead receiving node.
     *
     * @return true, if leader node.
     *         false, otherwise.
     */
    private synchronized boolean isLeader(CorfuMsg msg, IServerRouter r) {
        // If the current cluster has switched to the active role (no longer the receiver) or it is no longer the leader,
        // skip message processing (drop received message) and nack on leadership (loss of leadership)
        // This will re-trigger leadership discovery on the sender.
        boolean lostLeadership = isActive.get() || !isLeader.get();

        if (lostLeadership) {
            log.warn("This node has changed, active={}, leader={}. Dropping message type={}, id={}", isActive.get(),
                    isLeader.get(), msg.getMsgType(), msg.getRequestID());
            LogReplicationLeadershipLoss payload = new LogReplicationLeadershipLoss(serverContext.getLocalEndpoint());
            r.sendResponse(msg, CorfuMsgType.LOG_REPLICATION_LEADERSHIP_LOSS.payloadMsg(payload));
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
