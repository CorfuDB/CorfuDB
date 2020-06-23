package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.receive.LogReplicationSinkManager;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryMetadataResponse;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class represents the Log Replication Server, which is
 * responsible of providing Log Replication across sites.
 *
 * The Log Replication Server, handles log replication entries--which
 * represent parts of a Snapshot (full) sync or a Log Entry (delta) sync
 * and also handles negotiation messages, which allows the Source Replicator
 * to get a view of the last synchronized point at the remote site.
 */
@Slf4j
public class LogReplicationServer extends AbstractServer {

    private static final String configFilePath = "/config/corfu/log_replication_config.properties";

    private final ServerContext serverContext;

    //Used for receiving and applying messages.
    private final ExecutorService executor;

    @Getter
    private final LogReplicationSinkManager sinkManager;

    @Getter
    private final HandlerMethods handler = HandlerMethods.generateHandler(MethodHandles.lookup(), this);

    public LogReplicationServer(@Nonnull ServerContext context, LogReplicationConfig logReplicationConfig) {
        this.serverContext = context;

        /*
         * TODO xq: we need two thread pools
         * One thread pool use for communication and process short messages. If we have multiple threads in this pool
         * the SinkManager receive function need to be synchronzied.
         *
         * One pool is used to process the Snapshot full sync phase II: apply phase.
         *
         */

        this.executor = Executors.newFixedThreadPool(1,
                new ServerThreadFactory("LogReplicationServer-", new ServerThreadFactory.ExceptionHandler()));

        // TODO (hack): where can we obtain the local corfu endpoint? site manager? or can we always assume port is 9000?
        String corfuPort = serverContext.getLocalEndpoint().equals("localhost:9020") ? ":9001" : ":9000";
        String corfuEndpoint = serverContext.getNodeLocator().getHost() + corfuPort;
        log.info("Initialize Sink Manager with CorfuRuntime to {}", corfuEndpoint);
        this.sinkManager = new LogReplicationSinkManager(corfuEndpoint, logReplicationConfig);
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
        log.info("Log Replication Entry received by Server.");

        LogReplicationEntry ack = sinkManager.receive(msg.getPayload());

        if (ack != null) {
            long ts = ack.getMetadata().getMessageMetadataType().equals(MessageType.LOG_ENTRY_REPLICATED) ?
                    ack.getMetadata().getTimestamp() : ack.getMetadata().getSnapshotTimestamp();
            log.info("Sending ACK {} on {} to Client ", ack.getMetadata(), ts);
            r.sendResponse(ctx, msg, CorfuMsgType.LOG_REPLICATION_ENTRY.payloadMsg(ack));
        }
    }

    /**
     * This API is used by sender to query the log replication status at the receiver side.
     * It is used at the negotiation phase to decide to start a snapshot full sync or log entry sync.
     * It is also used during full snapshot sync while polling the receiver's status when the receiver is
     * applying the data to the real streams.
     * @param msg
     * @param ctx
     * @param r
     */
    @ServerHandler(type = CorfuMsgType.LOG_REPLICATION_QUERY_METADATA_REQUEST)
    private void handleLogReplicationQueryMetadataRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.info("Log Replication Query Metadata request received by Server.");
        LogReplicationQueryMetadataResponse response = sinkManager.processQueryMetadataRequest();
        r.sendResponse(ctx, msg, CorfuMsgType.LOG_REPLICATION_QUERY_METADATA_RESPONSE.payloadMsg(response));
    }

    @ServerHandler(type = CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP)
    private void handleLogReplicationQueryLeadership(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.info("******Log Replication Query Leadership Request received by Server.");
        LogReplicationQueryLeaderShipResponse resp = new LogReplicationQueryLeaderShipResponse(0, getSinkManager().isLeader());
        r.sendResponse(ctx, msg, CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP_RESPONSE.payloadMsg(resp));
    }
}
