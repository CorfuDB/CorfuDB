package org.corfudb.infrastructure;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.SinkManager;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationNegotiationResponse;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
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

    private final ExecutorService executor;

    @Getter
    private final SinkManager sinkManager;

    @Getter
    private final HandlerMethods handler = HandlerMethods.generateHandler(MethodHandles.lookup(), this);

    public LogReplicationServer(@Nonnull ServerContext context) {
        this.serverContext = context;
        this.executor = Executors.newFixedThreadPool(1,
                new ServerThreadFactory("LogReplicationServer-", new ServerThreadFactory.ExceptionHandler()));
        LogReplicationConfig config = getDefaultLogReplicationConfig();

        // TODO (hack): where can we obtain the local corfu endpoint? site manager? or can we always assume port is 9000?
        String corfuPort = serverContext.getLocalEndpoint().equals("localhost:9020") ? ":9001" : ":9000";
        String corfuEndpoint = serverContext.getNodeLocator().getHost() + corfuPort;
        log.info("Initialize Sink Manager with CorfuRuntime to {}", corfuEndpoint);
        this.sinkManager = new SinkManager(corfuEndpoint, config);
    }

    private LogReplicationConfig getDefaultLogReplicationConfig() {
        Set<String> streamsToReplicate = new HashSet<>(Arrays.asList("Table001", "Table002", "Table003"));
        return new LogReplicationConfig(streamsToReplicate, UUID.randomUUID(), UUID.randomUUID());
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
            log.info("Sending ACK {} on {} to Client", ack.getMetadata().getMessageMetadataType(), ts);
            r.sendResponse(ctx, msg, CorfuMsgType.LOG_REPLICATION_ENTRY.payloadMsg(ack));
        }
    }

    @ServerHandler(type = CorfuMsgType.LOG_REPLICATION_NEGOTIATION_REQUEST)
    private void handleLogReplicationNegotiationRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.info("Log Replication Negotiation Request received by Server.");
        LogReplicationNegotiationResponse response = new LogReplicationNegotiationResponse(0, 0);
        r.sendResponse(ctx, msg, CorfuMsgType.LOG_REPLICATION_NEGOTIATION_RESPONSE.payloadMsg(response));
    }

    @ServerHandler(type = CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP)
    private void handleLogReplicationQueryLeadership(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.info("******Log Replication Query Leadership Request received by Server.");
        //TODO: setup
        LogReplicationQueryLeaderShipResponse resp = new LogReplicationQueryLeaderShipResponse(0, true);
        r.sendResponse(ctx, msg, CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP_RESPONSE.payloadMsg(resp));
    }
}
