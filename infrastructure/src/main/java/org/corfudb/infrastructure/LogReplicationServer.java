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

import javax.annotation.Nonnull;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.File;

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

    private SinkManager sinkManager;

    @Getter
    private final HandlerMethods handler = HandlerMethods.generateHandler(MethodHandles.lookup(), this);

    @Override
    public boolean isServerReadyToHandleMsg(CorfuMsg msg) {
        return getState() == ServerState.READY;
    }

    public LogReplicationServer(@Nonnull ServerContext context) {
        this.serverContext = context;
        this.executor = Executors.newFixedThreadPool(1,
                new ServerThreadFactory("LogReplicationServer-", new ServerThreadFactory.ExceptionHandler()));
        // Todo (hack): remove this if condition, can we always assume that Corfu's port is 9000
        //  or will this be available through the Site Manager?
        if (serverContext.getLocalEndpoint().equals("localhost:9020")) {
            log.info("Initialize Sink Manager with Runtime to {}", serverContext.getNodeLocator().getHost() + ":9001");
            LogReplicationConfig config = getLogReplicationConfig();
            // TODO (hack): where can we obtain local corfu endpoint?
            this.sinkManager = new SinkManager(serverContext.getNodeLocator().getHost() + ":9001", config);
        }
    }

    private LogReplicationConfig getLogReplicationConfig() {
        return LogReplicationConfig.fromFile(configFilePath);
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

    @ServerHandler(type = CorfuMsgType.LOG_REPLICATION_ENTRY)
    private void handleLogReplicationEntry(CorfuPayloadMsg<LogReplicationEntry> msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.info("Log Replication Entry received by Server.");

        LogReplicationEntry ack = sinkManager.receive(msg.getPayload());

        if (ack != null) {
            log.info("Sending ACK to Client");
            r.sendResponse(ctx, msg, CorfuMsgType.LOG_REPLICATION_ENTRY.payloadMsg(ack));
        }
    }

    @ServerHandler(type = CorfuMsgType.LOG_REPLICATION_NEGOTIATION_REQUEST)
    private void handleLogReplicationNegotiationRequest(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        log.info("Log Replication Negotiation Request received by Server.");
        LogReplicationNegotiationResponse response = new LogReplicationNegotiationResponse(0, 0);
        r.sendResponse(ctx, msg, CorfuMsgType.LOG_REPLICATION_NEGOTIATION_RESPONSE.payloadMsg(response));
    }
}
