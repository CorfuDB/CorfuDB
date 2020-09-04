package org.corfudb.infrastructure.logreplication.runtime;

import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationMetadataResponse;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;
import org.corfudb.runtime.Messages;
import org.corfudb.runtime.clients.ClientHandler;
import org.corfudb.runtime.clients.ClientMsgHandler;
import org.corfudb.runtime.clients.IClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.IHandler;

import java.lang.invoke.MethodHandles;
import java.util.UUID;


/**
 * A client to the Log Replication Server
 */
@Slf4j
public class LogReplicationHandler implements IClient, IHandler<LogReplicationClient> {

    @Setter
    @Getter
    private IClientRouter router;

    @Getter
    public ClientMsgHandler msgHandler = new ClientMsgHandler(this)
            .generateHandlers(MethodHandles.lookup(), this);

    /**
     * Handle an ACK from Log Replication server.
     *
     * @param msg The ack message
     * @param ctx The context the message was sent under
     * @param r   A reference to the router
     */
    @ClientHandler(type = CorfuMsgType.LOG_REPLICATION_ENTRY)
    private static Object handleLogReplicationAck(CorfuPayloadMsg<LogReplicationEntry> msg,
                                                  ChannelHandlerContext ctx, IClientRouter r) {
        log.debug("Handle log replication ACK");
        return msg.getPayload();
    }

    @ClientHandler(type = CorfuMsgType.LOG_REPLICATION_METADATA_RESPONSE)
    private static Object handleLogReplicationMetadata(CorfuPayloadMsg<LogReplicationMetadataResponse> msg,
                                                       ChannelHandlerContext ctx, IClientRouter r) {
        log.debug("Handle log replication Metadata Response");
        return msg.getPayload();
    }

    @ClientHandler(type = CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP_RESPONSE)
    private static Object handleLogReplicationQueryLeadershipResponse(CorfuPayloadMsg<LogReplicationQueryLeaderShipResponse> msg,
                                                                      ChannelHandlerContext ctx, IClientRouter r) {
        log.trace("Handle log replication query leadership response msg {}", msg);
        return msg.getPayload();
    }

    @ClientHandler(type = CorfuMsgType.LOG_REPLICATION_LEADERSHIP_LOSS)
    private static Object handleLogReplicationLeadershipLoss(CorfuPayloadMsg<Messages.LogReplicationLeadershipLoss> msg,
                                                                      ChannelHandlerContext ctx, IClientRouter r) {
        log.debug("Handle log replication leadership loss msg {}", msg);
        return msg.getPayload();
    }

    @Override
    public LogReplicationClient getClient(long epoch, UUID clusterID) {
        return new LogReplicationClient(router, epoch);
    }
}
