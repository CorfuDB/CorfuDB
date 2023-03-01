package org.corfudb.infrastructure.logreplication.runtime;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataRequestMsg;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.clients.AbstractClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.proto.service.CorfuMessage;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.corfudb.infrastructure.logreplication.runtime.LogReplicationSourceClientRouter.REMOTE_LEADER;

/**
 * A client to send messages to the Log Replication Unit.
 *
 * This class provides access to operations on a remote server
 * for the purpose of log replication.
 *
 * @author amartinezman
 */
@Slf4j
public class LogReplicationClient extends AbstractClient {

    @Getter
    @Setter
    private IClientRouter router;

    private final LogReplicationSession replicationSession;

    public LogReplicationClient(IClientRouter router, LogReplicationSession replicationSession) {
        super(router, 0, UUID.fromString(replicationSession.getSinkClusterId()));
        this.replicationSession = replicationSession;
        setRouter(router);
    }

    public LogReplicationClient(IClientRouter router, long epoch, LogReplicationSession replicationSession) {
        super(router, epoch, null);
        this.replicationSession = replicationSession;
        setRouter(router);
    }

    public CompletableFuture<LogReplicationMetadataResponseMsg> sendMetadataRequest() {
        CorfuMessage.RequestPayloadMsg payload =
                CorfuMessage.RequestPayloadMsg.newBuilder()
                        .setLrMetadataRequest(LogReplicationMetadataRequestMsg.newBuilder().build())
                        .build();
        return getRouter().sendRequestAndGetCompletable(payload, REMOTE_LEADER);
    }

    public CompletableFuture<LogReplicationEntryMsg> sendLogEntry(
            LogReplicationEntryMsg logReplicationEntry) {
        CorfuMessage.RequestPayloadMsg payload =
                CorfuMessage.RequestPayloadMsg.newBuilder()
                        .setLrEntry(logReplicationEntry)
                        .build();
        return getRouter().sendRequestAndGetCompletable(payload, REMOTE_LEADER);
    }

    @Override
    public void setRouter(IClientRouter router) {
        this.router = router;
    }

    @Override
    public IClientRouter getRouter() {
        return router;
    }

}
