package org.corfudb.infrastructure.logreplication.runtime;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationMetadataResponse;
import org.corfudb.runtime.clients.AbstractClient;
import org.corfudb.runtime.clients.IClientRouter;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

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

    public LogReplicationClient(IClientRouter router, String clusterId) {
        super(router, 0, UUID.fromString(clusterId));
        setRouter(router);
    }

    public LogReplicationClient(IClientRouter router, long epoch) {
        super(router, epoch, null);
        setRouter(router);
    }

    public CompletableFuture<LogReplicationMetadataResponse> sendMetadataRequest() {
        return getRouter().sendMessageAndGetCompletable(
                    new CorfuMsg(CorfuMsgType.LOG_REPLICATION_METADATA_REQUEST).setEpoch(0));
    }

    public CompletableFuture<LogReplicationEntry> sendLogEntry(LogReplicationEntry logReplicationEntry) {
        CorfuMsg msg = new CorfuPayloadMsg<>(CorfuMsgType.LOG_REPLICATION_ENTRY, logReplicationEntry).setEpoch(0);
        return getRouter().sendMessageAndGetCompletable(msg);
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
