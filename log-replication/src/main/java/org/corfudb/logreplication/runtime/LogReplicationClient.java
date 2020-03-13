package org.corfudb.logreplication.runtime;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationNegotiationResponse;
import org.corfudb.runtime.clients.AbstractClient;
import org.corfudb.runtime.clients.IClientRouter;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class LogReplicationClient extends AbstractClient {

    @Getter
    @Setter
    private IClientRouter router;

    public LogReplicationClient(IClientRouter router, long epoch) {
        super(router, epoch);
        setRouter(router);
    }

    public LogReplicationClient(IClientRouter router) {
        this(router, 0);
    }

    public CompletableFuture<LogReplicationNegotiationResponse> sendNegotiationRequest() {
        return getRouter().sendMessageAndGetCompletable(
                new CorfuMsg(CorfuMsgType.LOG_REPLICATION_NEGOTIATION_REQUEST).setEpoch(0));
    }

    public boolean sendSnapshotSync(LogReplicationEntry logReplicationEntry) {
        try {
            log.info("Send log entry message...");
            CorfuMsg msg = new CorfuPayloadMsg<>(CorfuMsgType.LOG_REPLICATION_ENTRY, logReplicationEntry).setEpoch(0);
            getRouter().sendMessage(msg);
            return true;
        } catch (Exception e) {
            log.error("Exception caught while sending snapshot sync message", e);
            return false;
        }
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
