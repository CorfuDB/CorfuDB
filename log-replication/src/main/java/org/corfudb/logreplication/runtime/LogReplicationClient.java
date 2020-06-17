package org.corfudb.logreplication.runtime;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.infrastructure.CorfuReplicationDiscoveryService;
import org.corfudb.logreplication.infrastructure.DiscoveryServiceEvent;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryMetadataResponse;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationQueryLeaderShipResponse;
import org.corfudb.runtime.clients.AbstractClient;
import org.corfudb.runtime.clients.IClientRouter;

import java.util.concurrent.CompletableFuture;

@Slf4j
public class LogReplicationClient extends AbstractClient {

    @Getter
    @Setter
    private IClientRouter router;

    private CorfuReplicationDiscoveryService discoveryService;

    private String remoteSiteID;

    public LogReplicationClient(IClientRouter router, long epoch) {
        super(router, epoch, null);
        setRouter(router);
    }

    public LogReplicationClient(IClientRouter router, CorfuReplicationDiscoveryService discoveryService, String remoteSiteID) {
        this(router, 0);
        this.discoveryService = discoveryService;
        this.remoteSiteID = remoteSiteID;
    }

    public CompletableFuture<LogReplicationQueryMetadataResponse> sendQueryMetadata() {
        return getRouter().sendMessageAndGetCompletable(
                new CorfuMsg(CorfuMsgType.LOG_REPLICATION_QUERY_METADATA_REQUEST).setEpoch(0));
    }

    public CompletableFuture<LogReplicationQueryLeaderShipResponse> sendQueryLeadership() {
        return getRouter().sendMessageAndGetCompletable(
                new CorfuMsg(CorfuMsgType.LOG_REPLICATION_QUERY_LEADERSHIP).setEpoch(0));
    }

    public CompletableFuture<LogReplicationEntry> sendLogEntry(LogReplicationEntry logReplicationEntry) {
        CompletableFuture<LogReplicationEntry> result = null;
        CorfuMsg msg = new CorfuPayloadMsg<>(CorfuMsgType.LOG_REPLICATION_ENTRY, logReplicationEntry).setEpoch(0);
        try {
            result = getRouter().sendMessageAndGetCompletable(msg);
        } catch (Exception e) {
            discoveryService.putEvent(new DiscoveryServiceEvent(DiscoveryServiceEvent.DiscoveryServiceEventType.ConnectionLoss, remoteSiteID));
        } finally {
            return result;
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
