package org.corfudb.runtime.clients;

import java.util.concurrent.CompletableFuture;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;

import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.runtime.proto.service.CorfuMessage.PriorityLevel;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;

import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;

/**
 * Abstract clients stamped with an epoch to send messages stamped with the required epoch.
 *
 * <p>Created by zlokhandwala on 3/9/18.
 */
public abstract class AbstractClient implements IClient {

    @Getter
    private final long epoch;

    @Getter
    private final UUID clusterID;

    @Getter
    @Setter
    private IClientRouter router;

    @Setter
    private PriorityLevel priorityLevel = PriorityLevel.NORMAL;

    public AbstractClient(IClientRouter router, long epoch, UUID clusterID) {
        this.router = router;
        this.epoch = epoch;
        this.clusterID = clusterID;
    }

    <T> CompletableFuture<T> sendRequestWithFuture(RequestPayloadMsg payload,
                                                   ClusterIdCheck ignoreClusterId, EpochCheck ignoreEpoch) {
        return router.sendRequestAndGetCompletable(payload, epoch,
                getUuidMsg(clusterID), priorityLevel, ignoreClusterId, ignoreEpoch);
    }
}
