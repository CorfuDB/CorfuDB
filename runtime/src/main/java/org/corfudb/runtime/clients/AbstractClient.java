package org.corfudb.runtime.clients;

import java.util.concurrent.CompletableFuture;
import java.util.UUID;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.service.CorfuProtocolMessage.ClusterIdCheck;
import org.corfudb.protocols.service.CorfuProtocolMessage.EpochCheck;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.PriorityLevel;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;

import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolMessage.priorityTypeMap;

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

    @Deprecated
    <T> CompletableFuture<T> sendMessageWithFuture(CorfuMsg msg) {
        return router.sendMessageAndGetCompletable(
                msg.setEpoch(epoch).setClusterID(clusterID).setPriorityLevel(priorityLevel));
    }

    <T> CompletableFuture<T> sendRequestWithFuture(RequestPayloadMsg payload,
                                                   ClusterIdCheck ignoreClusterId, EpochCheck ignoreEpoch) {
        CorfuMessage.PriorityLevel protoPriorityLevel =
                priorityTypeMap.getOrDefault(priorityLevel, CorfuMessage.PriorityLevel.NORMAL);

        return router.sendRequestAndGetCompletable(payload, epoch,
                getUuidMsg(clusterID), protoPriorityLevel, ignoreClusterId, ignoreEpoch);
    }
}
