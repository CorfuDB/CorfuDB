package org.corfudb.runtime.clients;

import lombok.Getter;
import lombok.Setter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.PriorityLevel;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Abstract clients stamped with an epoch to send messages stamped with the required epoch.
 *
 * <p>Created by zlokhandwala on 3/9/18.
 */
abstract class AbstractClient implements IClient {

    @Getter
    private final long epoch;

    @Getter
    private final UUID clusterID;

    @Getter
    @Setter
    private IClientRouter router;

    @Setter
    private PriorityLevel priorityLevel = PriorityLevel.NORMAL;

    AbstractClient(IClientRouter router, long epoch, UUID clusterID) {
        this.router = router;
        this.epoch = epoch;
        this.clusterID = clusterID;
    }

    <T> CompletableFuture<T> sendMessageWithFuture(CorfuMsg msg) {
        return router.sendMessageAndGetCompletable(
                msg.setEpoch(epoch).setClusterID(clusterID).setPriorityLevel(priorityLevel));
    }
}
