package org.corfudb.runtime.clients;

import java.util.concurrent.CompletableFuture;

import lombok.Getter;
import lombok.Setter;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.PriorityLevel;

/**
 * Abstract clients stamped with an epoch to send messages stamped with the required epoch.
 *
 * <p>Created by zlokhandwala on 3/9/18.
 */
public abstract class AbstractClient implements IClient {

    @Getter
    private final long epoch;

    @Getter
    @Setter
    private IClientRouter router;

    @Setter
    private PriorityLevel priorityLevel = PriorityLevel.NORMAL;

    public AbstractClient(IClientRouter router, long epoch) {
        this.router = router;
        this.epoch = epoch;
    }

    <T> CompletableFuture<T> sendMessageWithFuture(CorfuMsg msg) {
        return router.sendMessageAndGetCompletable(msg.setEpoch(epoch)
                .setPriorityLevel(priorityLevel));
    }
}
