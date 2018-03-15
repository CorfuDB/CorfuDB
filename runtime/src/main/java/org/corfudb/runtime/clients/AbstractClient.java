package org.corfudb.runtime.clients;

import java.util.concurrent.CompletableFuture;

import lombok.Getter;
import lombok.Setter;

import org.corfudb.protocols.wireprotocol.CorfuMsg;

/**
 * Abstract clients stamped with an epoch to send messages stamped with the required epoch.
 *
 * <p>Created by zlokhandwala on 3/9/18.
 */
abstract class AbstractClient implements IClient {

    @Getter
    private final long epoch;

    @Getter
    @Setter
    private IClientRouter router;

    AbstractClient(IClientRouter router, long epoch) {
        this.router = router;
        this.epoch = epoch;
    }

    <T> CompletableFuture<T> sendMessageWithFuture(CorfuMsg msg) {
        return router.sendMessageAndGetCompletable(msg.setEpoch(epoch));
    }

    public void sendMessage(CorfuMsg msg) {
        router.sendMessage(msg.setEpoch(epoch));
    }
}
