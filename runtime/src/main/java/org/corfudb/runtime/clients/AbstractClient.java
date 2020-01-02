package org.corfudb.runtime.clients;

import java.util.concurrent.CompletableFuture;

import com.codahale.metrics.Timer;
import lombok.Getter;
import lombok.Setter;

import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.PriorityLevel;
import org.corfudb.util.MetricsUtils;

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

    @Setter
    private PriorityLevel priorityLevel = PriorityLevel.NORMAL;

    AbstractClient(IClientRouter router, long epoch) {
        this.router = router;
        this.epoch = epoch;
    }

    <T> CompletableFuture<T> sendMessageWithFuture(CorfuMsg msg) {
        return router.sendMessageAndGetCompletable(msg.setEpoch(epoch)
                .setPriorityLevel(priorityLevel));
    }

    <T> CompletableFuture<T> sendMessageWithFuture(CorfuMsg msg, Timer timer) {
        Timer.Context context = MetricsUtils.getConditionalContext(timer);
        CorfuMsg message = msg.setEpoch(epoch).setPriorityLevel(priorityLevel);
        return router
                .<T>sendMessageAndGetCompletable(message)
                .thenApply(token -> {
                    MetricsUtils.stopConditionalContext(context);
                    return token;
                });
    }

    public void sendMessage(CorfuMsg msg) {
        router.sendMessage(msg.setEpoch(epoch)
                .setPriorityLevel(priorityLevel));
    }
}
