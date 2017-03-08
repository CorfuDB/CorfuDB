package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Created by zlokhandwala on 3/7/17.
 */
public class SealLayoutServers {

    public CompletableFuture<Boolean> asyncSealLayoutServers(CorfuRuntime runtime, Layout layout, long sealEpoch) {

        CompletableFuture<Boolean> completableLayoutServersSeal = new CompletableFuture<>();

        CompletableFuture.supplyAsync(() -> {
            try {
                completableLayoutServersSeal.complete(sealLayoutServers(runtime, layout, sealEpoch));
            } catch (QuorumUnreachableException que) {
                completableLayoutServersSeal.completeExceptionally(que);
            }
            return completableLayoutServersSeal;
        });

        return completableLayoutServersSeal;
    }

    private Boolean sealLayoutServers (CorfuRuntime runtime, Layout layout, long sealEpoch)
            throws QuorumUnreachableException {

        // Seal layout servers
        CompletableFuture<Boolean>[] layoutSealFutures = layout.getLayoutServers().stream()
                .map(runtime::getRouter)
                .map(x -> x.getClient(BaseClient.class))
                .map(x -> x.setRemoteEpoch(sealEpoch))
                .toArray(CompletableFuture[]::new);

        Boolean success = false;
        QuorumFutureFactory.CompositeFuture<Boolean> quorumFuture =
                QuorumFutureFactory.getQuorumFuture(Boolean::compareTo, layoutSealFutures);
        try {
            success = quorumFuture.get();
        } catch (ExecutionException | InterruptedException e) {
            if (e.getCause() instanceof QuorumUnreachableException) {
                throw (QuorumUnreachableException) e.getCause();
            }
        }
        int reachableServers = (int) Arrays.stream(layoutSealFutures)
                .filter(booleanCompletableFuture -> !booleanCompletableFuture.isCompletedExceptionally()).count();

        if (!success) throw new QuorumUnreachableException(reachableServers, layoutSealFutures.length);

        return success;
    }
}
