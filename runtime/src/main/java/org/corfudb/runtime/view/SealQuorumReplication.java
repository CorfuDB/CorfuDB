package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Seals the Log unit servers in the Quorum Replication View
 *
 * Created by zlokhandwala on 3/7/17.
 */
public class SealQuorumReplication implements ISealReplicationView {

    /**
     * Seals the Layout Segment if quorum of log unit servers are sealed in every stripe.
     * Else completes exceptionally
     *
     * @param runtime       Runtime to set remote epoch for log unit servers.
     * @param layoutSegment Layout segment to be sealed.
     * @param sealEpoch     Epoch number to seal the servers with
     * @return  Completable future completing on successful seal.
     */
    @Override
    public CompletableFuture<Boolean> asyncSealLayoutSegment(CorfuRuntime runtime, Layout.LayoutSegment layoutSegment, long sealEpoch) {

        CompletableFuture<Boolean> completableLayoutSegmentSeal = new CompletableFuture<>();

        CompletableFuture.supplyAsync(() -> {
            try {
                completableLayoutSegmentSeal.complete(sealLayoutSegment(runtime, layoutSegment, sealEpoch));
            } catch (QuorumUnreachableException que) {
                completableLayoutSegmentSeal.completeExceptionally(que);
            }
            return completableLayoutSegmentSeal;
        });

        return completableLayoutSegmentSeal;
    }

    /**
     * Seals the Layout Segment if quorum of log unit servers are sealed in every stripe.
     * Else throws exception
     *
     * @param runtime       Runtime to set remote epoch for log unit servers.
     * @param layoutSegment Layout segment to be sealed.
     * @param sealEpoch     Epoch number to seal the servers with
     * @return  True if seal successful
     * @throws QuorumUnreachableException   Thrown if seal unsuccessful
     */
    @Override
    public Boolean sealLayoutSegment(CorfuRuntime runtime, Layout.LayoutSegment layoutSegment, long sealEpoch) throws QuorumUnreachableException {
        Boolean success;

        for (Layout.LayoutStripe layoutStripe : layoutSegment.getStripes()) {
            success = false;
            CompletableFuture<Boolean>[] logUnitSeaFutures = layoutStripe.getLogServers().stream()
                    .map(runtime::getRouter)
                    .map(x -> x.getClient(BaseClient.class))
                    .map(x -> x.setRemoteEpoch(sealEpoch))
                    .toArray(CompletableFuture[]::new);

            QuorumFutureFactory.CompositeFuture<Boolean> quorumFuture =
                    QuorumFutureFactory.getQuorumFuture(Boolean::compareTo, logUnitSeaFutures);
            try {
                success = quorumFuture.get();
            } catch (ExecutionException | InterruptedException e) {
                if (e.getCause() instanceof QuorumUnreachableException) {
                    throw (QuorumUnreachableException) e.getCause();
                }
            }

            int reachableServers = (int) Arrays.stream(logUnitSeaFutures)
                    .filter(booleanCompletableFuture -> !booleanCompletableFuture.isCompletedExceptionally()).count();

            if (!success) throw new QuorumUnreachableException(reachableServers, logUnitSeaFutures.length);
        }
        return true;
    }
}
