package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Seals the Log unit servers in the Chain Replication View
 *
 * Created by zlokhandwala on 3/7/17.
 */
@Slf4j
public class SealChainReplication implements ISealReplicationView {

    /**
     * Seals the Layout Segment if atl east one log unit server is sealed in every stripe.
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
                    QuorumFutureFactory.getFirstWinsFuture(Boolean::compareTo, logUnitSeaFutures);
            try {
                success = quorumFuture.get();
            } catch (ExecutionException | InterruptedException e) {
                log.error("Seal Segment future exception : {}", e);
            }
            if (!success) throw new QuorumUnreachableException(0, logUnitSeaFutures.length);
        }
        return true;
    }
}
