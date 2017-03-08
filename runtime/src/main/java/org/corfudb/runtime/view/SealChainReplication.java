package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.util.CFUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Seals the Log unit servers in the Chain Replication View
 * <p>
 * Created by zlokhandwala on 3/7/17.
 */
@Slf4j
public class SealChainReplication implements ISealReplicationView {

    /**
     * Seals the Layout Segment if all log unit servers are sealed in every stripe.
     * Else completes exceptionally
     *
     * @param runtime       Runtime to set remote epoch for log unit servers.
     * @param layoutSegment Layout segment to be sealed.
     * @param sealEpoch     Epoch number to seal the servers with
     * @return Completable future completing on successful seal.
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
     * Seals the Layout Segment if atl east one log unit server is sealed in every stripe.
     * Else throws exception
     *
     * @param runtime       Runtime to set remote epoch for log unit servers.
     * @param layoutSegment Layout segment to be sealed.
     * @param sealEpoch     Epoch number to seal the servers with
     * @return True if seal successful
     * @throws QuorumUnreachableException Thrown if seal unsuccessful
     */
    @Override
    public Boolean sealLayoutSegment(CorfuRuntime runtime, Layout.LayoutSegment layoutSegment, long sealEpoch) throws QuorumUnreachableException {

        for (Layout.LayoutStripe layoutStripe : layoutSegment.getStripes()) {
            try {
                for (String logServer : layoutStripe.getLogServers()) {
                    CFUtils.getUninterruptibly(runtime.getRouter(logServer).getClient(BaseClient.class).setRemoteEpoch(sealEpoch), TimeoutException.class);
                }
            } catch (TimeoutException te) {
                throw new QuorumUnreachableException(0, layoutStripe.getLogServers().size());
            }
        }
        return true;
    }
}
