package org.corfudb.runtime.view;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;

import java.util.concurrent.CompletableFuture;

/**
 * Seals all the log unit servers in every stripe in the specified segment following a certain replication method.
 *
 * Created by zlokhandwala on 3/7/17.
 */
public interface ISealReplicationView {

    /**
     * Seals the layout segment by attempting to seal all log unit servers in every stripe in the segment.
     * It however waits only for a set of responses which is decided upon the replication mode.
     * @param runtime       Runtime to seal the servers.
     * @param layoutSegment Layout Segment to be sealed.
     * @param sealEpoch     Epoch to which the servers are to be sealed.
     * @return  Completable Future which completes if segment is sealed else it completes exceptionally.
     */
    CompletableFuture<Boolean> asyncSealLayoutSegment(CorfuRuntime runtime, Layout.LayoutSegment layoutSegment, long sealEpoch);

    /**
     * Seals the layout segment by attempting to seal all log unit servers in every stripe in the segment.
     * It however waits only for a set of responses which is decided upon the replication mode.
     * @param runtime       Runtime to seal the servers.
     * @param layoutSegment Layout Segment to be sealed.
     * @param sealEpoch     Epoch to which the servers are to be sealed.
     * @return  true if sealed
     * @throws QuorumUnreachableException Thrown if quorum not possible while sealing.
     */
    Boolean sealLayoutSegment(CorfuRuntime runtime, Layout.LayoutSegment layoutSegment, long sealEpoch) throws QuorumUnreachableException;
}
