package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableList;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegment;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CurrentTransferSegmentStatus;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.RESTORED;
import static org.corfudb.runtime.view.Address.NON_ADDRESS;

/**
 * A redundancy calculator that also considers a prefix trim.
 */
public class PrefixTrimRedundancyCalculator extends RedundancyCalculator {

    private final CorfuRuntime runtime;

    public PrefixTrimRedundancyCalculator(String node, CorfuRuntime runtime) {
        super(node);
        this.runtime = runtime;
    }

    /**
     * Sets the trim mark on this log unit and also performs a prefix trim.
     *
     * @param layout   A current layout.
     * @param runtime  A current runtime.
     * @param endpoint A current endpoint.
     * @return A retrieved trim mark.
     */
    long setTrimOnNewLogUnit(Layout layout, CorfuRuntime runtime,
                             String endpoint) {

        long trimMark = runtime.getAddressSpaceView().getTrimMark().getSequence();

        Token prefixToken = new Token(layout.getEpoch(), trimMark - 1);
        CFUtils.getUninterruptibly(runtime.getLayoutView().getRuntimeLayout(layout)
                .getLogUnitClient(endpoint)
                .prefixTrim(prefixToken));
        return trimMark;
    }

    /**
     * Given a layout, creates an initial list of transfer segments.
     *
     * @param layout A current layout.
     * @return A list of transfer segments.
     */
    public ImmutableList<CurrentTransferSegment> createStateList(Layout layout) {
        long trimMark = setTrimOnNewLogUnit(layout, runtime, getServer());

        return layout.getSegments()
                .stream()
                // filter the segments that end before the trim mark and are not open.
                .filter(segment -> segment.getEnd() >= trimMark && segment.getEnd() != NON_ADDRESS)
                .map(segment -> {
                    long segmentStart = Math.max(segment.getStart(), trimMark);
                    long segmentEnd = segment.getEnd() - 1L;

                    if (segmentContainsServer(segment, getServer())) {
                        CurrentTransferSegmentStatus restored = CurrentTransferSegmentStatus
                                .builder()
                                .segmentState(RESTORED)
                                .totalTransferred(segmentEnd - segmentStart + 1L)
                                .build();

                        return CurrentTransferSegment
                                .builder()
                                .startAddress(segmentStart)
                                .endAddress(segmentEnd)
                                .completedStatus(restored)
                                .build();
                    } else {
                        CurrentTransferSegmentStatus notTransferred = CurrentTransferSegmentStatus
                                .builder()
                                .segmentState(NOT_TRANSFERRED)
                                .totalTransferred(0L)
                                .build();

                        return CurrentTransferSegment
                                .builder()
                                .startAddress(segmentStart)
                                .endAddress(segmentEnd)
                                .completedStatus(notTransferred)
                                .build();
                    }

                })
                .collect(ImmutableList.toImmutableList());
    }

}
