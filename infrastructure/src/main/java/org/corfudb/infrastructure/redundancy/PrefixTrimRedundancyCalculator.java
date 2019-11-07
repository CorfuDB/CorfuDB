package org.corfudb.infrastructure.redundancy;

import com.google.common.collect.ImmutableList;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegment;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.TransferSegmentStatus.SegmentState.RESTORED;
import static org.corfudb.runtime.view.Address.NON_ADDRESS;

/**
 * A redundancy calculator that reads the global trim mark before creating a list of
 * segments to transfer.
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
        runtime.getLayoutView().getRuntimeLayout(layout)
                .getLogUnitClient(endpoint)
                .prefixTrim(prefixToken)
                .join();
        return trimMark;
    }

    /**
     * Given a layout, creates an initial list of non-empty and bounded transfer segments.
     *
     * @param layout A current layout.
     * @return A list of transfer segments.
     */
    public ImmutableList<TransferSegment> createStateList(Layout layout) {
        long trimMark = setTrimOnNewLogUnit(layout, runtime, getServer());
        return layout.getSegments()
                .stream()
                // Keep all the segments after the trim mark, except the open one.
                .filter(segment -> segment.getEnd() != NON_ADDRESS && segment.getEnd() >= trimMark)
                .map(segment -> {
                    // The transfer segment's start is the layout segment's start or a trim mark,
                    // whichever is greater.
                    long segmentStart = Math.max(segment.getStart(), trimMark);
                    // The transfer segment's end should be inclusive.
                    // It is the last address to transfer.
                    long segmentEnd = segment.getEnd() - 1L;

                    if (segmentContainsServer(segment, getServer())) {
                        TransferSegmentStatus restored = TransferSegmentStatus
                                .builder()
                                .segmentState(RESTORED)
                                .totalTransferred(segmentEnd - segmentStart + 1L)
                                .build();

                        return TransferSegment
                                .builder()
                                .startAddress(segmentStart)
                                .endAddress(segmentEnd)
                                .status(restored)
                                .build();
                    } else {
                        TransferSegmentStatus notTransferred = TransferSegmentStatus
                                .builder()
                                .segmentState(NOT_TRANSFERRED)
                                .totalTransferred(0L)
                                .build();

                        return TransferSegment
                                .builder()
                                .startAddress(segmentStart)
                                .endAddress(segmentEnd)
                                .status(notTransferred)
                                .build();
                    }

                })
                .collect(ImmutableList.toImmutableList());
    }

}
