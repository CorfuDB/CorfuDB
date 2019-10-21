package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableList;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.CommittedTransferData;
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
        long globalCommittedOffset = retrieveGlobalCommittedOffset(layout, runtime);
        Optional<CommittedTransferData> committedTransferData;
        if (globalCommittedOffset == NON_ADDRESS) {
            committedTransferData = Optional.empty();
        } else {
            committedTransferData = Optional.of(new CommittedTransferData(globalCommittedOffset,
                    ImmutableList.copyOf(layout.getSegment(globalCommittedOffset)
                            .getAllLogServers())));
        }
        return ImmutableList.copyOf(layout.getSegments()
                .stream()
                // filter the segments that end before the trim mark and are not open.
                .filter(segment -> segment.getEnd() >= trimMark && segment.getEnd() != NON_ADDRESS)
                .map(segment -> {
                    long segmentStart = Math.max(segment.getStart(), trimMark);
                    long segmentEnd = segment.getEnd() - 1L;

                    Optional<CommittedTransferData> thisSegmentsCommittedData =
                            committedTransferData.flatMap(data -> {
                                long committedOffset = data.getCommittedOffset();
                                List<String> sourceServers = data.getSourceServers();
                                // If committed offset > than the end of the segment,
                                // this segments committed offset is it's last address.
                                if (committedOffset > segmentEnd) {
                                    return Optional.of(
                                            new CommittedTransferData(segmentEnd, sourceServers)
                                    );
                                }
                                // If committed offset < than the start of the segment,
                                // this segments committed data is empty.
                                else if(committedOffset < segmentStart){
                                    return Optional.empty();
                                }
                                // Pass on the data
                                else{
                                    return Optional.of(data);
                                }
                            });

                    if (segmentContainsServer(segment, getServer())) {

                        return new CurrentTransferSegment(segmentStart, segmentEnd,
                                CompletableFuture.completedFuture(
                                        new CurrentTransferSegmentStatus(RESTORED,
                                                segmentEnd - segmentStart + 1L)),
                                thisSegmentsCommittedData);
                    } else {
                        return new CurrentTransferSegment(segmentStart, segmentEnd,
                                CompletableFuture.completedFuture(
                                        new CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                                0L)), thisSegmentsCommittedData);
                    }

                })
                .collect(Collectors.toList()));
    }

}
