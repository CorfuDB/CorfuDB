package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.RESTORED;
import static org.corfudb.runtime.view.Address.*;

public class PrefixTrimRedundancyCalculator extends RedundancyCalculator {

    private final CorfuRuntime runtime;

    public PrefixTrimRedundancyCalculator(String node, CorfuRuntime runtime) {
        super(node);
        this.runtime = runtime;
    }

    long setTrimOnNewLogUnit(Layout layout, CorfuRuntime runtime,
                             String endpoint) {

        long trimMark = runtime.getAddressSpaceView().getTrimMark().getSequence();

        Token prefixToken = new Token(layout.getEpoch(), trimMark - 1);
        CFUtils.getUninterruptibly(runtime.getLayoutView().getRuntimeLayout(layout)
                .getLogUnitClient(endpoint)
                .prefixTrim(prefixToken));
        return trimMark;
    }

    public ImmutableList<CurrentTransferSegment> createStateList(Layout layout) {
        long trimMark = setTrimOnNewLogUnit(layout, runtime, getServer());
        long globalCommittedOffset = retrieveGlobalCommittedOffset(layout, runtime);
        Optional<CommittedTransferData> committedTransferData;
        if (globalCommittedOffset == NON_ADDRESS) {
            committedTransferData = Optional.empty();
        } else {
            committedTransferData = Optional.of(new CommittedTransferData(globalCommittedOffset,
                    ImmutableList.copyOf(layout.getSegment(globalCommittedOffset).getAllLogServers())));
        }
        return ImmutableList.copyOf(layout.getSegments()
                .stream()
                // filter the segments that end before the trim mark and are not open.
                .filter(segment -> segment.getEnd() >= trimMark && segment.getEnd() != NON_ADDRESS)
                .map(segment -> {
                    long segmentStart = Math.max(segment.getStart(), trimMark);
                    long segmentEnd = segment.getEnd() - 1;

                    if (segmentContainsServer(segment, getServer())) {

                        return new CurrentTransferSegment(segmentStart, segmentEnd,
                                CompletableFuture.completedFuture(
                                        new CurrentTransferSegmentStatus(RESTORED,
                                                segmentEnd)), committedTransferData);
                    } else {
                        return new CurrentTransferSegment(segmentStart, segmentEnd,
                                CompletableFuture.completedFuture(
                                        new CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                                NON_ADDRESS)), committedTransferData);
                    }

                })
                .collect(Collectors.toList()));
    }

}
