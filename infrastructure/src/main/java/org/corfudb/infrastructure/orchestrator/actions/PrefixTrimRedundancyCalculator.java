package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ImmutableMap;
import org.corfudb.infrastructure.log.statetransfer.StateTransferManager;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.*;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.NOT_TRANSFERRED;
import static org.corfudb.infrastructure.log.statetransfer.StateTransferManager.SegmentState.RESTORED;

public class PrefixTrimRedundancyCalculator extends RedundancyCalculator {

    private final CorfuRuntime runtime;

    public PrefixTrimRedundancyCalculator(String node, CorfuRuntime runtime){
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

    @Override
    public ImmutableMap<CurrentTransferSegment,
            CompletableFuture<CurrentTransferSegmentStatus>>
    createStateMap(Layout layout){
        long trimMark = setTrimOnNewLogUnit(layout, runtime, getServer());
        Map<CurrentTransferSegment, CompletableFuture<CurrentTransferSegmentStatus>> map =
                layout.getSegments()
                        .stream()
                        .filter(segment -> trimMark <= segment.getEnd())
                        .map(segment -> {
                            long segmentStart = Math.max(segment.getStart(), trimMark);
                            long segmentEnd = segment.getEnd() - 1;
                            CurrentTransferSegment transferSegment =
                                    new CurrentTransferSegment(segmentStart, segmentEnd);
                            if (segmentContainsServer(segment)) {
                                return new SimpleEntry<>(transferSegment,
                                        CompletableFuture
                                                .completedFuture(new
                                                        CurrentTransferSegmentStatus(RESTORED,
                                                        segmentEnd)));
                            } else {
                                return new SimpleEntry<>(transferSegment,
                                        CompletableFuture
                                                .completedFuture(new
                                                        CurrentTransferSegmentStatus(NOT_TRANSFERRED,
                                                        Address.NON_ADDRESS)));
                            }
                        }).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
        return ImmutableMap.copyOf(map);
    }

}
