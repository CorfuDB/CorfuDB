package org.corfudb.infrastructure.orchestrator.actions;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;

/**
 * State transfer utility.
 * Created by zlokhandwala on 2019-02-06.
 */
@Slf4j
public class StateTransfer {

    private StateTransfer() {
        // Hide implicit public constructor.
    }

    /**
     * Fetch and propagate the trimMark to the new/healing nodes. Else, a FastLoader reading from
     * them will have to mark all the already trimmed entries as holes.
     * Transfer an address segment from a cluster to a set of specified nodes.
     * There are no cluster reconfigurations, hence no epoch change side effects.
     *
     * @param layout    layout
     * @param endpoints destination nodes
     * @param runtime   The runtime to read the segment from
     * @param segment   segment to transfer
     */
    public static void transfer(Layout layout,
                                Set<String> endpoints,
                                CorfuRuntime runtime,
                                Layout.LayoutSegment segment) throws ExecutionException, InterruptedException {

        if (endpoints.isEmpty()) {
            log.debug("stateTransfer: No server needs to transfer for segment [{} - {}], " +
                    "skipping state transfer for this segment.", segment.getStart(), segment.getEnd());
            return;
        }

        int batchSize = runtime.getParameters().getBulkReadSize();

        long trimMark = runtime.getAddressSpaceView().getTrimMark().getSequence();
        // Send the trimMark to the new/healing nodes.
        // If this times out or fails, the Action performing the stateTransfer fails and retries.

        endpoints.stream()
                .map(endpoint -> {
                    // TrimMark is the first address present on the log unit server.
                    // Perform the prefix trim on the preceding address = (trimMark - 1).
                    // Since the LU will reject trim decisions made from older epochs, we
                    // need to adjust the new trim mark to have the new layout's epoch.
                    Token prefixToken = new Token(layout.getEpoch(), trimMark - 1);
                    return runtime.getLayoutView().getRuntimeLayout(layout)
                            .getLogUnitClient(endpoint)
                            .prefixTrim(prefixToken);
                })
                .forEach(CFUtils::getUninterruptibly);

        if (trimMark > segment.getEnd()) {
            log.info("stateTransfer: Nothing to transfer, trimMark {} greater than end of segment {}",
                    trimMark, segment.getEnd());
            return;
        }

        // State transfer should start from segment start address or trim mark whichever is lower.
        long segmentStart = Math.max(trimMark, segment.getStart());

        for (long chunkStart = segmentStart; chunkStart < segment.getEnd()
                ; chunkStart = chunkStart + batchSize) {
            long chunkEnd = Math.min((chunkStart + batchSize - 1), segment.getEnd() - 1);

            long ts1 = System.currentTimeMillis();

            Map<Long, ILogData> dataMap = runtime.getAddressSpaceView()
                    .fetchAll(ContiguousSet.create(Range.closed(chunkStart, chunkEnd),
                            DiscreteDomain.longs()), true);

            long ts2 = System.currentTimeMillis();

            log.info("stateTransfer: read {}-{} in {} ms", chunkStart, chunkEnd, (ts2 - ts1));

            List<LogData> entries = new ArrayList<>();
            for (long x = chunkStart; x <= chunkEnd; x++) {
                if (!dataMap.containsKey(x)) {
                    log.error("Missing address {} in range {}-{}", x, chunkStart, chunkEnd);
                    throw new IllegalStateException("Missing address");
                }
                entries.add((LogData) dataMap.get(x));
            }

            for (String endpoint : endpoints) {
                // Write segment chunk to the new logunit
                ts1 = System.currentTimeMillis();
                boolean transferSuccess = runtime.getLayoutView().getRuntimeLayout(layout)
                        .getLogUnitClient(endpoint)
                        .writeRange(entries).get();
                ts2 = System.currentTimeMillis();

                if (!transferSuccess) {
                    log.error("stateTransfer: Failed to transfer {}-{} to {}", chunkStart,
                            chunkEnd, endpoint);
                    throw new IllegalStateException("Failed to transfer!");
                }

                log.info("stateTransfer: Transferred address chunk [{}, {}] to {} in {} ms",
                        chunkStart, chunkEnd, endpoint, (ts2 - ts1));
            }
        }
    }
}
