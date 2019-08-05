package org.corfudb.infrastructure.orchestrator.actions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.ReadOptions;
import org.corfudb.util.CFUtils;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.RetryNeededException;

/**
 * State transfer utility.
 * Created by zlokhandwala on 2019-02-06.
 */
@Slf4j
public class StateTransfer {

    private StateTransfer() {
        // Hide implicit public constructor.
    }

    // The exponential backoff will not exceed this max retry timeout.
    private static final Duration MAX_RETRY_TIMEOUT = Duration.ofSeconds(10);

    // Random factor introduced into the exponential backoff.
    private static final float RANDOM_FACTOR_BACKOFF = 0.5f;

    // Maximum number of retries after which the Overwrite Exception is rethrown.
    private static final int OVERWRITE_RETRIES = 3;

    // Default read options for the state read calls
    private static ReadOptions readOptions = ReadOptions.builder()
            .waitForHole(true)
            .clientCacheable(false)
            .serverCacheable(false)
            .build();

    /**
     * Fetch and propagate the trimMark to the new/healing nodes. Else, a FastLoader reading from
     * them will have to mark all the already trimmed entries as holes.
     * Transfer an address segment from a cluster to a set of specified nodes.
     * There are no cluster reconfigurations, hence no epoch change side effects.
     *
     * @param layout   layout
     * @param endpoint destination node
     * @param runtime  The runtime to read the segment from
     * @param segment  segment to transfer
     */
    public static void transfer(Layout layout,
                                @NonNull String endpoint,
                                CorfuRuntime runtime,
                                Layout.LayoutSegment segment) throws InterruptedException {

        int chunkSize = runtime.getParameters().getBulkReadSize();
        final AtomicInteger overwriteRetries = new AtomicInteger();

        IRetry.build(ExponentialBackoffRetry.class, RetryExhaustedException.class, () -> {

            try {

                long trimMark = setTrimOnNewLogUnit(layout, runtime, endpoint);

                if (trimMark > segment.getEnd()) {
                    log.info("stateTransfer: Nothing to transfer, trimMark {}"
                                    + "greater than end of segment {}",
                            trimMark, segment.getEnd());
                    return true;
                }

                // State transfer should start from segment start address or trim mark
                // whichever is higher.
                final long segmentStart = Math.max(trimMark, segment.getStart());
                final long segmentEnd = segment.getEnd() - 1;
                log.info("stateTransfer: Total address range to transfer: [{}-{}] to node {}",
                        segmentStart, segmentEnd, endpoint);

                // Transfer non-written chunks of addresses.
                for (long chunkStart = segmentStart; chunkStart <= segmentEnd
                        ; chunkStart += chunkSize) {

                    long chunkEnd = Math.min(segmentEnd, chunkStart + chunkSize - 1);

                    // Fetch all missing entries in this range [chunkStart - chunkEnd].
                    List<Long> chunk = getMissingEntriesChunk(layout, runtime, endpoint,
                            chunkStart, chunkEnd);

                    // Read and write in chunks of chunkSize.
                    transferChunk(layout, runtime, endpoint, chunk);
                }
            } catch (OverwriteException oe) {

                log.error("stateTransfer: Overwrite Exception: retried: {} times",
                        overwriteRetries.get());

                if (overwriteRetries.getAndIncrement() >= OVERWRITE_RETRIES) {
                    throw new RetryExhaustedException("StateTransfer: Retries exhausted.");
                }
                throw new RetryNeededException();
            }

            return true;
        }).setOptions(retry -> {
            retry.setMaxRetryThreshold(MAX_RETRY_TIMEOUT);
            retry.setRandomPortion(RANDOM_FACTOR_BACKOFF);
        }).run();
    }

    /**
     * Send the trimMark to the new/healing nodes.
     * If this times out or fails, the Action performing the stateTransfer
     * fails and retries.
     * TrimMark is the first address present on the log unit server.
     * Perform the prefix trim on the preceding address = (trimMark - 1).
     * Since the LU will reject trim decisions made from older epochs, we
     * need to adjust the new trim mark to have the new layout's epoch.
     *
     * @param layout   Current layout.
     * @param runtime  Corfu runtime instance.
     * @param endpoint Endpoint ot transfer data to.
     * @return Trim Address.
     */
    private static long setTrimOnNewLogUnit(Layout layout, CorfuRuntime runtime,
                                            String endpoint) {

        long trimMark = runtime.getAddressSpaceView().getTrimMark().getSequence();

        Token prefixToken = new Token(layout.getEpoch(), trimMark - 1);
        CFUtils.getUninterruptibly(runtime.getLayoutView().getRuntimeLayout(layout)
                .getLogUnitClient(endpoint)
                .prefixTrim(prefixToken));
        return trimMark;
    }

    /**
     * Get the missing entries from the destination log unit in a specific range.
     * Partition the missing entries into chunks of chunkSize so that they can be
     * read from the cluster and written to the destination log unit server within
     * the RPC timeout.
     *
     * @param layout     Current layout.
     * @param runtime    Corfu runtime instance.
     * @param endpoint   Endpoint ot transfer data to.
     * @param chunkStart Start address of batch of missing entries.
     * @param chunkEnd   End address of batch of missing entries.
     * @return Iterable list of partitioned entries.
     */
    private static List<Long> getMissingEntriesChunk(Layout layout,
                                                     CorfuRuntime runtime,
                                                     String endpoint,
                                                     long chunkStart,
                                                     long chunkEnd) {

        // For each batch request known addresses in this batch.
        // This is an optimization in case the state transfer is repeated to
        // prevent redundant transfer.
        Set<Long> knownAddresses = CFUtils.getUninterruptibly(runtime.getLayoutView()
                .getRuntimeLayout(layout)
                .getLogUnitClient(endpoint)
                .requestKnownAddresses(chunkStart, chunkEnd))
                .getKnownAddresses();
        List<Long> missingEntries = new ArrayList<>();
        for (long address = chunkStart; address <= chunkEnd; address++) {
            if (!knownAddresses.contains(address)) {
                missingEntries.add(address);
            }
        }

        log.info("Addresses to be transferred in range [{}-{}] = {}",
                chunkStart, chunkEnd, missingEntries.size());

        return missingEntries;
    }

    /**
     * Read the chunk of data from the cluster and write to the destination log unit.
     *
     * @param layout   Current layout.
     * @param runtime  Corfu runtime instance.
     * @param endpoint Endpoint ot transfer data to.
     * @param chunk    List of addresses to be read and transferred.
     */
    private static void transferChunk(Layout layout,
                                      CorfuRuntime runtime,
                                      String endpoint,
                                      List<Long> chunk) {

        if (chunk.isEmpty()) {
            return;
        }

        long ts1 = System.currentTimeMillis();

        // Don't cache the read results on server for state transfer.
        Map<Long, ILogData> dataMap = runtime.getAddressSpaceView().read(chunk, readOptions);

        long ts2 = System.currentTimeMillis();

        log.info("stateTransfer: read [{}-{}] in {} ms",
                chunk.get(0), chunk.get(chunk.size() - 1), (ts2 - ts1));

        List<LogData> entries = new ArrayList<>();
        for (long address : chunk) {
            if (!dataMap.containsKey(address)) {
                log.error("Missing address {} in batch {}", address, chunk);
                throw new IllegalStateException("Missing address");
            }
            entries.add((LogData) dataMap.get(address));
        }

        try {
            // Write segment chunk to the new log unit
            ts1 = System.currentTimeMillis();
            boolean transferSuccess = CFUtils.getUninterruptibly(runtime.getLayoutView()
                    .getRuntimeLayout(layout)
                    .getLogUnitClient(endpoint)
                    .writeRange(entries), OverwriteException.class);

            ts2 = System.currentTimeMillis();

            if (!transferSuccess) {
                log.error("stateTransfer: Failed to transfer {} to {}",
                        chunk, endpoint);
                throw new IllegalStateException("Failed to transfer!");
            }

        } catch (OverwriteException oe) {
            log.error("stateTransfer: Overwrite Exception on transfer of chunk: {}", chunk);
            throw oe;
        }

        log.info("stateTransfer: Transferred address chunk [{}-{}] to {} in {} ms",
                chunk.get(0), chunk.get(chunk.size() - 1), endpoint, (ts2 - ts1));
    }
}
