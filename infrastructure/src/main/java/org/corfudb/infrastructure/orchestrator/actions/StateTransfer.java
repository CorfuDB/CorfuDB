package org.corfudb.infrastructure.orchestrator.actions;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.LogRecoveryStateResponse;
import org.corfudb.protocols.wireprotocol.LogRecoveryStateWriteMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.RetryExhaustedException;
import org.corfudb.runtime.view.Layout;
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
                // TODO(Xin): Will there be any change in the context of sparse-trim garbage collection.

                // State transfer should start from segment start address
                final long segmentStart = segment.getStart();
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

                // Send the segment end as the committed tail to the new log unit.
                // This is required to prevent loss of committed tail on new log unit
                // after state transfer finishes and then all other log units failed
                // and auto commit service is paused.
                CFUtils.getUninterruptibly(runtime.getLayoutView()
                        .getRuntimeLayout(layout)
                        .getLogUnitClient(endpoint)
                        .updateCommittedTail(segmentEnd));

                // Inform the log unit it has finished state transfer (so its committedTail becomes valid).
                CFUtils.getUninterruptibly(runtime.getLayoutView().getRuntimeLayout(layout)
                        .getLogUnitClient(endpoint).informStateTransferFinished());

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
        // The returned state already filtered out compacted log data.
        LogRecoveryStateResponse recoveryState = runtime.getAddressSpaceView().readRecoveryStates(chunk);

        long ts2 = System.currentTimeMillis();

        log.info("stateTransfer: read [{}-{}] in {} ms",
                chunk.get(0), chunk.get(chunk.size() - 1), (ts2 - ts1));

        List<LogData> logEntries = new ArrayList<>();
        for (long address : chunk) {
            LogData logEntry = recoveryState.getLogEntryMap().get(address);
            // The recoveryState read from AddressSpaceView does not
            // contain compacted entries, so null check is needed.
            if (logEntry != null) {
                logEntries.add(logEntry);
            }
        }

        List<LogData> garbageEntries = new ArrayList<>(recoveryState.getGarbageEntryMap().values());

        LogRecoveryStateWriteMsg stateWriteMsg = new LogRecoveryStateWriteMsg(
                logEntries, garbageEntries, recoveryState.getCompactionMark());

        try {
            // Write segment chunk to the new log unit.
            ts1 = System.currentTimeMillis();
            boolean transferSuccess = CFUtils.getUninterruptibly(runtime.getLayoutView()
                    .getRuntimeLayout(layout)
                    .getLogUnitClient(endpoint)
                    .writeRecoveryStates(stateWriteMsg), OverwriteException.class);

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
