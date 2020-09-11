package org.corfudb.infrastructure.logreplication.replication;

import static org.corfudb.runtime.view.ObjectsView.TRANSACTION_STREAM_ID;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.LogEntryReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.StreamsLogEntryReader.StreamIteratorMetadata;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.StreamAddressSpace;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class LogReplicationAckReader {
    private LogReplicationMetadataManager metadataManager;
    private LogReplicationConfig config;
    private CorfuRuntime runtime;
    private String remoteClusterId;
    // Log tail when the current snapshot sync started.  We do not need to synchronize access to it because it will not
    // be read(calculateRemainingEntriesToSend) and written(setBaseSnapshot) concurrently.
    private long baseSnapshotTimestamp;

    /*
     * Periodic Thread which reads the last Acked Timestamp and writes it to the metadata table
     */
    private ScheduledExecutorService lastAckedTsPoller;

    /*
     * Interval at which the thread reads the last Acked Timestamp
     */
    private static int ACKED_TS_READ_INTERVAL_SECONDS = 15;

    private static int NO_REPLICATION_REMAINING_ENTRIES = 0;

    /*
     * Last ack'd timestamp from Receiver
     */
    private long lastAckedTimestamp = Address.NON_ADDRESS;

    /*
     * Sync Type for which last Ack was Received.  Set to Log Entry Type as the initial FSM state is
     * Log Entry Sync
     */
    private LogReplicationMetadata.ReplicationStatusVal.SyncType lastSyncType =
            LogReplicationMetadata.ReplicationStatusVal.SyncType.LOG_ENTRY;

    private LogEntryReader logEntryReader;

    private Lock lock = new ReentrantLock();

    public LogReplicationAckReader(LogReplicationMetadataManager metadataManager, LogReplicationConfig config,
                                    CorfuRuntime runtime, String remoteClusterId) {
        this.metadataManager = metadataManager;
        this.config = config;
        this.runtime = runtime;
        this.remoteClusterId = remoteClusterId;
        lastAckedTsPoller = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("ack-timestamp-reader").build());
    }

    public void setAckedTsAndSyncType(long ackedTs, LogReplicationMetadata.ReplicationStatusVal.SyncType syncType) {
        lock.lock();
        try {
            lastAckedTimestamp = ackedTs;
            lastSyncType = syncType;
        } finally {
            lock.unlock();
        }
    }

    public void setSyncType(LogReplicationMetadata.ReplicationStatusVal.SyncType syncType) {
        lock.lock();
        try {
            lastSyncType = syncType;
        } finally {
            lock.unlock();
        }
    }

    public void startAckReader(LogEntryReader logEntryReader) {
        this.logEntryReader = logEntryReader;
        lastAckedTsPoller.scheduleWithFixedDelay(new TsPollingTask(), 0,
                ACKED_TS_READ_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Given a timestamp acked by the receiver, calculate how many entries remain to be sent for all replicated streams.
     *
     * @param ackedTimestamp Timestamp ack'd by the receiver
     *
     * For Log Entry Sync, this function returns the total number of entries remaining to be sent across all replicated
     * streams based on the transaction stream.
     *
     * For Snapshot Sync, a snapshot of each stream at a given point in time is sent. The stream could have been
     * checkpointed and trimmed so we cannot find the remaining number of entries accurately. In this case, we simply
     * subtract the acked timestamp from the global log tail when the snapshot sync started.
     * Note that this method is not accurate because the global tail can reflect the interleaving of replicated and
     * non-replicated streams, and hence, does not accurately represent the remaining entries to send for replicated streams.
     *
     * If there is no data on the active, it returns 0, which means no replication remaining.
     * If the ack'd timestamp is uninitialized(no ack received), it returns the log tail, which means no replication has
     * been done.
     */
    private long calculateRemainingEntriesToSend(long ackedTimestamp) {
        // Get all streams tails, which will be used in the computation of remaining entries
        Map<UUID, Long> tailMap = runtime.getAddressSpaceView().getAllTails().getStreamTails();

        long txStreamTail = getTxStreamTail(tailMap);
        long maxReplicatedStreamTail = getMaxReplicatedStreamsTail(tailMap);
        StreamIteratorMetadata currentTxStreamProcessedTs = logEntryReader.getCurrentProcessedEntryMetadata();

        log.trace("calculateRemainingEntriesToSend:: maxTailReplicateStreams={}, txStreamTail={}, lastTxStreamProcessedTs={}, " +
                        "lastTxStreamProcessedStreamsPresent={}, sync={}",
                maxReplicatedStreamTail, txStreamTail, currentTxStreamProcessedTs.getTimestamp(),
                currentTxStreamProcessedTs.isStreamsToReplicatePresent(), lastSyncType);

        // No data to send on the active, so no replication remaining
        if (maxReplicatedStreamTail == Address.NON_ADDRESS) {
            log.debug("No data to replicate, replication complete.");
            return NO_REPLICATION_REMAINING_ENTRIES;
        }

        if (lastSyncType.equals(LogReplicationMetadata.ReplicationStatusVal.SyncType.SNAPSHOT)) {

            // If during snapshot sync nothing has been ack'ed, all replication is remaining
            if (ackedTimestamp == Address.NON_ADDRESS) {
                ackedTimestamp = 0;
            }

            // In Snapshot Sync
            // Simply subtract the ack'ed timestamp from the global log tail from the time the snapshot sync started.
            // Note that this is not accurate because the global log tail does not accurately represent the remaining entries
            // for replicated streams.
            // When snapshot sync is ongoing, there may be delta updates also. Add those new entries by querying the address maps
            return ((baseSnapshotTimestamp - ackedTimestamp) +
                    getTxStreamTotalEntries(baseSnapshotTimestamp, txStreamTail));
        }

        // In Log Entry Sync
        return calculateRemainingEntriesIncrementalUpdates(ackedTimestamp, txStreamTail, currentTxStreamProcessedTs);
    }

    private long getTxStreamTail(Map<UUID, Long> tailMap) {
        if (tailMap.containsKey(TRANSACTION_STREAM_ID)) {
            return tailMap.get(TRANSACTION_STREAM_ID);
        }

        log.warn("Tx Stream tail not present in sequencer, id={}", TRANSACTION_STREAM_ID);
        return Address.NON_ADDRESS;
    }

    /**
     * For the given replication runtime, query max stream tail for all streams to be replicated.
     *
     * @return max tail of all streams to be replicated for the given runtime
     */
    private long getMaxReplicatedStreamsTail(Map<UUID, Long> tailMap) {
        long maxTail = Address.NON_ADDRESS;
        for (String streamName : config.getStreamsToReplicate()) {
            UUID streamUuid = CorfuRuntime.getStreamID(streamName);
            if (tailMap.containsKey(streamUuid)) {
                long streamTail = tailMap.get(streamUuid);
                maxTail = Math.max(maxTail, streamTail);
            }
        }
        return maxTail;
    }

    /**
     * The calculation of remaining entries during log entry sync takes into account:
     * - lastAckedTimestamp: last timestamp of entry for which we've received an ACK
     * - txStreamTail: transaction stream tail
     * - currentTxStreamProcessedTs: metadata of current entry being processed by the StreamsLogEntryReader,
     *           it contains the timestamp and a boolean indicating if that entry has data to replicate or not,
     *           this will give an indication whether if an ACK is expected or not.
     *
     * Consider the following representation of the data log, where entries 20, 50, 60, 70 belong to the
     * tx stream.
     *

     +---+---+---+---+---+---+---+---+---+---+---+---+
     |   |   |   |   |   |   |   |   |   |   |   |   |
     +---+---+---+---+---+---+---+---+---+---+---+---+
     20          50          60       70         100
     tx          tx          tx       tx        (log tail / not part of tx stream)


     * Case 1.0: Log Entry Sync lagging behind (in processing) current processing not acked with entries to replicate
     *          - lastAckedTimestamp = 50
     *          - txStreamTail = 70
     *          - currentTxStreamProcessedTs = 60, true (contains replicated streams)
     *
     *          remainingEntries = entriesBetween(50, 70] = 2

     * Case 1.1: Log Entry Sync lagging behind (in processing) current processing no entries to replicate
     *          - lastAckedTimestamp = 50
     *          - txStreamTail = 70
     *          - currentTxStreamProcessedTs = 60, false (does not contain streams to replicate)
     *
     * (despite the current processed not requiring ACk, there might be entries between lastAcked and currentProcessed
     * which still have not been acknowledged so it is better to overestimate as it will eventually converge to an accurate value)
     *          remainingEntries = entriesBetween(50, 70] = 1

     * Case 2.0: Log Entry Sync Up to Date
     *          - lastAckedTimestamp = 70
     *          - txStreamTail = 70
     *          - currentTxStreamProcessedTs = 70, true (contains replicated streams)
     *
     *          remainingEntries = 0

     * Case 2.1: Log Entry Sync Up to Date
     *          - lastAckedTimestamp = 60
     *          - txStreamTail = 70
     *          - currentTxStreamProcessedTs = 70, false (does not contain replicated streams,
     *          so there is no expectation that lastAckedTimestamp reaches 70)
     *
     *          remainingEntries = 0


     * Case 2.2: Log Entry Sync Lagging Behind (in ack)
     *          - lastAckedTimestamp = 60
     *          - txStreamTail = 70
     *          - currentTxStreamProcessedTs = 70, true (does contain replicated streams,
     *           wait until lastAckedTimestamp reflects this)
     *
     *          remainingEntries = entriesBetween(60, 70] = 1
     *
     */
    private long calculateRemainingEntriesIncrementalUpdates(long lastAckedTs, long txStreamTail,
                                                             StreamIteratorMetadata currentTxStreamProcessedTs) {
        long noRemainingEntriesToSend = 0;

        // If we have processed up to or beyond the latest known tx stream tail
        // we can assume we are up to date.
        // Note: in the case of a switchover the tail won't be moving so we can assume
        // the tx stream last processed timestamp will never be above the stream's tail,
        // but in the case of ongoing replication or holes, it might be above.
        // We can't do much other than report that we're up to date (as it might continue moving)
        if (txStreamTail <= currentTxStreamProcessedTs.getTimestamp()) {
            // (Case 2 from description)
            // If the current processed tx stream entry has data to replicate,
            // we need to ensure we have received an ACK it, otherwise,
            // we might be signalling completion when there is still an entry for which
            // we haven't received confirmation of the recipient.
            if (currentTxStreamProcessedTs.isStreamsToReplicatePresent()) {
                if (lastAckedTs == currentTxStreamProcessedTs.getTimestamp()) {
                    log.trace("Log Entry Sync up to date, lastAckedTs={}, txStreamTail={}, currentTxProcessedTs={}, containsEntries={}", lastAckedTs,
                            txStreamTail, currentTxStreamProcessedTs.getTimestamp(), currentTxStreamProcessedTs.isStreamsToReplicatePresent());
                    // (Case 2.0)
                    return noRemainingEntriesToSend;
                }

                // (Case 2.2)
                // Last ack'ed timestamp should match the last processed tx stream timestamp.
                // Calculate how many entries are missing, based on the tx stream's address map
                log.trace("Log Entry Sync pending ACKs, lastAckedTs={}, txStreamTail={}, currentTxProcessedTs={}, containsEntries={}", lastAckedTs,
                        txStreamTail, currentTxStreamProcessedTs.getTimestamp(), currentTxStreamProcessedTs.isStreamsToReplicatePresent());
                return getTxStreamTotalEntries(lastAckedTs, currentTxStreamProcessedTs.getTimestamp());
            }

            // (Case 2.1)
            // Since last tx stream processed timestamp is not intended to be replicated
            // and we're at or beyond the last known tail, no entries remaining to be sent
            // at this point.
            log.trace("Log Entry Sync up to date, no pending ACKs, lastAckedTs={}, txStreamTail={}, currentTxProcessedTs={}, containsEntries={}", lastAckedTs,
                    txStreamTail, currentTxStreamProcessedTs.getTimestamp(), currentTxStreamProcessedTs.isStreamsToReplicatePresent());
            return noRemainingEntriesToSend;
        }

        // (Cases 1.0 and 1.1)
        long remainingEntries = getTxStreamTotalEntries(lastAckedTs, txStreamTail);
        log.trace("Log Entry Sync pending entries for processing, lastAckedTs={}, txStreamTail={}, currentTxProcessedTs={}, containsEntries={}, remaining={}", lastAckedTs,
                txStreamTail, currentTxStreamProcessedTs.getTimestamp(), currentTxStreamProcessedTs.isStreamsToReplicatePresent(), remainingEntries);

        if (!currentTxStreamProcessedTs.isStreamsToReplicatePresent()) {
            // Case 1.1
            // Remove one entry, which accounts for the current processed which does not have streams to replicate
            return remainingEntries - 1;
        }

        return remainingEntries;
    }

    private long getTxStreamTotalEntries(long lowerBoundary, long upperBoundary) {
        long totalEntries = 0;

        if (upperBoundary > lowerBoundary) {
            StreamAddressRange range = new StreamAddressRange(TRANSACTION_STREAM_ID, upperBoundary, lowerBoundary);
            StreamAddressSpace txStreamAddressSpace = runtime.getSequencerView().getStreamAddressSpace(range);
            // Count how many entries are present in the Tx Stream (this can include holes,
            // valid entries and invalid entries), but we count them all (equal weight).
            // An invalid entry, is a transactional entry with no streams to replicate (which will be ignored)
            totalEntries = txStreamAddressSpace.getAddressMap().getLongCardinality();
        }

        log.trace("getTxStreamTotalEntries:: entries={} in range ({}, {}]", totalEntries, lowerBoundary, upperBoundary);
        return totalEntries;
    }

    public void shutdown() {
        // Stop accepting any new updates
        lastAckedTsPoller.shutdown();
        try {
            // Wait 100ms for currently running tasks to finish.  If they do not finish, shutdown immediately
            if (!lastAckedTsPoller.awaitTermination(100, TimeUnit.MILLISECONDS)) {
                lastAckedTsPoller.shutdownNow();
            }
        } catch (InterruptedException e) {
            // If any task is interrupted by shutdownNow, catch that exception and shutdown immediately
            lastAckedTsPoller.shutdownNow();
        }
    }

    public void setBaseSnapshot(long baseSnapshotTimestamp) {
        this.baseSnapshotTimestamp = baseSnapshotTimestamp;
    }

    /**
     * Task which periodically updates the metadata table with replication completion percentage
     */
    private class TsPollingTask implements Runnable {
        @Override
        public void run() {
            lock.lock();
            try {
                long remainingReplicationStatus = calculateRemainingEntriesToSend(lastAckedTimestamp);
                metadataManager.setReplicationRemainingEntries(remoteClusterId, remainingReplicationStatus,
                        lastSyncType);
            } finally {
                lock.unlock();
            }
        }
    }
}
