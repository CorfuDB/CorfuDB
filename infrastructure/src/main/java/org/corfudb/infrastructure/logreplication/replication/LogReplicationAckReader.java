package org.corfudb.infrastructure.logreplication.replication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
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

    Lock lock = new ReentrantLock();

     public LogReplicationAckReader(LogReplicationMetadataManager metadataManager, LogReplicationConfig config,
                                    CorfuRuntime runtime, String remoteClusterId) {
        this.metadataManager = metadataManager;
        this.config = config;
        this.runtime = runtime;
        this.remoteClusterId = remoteClusterId;
        lastAckedTsPoller = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("ack-timestamp-reader").build());
        lastAckedTsPoller.scheduleWithFixedDelay(new TsPollingTask(), 0,
                ACKED_TS_READ_INTERVAL_SECONDS, TimeUnit.SECONDS);
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

    /**
     * For the given replication runtime, query max stream tail for all streams to be replicated.
     *
     * @return max tail of all streams to be replicated for the given runtime
     */
    private long getMaxReplicatedStreamsTail() {
        Map<UUID, Long> tailMap = runtime.getAddressSpaceView().getAllTails().getStreamTails();
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
     * Given a timestamp acked by the receiver, calculate how many entries remain to be sent for all replicated streams.
     *
     * @param ackedTimestamp Timestamp ack'd by the receiver
     *
     * For Log Entry Sync, this function returns the total number of entries remaining to be sent across all replicated
     * streams.
     *
     * For Snapshot Sync, a snapshot of each stream at a given point in time is sent.  The stream could have been
     * checkpointed and trimmed so we cannot find the remaining number of entries accurately.  In this case, we simply
     * subtract the acked timestamp from the global log tail when the snapshot sync started.
     * Note that this method is not accurate because the global tail can reflect the interleaving of replicated and
     * non-replicated streams, and hence, does not accurately represent the remaining entries to send for replicated streams.
     *
     * If there is no data on the active, it returns 0, which means no replication remaining.
     * If the ack'd timestamp is uninitialized(no ack received), it returns the log tail, which means no replication has
     * been done.
     */
    private long calculateRemainingEntriesToSend(long ackedTimestamp) {
        long maxReplicatedStreamTail = getMaxReplicatedStreamsTail();

        // No data to send on the Active, so no replication remaining
        if (maxReplicatedStreamTail == Address.NON_ADDRESS) {
            return NO_REPLICATION_REMAINING_ENTRIES;
        }

        // If doing a snapshot sync and nothing has been acked, all replication is remaining.  So set ack=0
        if (ackedTimestamp == Address.NON_ADDRESS &&
                lastSyncType == LogReplicationMetadata.ReplicationStatusVal.SyncType.SNAPSHOT) {
            ackedTimestamp = 0;
        }

        // When in LogEntry Sync, no CP and trim has taken place so the remaining entries can be queried using the
        // global tail and address maps of the replicated streams
        if (lastSyncType == LogReplicationMetadata.ReplicationStatusVal.SyncType.LOG_ENTRY) {
            return calculateRemainingEntriesForLogEntrySync(maxReplicatedStreamTail, ackedTimestamp);
        }

        // In Snapshot Sync
        // Simply subtract the ackedTimestamp from the global log tail from the time the snapshot sync started.
        // Note that this is not accurate because the global log tail does not accurately represent the remaining entries
        // for replicated streams.
        // When snapshot sync is ongoing, there may be delta updates also.  Add those new entries by querying the address maps
        return ((baseSnapshotTimestamp - ackedTimestamp) +
            calculateRemainingEntriesForLogEntrySync(maxReplicatedStreamTail, baseSnapshotTimestamp));
    }

    private long calculateRemainingEntriesForLogEntrySync(long start, long end) {
        long remainingEntriesToSend = 0;
        for (String stream : config.getStreamsToReplicate()) {
            UUID streamId = CorfuRuntime.getStreamID(stream);
            StreamAddressRange range = new StreamAddressRange(streamId, start, end);
            StreamAddressSpace addressSpace = runtime.getSequencerView().getStreamAddressSpace(range);
            remainingEntriesToSend += addressSpace.getAddressMap().getLongCardinality();
        }
        return remainingEntriesToSend;
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
