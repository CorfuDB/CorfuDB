package org.corfudb.infrastructure.logreplication.replication.send;

import com.google.common.collect.ImmutableList;
import io.micrometer.core.instrument.Tag;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.proto.LogReplicationMetadata.ReplicationStatusVal;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationAckReader;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.view.Address;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Manage the SnapshotSender's message buffer.
 */
@Slf4j
public class SnapshotSenderBufferManager extends SenderBufferManager {
    private LogReplicationAckReader ackReader;

    // The expectedSeqNum most recently reported by the sink, or Address.NON_ADDRESS if none has
    // been reported yet. expectedSeqNum is always lastProcessedSeq + 1 (see
    // SnapshotSinkBufferManager.generateAckMetadata()), so by itself it carries no more
    // information than the ack it rides on -- it only reveals a genuine gap when it fails to
    // advance across two acks despite the source continuing to send, which is what this field is
    // used to detect (see expediteResendFrom() callers).
    private long lastReportedExpectedSeqNum = Address.NON_ADDRESS;

    public SnapshotSenderBufferManager(DataSender dataSender, LogReplicationAckReader ackReader) {
        super(dataSender, configureAcksCounter());
        this.ackReader = ackReader;
    }

    /**
     * While receiving an ACK, will update the maxAck and also remove messages and CompletableFutures whose
     * corresponding snapshotSeqNum is not larger than the ACK.
     * @param newAck
     */
    @Override
    public void updateAck(Long newAck) {
        if (maxAckTimestamp < newAck) {
            log.debug("Ack Received for Snapshot Sync {}", newAck);
            markAckAdvanced();
            maxAckTimestamp = newAck;
            pendingMessages.evictAccordingToSeqNum(maxAckTimestamp);
            pendingCompletableFutureForAcks = pendingCompletableFutureForAcks.entrySet().stream()
                    .filter(entry -> entry.getKey() > maxAckTimestamp)
                    .collect(Collectors.toMap(x -> x.getKey(), x -> x.getValue()));
        }
    }

    /**
     * Update the maxAck with snapShotSyncSeqNum.
     * @param entry
     */
    @Override
    public void updateAck(LogReplicationEntryMsg entry) {
        updateAck(entry.getMetadata().getSnapshotSyncSeqNum());

        // If only a given stream has been replicated, update with the sequence number
        if (entry.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_REPLICATED) {
            ackReader.setAckedTsAndSyncType(entry.getMetadata().getSnapshotSyncSeqNum(),
                    ReplicationStatusVal.SyncType.SNAPSHOT);
        } else {
            // If all streams have been replicated, ack with the base snapshot so that the remaining entries(0) get
            // calculated correctly
            ackReader.setAckedTsAndSyncType(entry.getMetadata().getSnapshotTimestamp(),
                    ReplicationStatusVal.SyncType.SNAPSHOT);
        }

        // A sink new enough to report this has explicitly confirmed what it's still waiting for.
        // expectedSeqNum is always lastProcessedSeq + 1, so on its own it's just a restatement of
        // the ack -- it only becomes a genuine "I'm stuck" signal if it fails to advance across two
        // consecutive acks despite the source continuing to send in between. Only expedite in that
        // case; otherwise this would fire on every single ack of a perfectly healthy transfer,
        // resending the entire in-flight window each time. An old sink's acks simply never set this
        // field (hasExpectedSeqNum() == false), so this is a no-op against a peer that doesn't
        // support it -- safe during a rolling upgrade in either direction.
        if (entry.getMetadata().hasExpectedSeqNum()) {
            long reportedExpectedSeqNum = entry.getMetadata().getExpectedSeqNum();
            if (reportedExpectedSeqNum == lastReportedExpectedSeqNum) {
                expediteResendFrom(reportedExpectedSeqNum);
            }
            lastReportedExpectedSeqNum = reportedExpectedSeqNum;
        }
    }

    /**
     * Mark every pending entry at or after expectedSeqNum as due for resend on the very next
     * resend() call, bypassing its individual per-entry cadence timer. Uses an explicit flag
     * (LogReplicationPendingEntry.expedited) rather than manipulating the entry's own timer state:
     * that class's internal clock advances a fixed amount per call rather than per real elapsed
     * time, so there is no timer value that reliably forces an "immediate" timeout on the very next
     * check.
     */
    private void expediteResendFrom(long expectedSeqNum) {
        for (LogReplicationPendingEntry entry : pendingMessages.getPendingEntries()) {
            if (entry.getData().getMetadata().getSnapshotSyncSeqNum() >= expectedSeqNum) {
                entry.setExpedited(true);
            }
        }
    }

    /**
     * Use the message's snapshotSeqNum as the key to add its messages's corresponding ACK's
     * CompletableFuture to the hash table.
     * @param message
     * @param cf
     */
    @Override
    public void addCFToAcked(LogReplicationEntryMsg message, CompletableFuture<LogReplicationEntryMsg> cf) {
        pendingCompletableFutureForAcks.put(message.getMetadata().getSnapshotSyncSeqNum(), cf);
    }

    /**
     * In addition to the base reset, clears the previously-reported expectedSeqNum: it's scoped to
     * a single attempt, and each attempt's sequence numbers restart from Address.NON_ADDRESS (see
     * SenderBufferManager.reset()), so a stale value from a prior, now-abandoned attempt must not
     * be compared against this attempt's acks.
     */
    @Override
    public void reset(long lastAckedTimestamp) {
        super.reset(lastAckedTimestamp);
        lastReportedExpectedSeqNum = Address.NON_ADDRESS;
    }

    private static Optional<AtomicLong> configureAcksCounter() {
        return MeterRegistryProvider.getInstance()
                .map(registry -> registry.gauge("logreplication.acks",
                        ImmutableList.of(Tag.of("replication.type", "snapshot")),
                        new AtomicLong(0)));
    }
}
