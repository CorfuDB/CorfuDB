package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.protobuf.TextFormat;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;

import java.util.Objects;
import java.util.UUID;

import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;

@Slf4j
public class SnapshotSinkBufferManager extends SinkBufferManager {

     // It is used to remember the SNAPSHOT_END message sequence number.
    private long snapshotEndSeq = Long.MAX_VALUE;

    // Identity of the snapshot-sync attempt this buffer manager instance belongs to. A fresh
    // instance is constructed for every new attempt (see LogReplicationSinkManager.
    // processSnapshotStart()), but SNAPSHOT_MESSAGE/SNAPSHOT_END are otherwise matched purely by
    // numeric snapshotSyncSeqNum below -- and since every attempt's sender-side sequence numbers
    // restart from Address.NON_ADDRESS (SenderBufferManager.reset()), two successive attempts are
    // numbered on the same scale. A straggler message from a prior, already-cancelled attempt
    // (still in flight when it was cancelled -- cancellation can't retract an in-flight RPC) could
    // otherwise coincidentally match this attempt's buffer/lastProcessedSeq state purely by seqNum
    // and get applied into this attempt's shadow streams as if it were current data. Rejecting
    // anything whose syncRequestId doesn't match this attempt's closes that gap, mirroring the
    // identity check LogReplicationSinkManager.isValidSnapshotStart() already does for SNAPSHOT_START.
    private final UUID activeSyncRequestId;

    /**
     *
     * @param ackCycleTime
     * @param ackCycleCnt
     * @param size
     * @param lastProcessedSeq for a fresh snapshot transfer, the input should be Address.NO_ADDRESS.
     *                         If it restart the snapshot, it should be the value written in the metadata store.
     * @param activeSyncRequestId the syncRequestId of the snapshot-sync attempt currently in progress;
     *                            messages tagged with any other syncRequestId are dropped as stale.
     * @param sinkManager
     */
    public SnapshotSinkBufferManager(int ackCycleTime, int ackCycleCnt, int size,
                                     long lastProcessedSeq, UUID activeSyncRequestId,
                                     LogReplicationSinkManager sinkManager) {
        super(LogReplicationEntryType.SNAPSHOT_MESSAGE, ackCycleTime, ackCycleCnt, size, lastProcessedSeq, sinkManager);
        this.activeSyncRequestId = activeSyncRequestId;
    }

    /**
     *
     * @param entry
     * @return Previous in order message's snapshotSeqNumber.
     */
    @Override
    public long getPreSeq(LogReplicationEntryMsg entry) {
        return entry.getMetadata().getSnapshotSyncSeqNum() - 1;
    }

    /**
     * If it is a SNAPSHOT_END message, it will record snapshotEndSeqNum
     * @param entry
     * @return entry's snapshotSeqNum
     */
    @Override
    public long getCurrentSeq(LogReplicationEntryMsg entry) {
        if (entry.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_END) {
            snapshotEndSeq = entry.getMetadata().getSnapshotSyncSeqNum();
        }
        return entry.getMetadata().getSnapshotSyncSeqNum();
    }

    /**
     * Generate log entry sync acknowledgement metadata
     *
     * @param entry log replication entry message
     * @return ack message metadata
     */
    @Override
    public LogReplicationEntryMetadataMsg generateAckMetadata(LogReplicationEntryMsg entry) {
        LogReplicationEntryMetadataMsg.Builder metadata = LogReplicationEntryMetadataMsg
                .newBuilder()
                .mergeFrom(entry.getMetadata());

        /*
         * If SNAPSHOT_END message has been processed, send back SNAPSHOT_TRANSFER_COMPLETE to notify
         * sender the completion of the snapshot replication transfer.
         */
        if (lastProcessedSeq == snapshotEndSeq) {
            metadata.setEntryType(LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE);
        } else {
            metadata.setEntryType(LogReplicationEntryType.SNAPSHOT_REPLICATED);
        }

        metadata.setSnapshotSyncSeqNum(lastProcessedSeq);
        // Explicitly state what's still needed, so the source can target retransmission precisely
        // instead of blindly resending on a fixed cadence -- see the field's Javadoc in the .proto
        // for why this needs to always be set (not just on a detected gap) and why it's a oneof.
        metadata.setExpectedSeqNum(lastProcessedSeq + 1);
        log.debug("SnapshotSinkBufferManager send ACK {} for {}",
                lastProcessedSeq, TextFormat.shortDebugString(metadata));
        return metadata.build();
    }

    /**
     * Verify if the message is a SNAPSHOT replication message belonging to the currently active
     * snapshot-sync attempt. SNAPSHOT_START is not processed by the buffer.
     *
     * The syncRequestId check rejects a straggler from a prior, already-cancelled attempt -- see
     * activeSyncRequestId's Javadoc for why matching by seqNum alone isn't sufficient.
     *
     * @param entry
     * @return
     */
    @Override
    public boolean verifyMessageType(LogReplicationEntryMsg entry) {
        if (entry.getMetadata().getEntryType() != LogReplicationEntryType.SNAPSHOT_MESSAGE &&
                entry.getMetadata().getEntryType() != LogReplicationEntryType.SNAPSHOT_END) {
            return false;
        }

        UUID messageSyncRequestId = getUUID(entry.getMetadata().getSyncRequestId());
        if (!Objects.equals(messageSyncRequestId, activeSyncRequestId)) {
            log.warn("Dropping stale snapshot sync message: msg syncRequestId={}, active attempt " +
                            "syncRequestId={}, seqNum={}", messageSyncRequestId, activeSyncRequestId,
                    entry.getMetadata().getSnapshotSyncSeqNum());
            return false;
        }

        return true;
    }

    /**
     * Go through the buffer to find messages that are in order with the last processed message.
     */
    public void processBuffer() {
        while (true) {
            LogReplicationEntryMsg dataMessage = buffer.get(lastProcessedSeq);
            if (dataMessage == null) {
                return;
            }
            sinkManager.processMessage(dataMessage);
            ackCnt++;
            buffer.remove(lastProcessedSeq);
            lastProcessedSeq = getCurrentSeq(dataMessage);
        }
    }

    public boolean shouldAck() {
        if (lastProcessedSeq == snapshotEndSeq) {
            return true;
        }
        return super.shouldAck();
    }
}
