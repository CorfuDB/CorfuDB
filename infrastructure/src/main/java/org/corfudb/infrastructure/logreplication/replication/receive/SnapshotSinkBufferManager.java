package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.protobuf.TextFormat;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;

@Slf4j
public class SnapshotSinkBufferManager extends SinkBufferManager {

     // It is used to remember the SNAPSHOT_END message sequence number.
    private long snapshotEndSeq = Long.MAX_VALUE;

    /**
     *
     * @param ackCycleTime
     * @param ackCycleCnt
     * @param size
     * @param lastProcessedSeq for a fresh snapshot transfer, the input should be Address.NO_ADDRESS.
     *                         If it restart the snapshot, it should be the value written in the metadata store.
     * @param sinkManager
     */
    public SnapshotSinkBufferManager(int ackCycleTime, int ackCycleCnt, int size,
                                     long lastProcessedSeq, LogReplicationSinkManager sinkManager) {
        super(LogReplicationEntryType.SNAPSHOT_MESSAGE, ackCycleTime, ackCycleCnt, size, lastProcessedSeq, sinkManager);
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
        log.debug("[{}]:: SnapshotSinkBufferManager send ACK {} for {}", sinkManager.getSessionName(),
                lastProcessedSeq, TextFormat.shortDebugString(metadata));
        return metadata.build();
    }

    /**
     * Verify if the message is the SNAPSHOT replication message.
     * SNAPSHOT_START will not processed by the buffer.
     * @param entry
     * @return
     */
    @Override
    public boolean verifyMessageType(LogReplicationEntryMsg entry) {
        return entry.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_MESSAGE ||
                entry.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_END;
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
