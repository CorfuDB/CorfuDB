package org.corfudb.infrastructure.logreplication.replication.receive;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;

import static org.corfudb.protocols.wireprotocol.logreplication.MessageType.SNAPSHOT_END;
import static org.corfudb.protocols.wireprotocol.logreplication.MessageType.SNAPSHOT_MESSAGE;

@Slf4j
public class SnapshotSinkBufferManager extends SinkBufferManager {

     // It is used to remember the SNAPSHOT_END message sequence number.
    long snapshotEndSeq = Long.MAX_VALUE;

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

        super(SNAPSHOT_MESSAGE, ackCycleTime, ackCycleCnt, size, lastProcessedSeq, sinkManager);
    }

    /**
     *
     * @param entry
     * @return Previous inorder message's snapshotSeqNumber.
     */
    @Override
    long getPreSeq(LogReplicationEntry entry) {
        return entry.getMetadata().getSnapshotSyncSeqNum() - 1;
    }

    /**
     * If it is a SNAPSHOT_END message, it will record snapshotEndSeqNum
     * @param entry
     * @return entry's snapshotSeqNum
     */
    @Override
    long getCurrentSeq(LogReplicationEntry entry) {
        if (entry.getMetadata().getMessageMetadataType() == SNAPSHOT_END) {
            snapshotEndSeq = entry.getMetadata().getSnapshotSyncSeqNum();
        }
        return entry.getMetadata().getSnapshotSyncSeqNum();
    }

    /**
     * Make an Ack message with Snapshot type and lastProcesseSeq.
     * @param entry
     * @return
     */
    @Override
    public LogReplicationEntryMetadata makeAckMessage(LogReplicationEntry entry) {
        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(entry.getMetadata());

        /*
         * If SNAPSHOT_END message has been processed, send back SNAPSHOT_END to notify
         * sender the completion of the snapshot replication.
         */
        if (lastProcessedSeq == snapshotEndSeq) {
            metadata.setMessageMetadataType(MessageType.SNAPSHOT_END);
        } else {
            metadata.setMessageMetadataType(MessageType.SNAPSHOT_REPLICATED);
        }

        metadata.setSnapshotSyncSeqNum(lastProcessedSeq);
        log.debug("SnapshotSinkBufferManager send ACK {} for {}", lastProcessedSeq, metadata);
        return metadata;
    }

    /**
     * Verify if the message is the SNAPSHOT replication message.
     * SNAPSHOT_START will not processed by the buffer.
     * @param entry
     * @return
     */
    @Override
    public boolean verifyMessageType(LogReplicationEntry entry) {
        switch (entry.getMetadata().getMessageMetadataType()) {
            case SNAPSHOT_MESSAGE:
            case SNAPSHOT_END:
                return true;
            default:
                log.error("wrong message type ", entry.getMetadata());
                return false;
        }
    }

    boolean shouldAck() {
        if (lastProcessedSeq == snapshotEndSeq) {
            return true;
        }

        return super.shouldAck();
    }
}
