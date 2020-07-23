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
     * @param sinkManager
     */
    public SnapshotSinkBufferManager(int ackCycleTime, int ackCycleCnt, int size,
                                     LogReplicationSinkManager sinkManager) {

        super(SNAPSHOT_MESSAGE, ackCycleTime, ackCycleCnt, size, sinkManager);
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
            log.info("Setup snapshotEndSeq {}", snapshotEndSeq);
        }
        return entry.getMetadata().getSnapshotSyncSeqNum();
    }

    @Override
    long getLastProcessed() {
        return logReplicationMetadataManager.getLastSnapSeqNum();
    }

    /**
     * Make an Ack message with Snapshot type and lastProcesseSeq.
     * @param entry
     * @return
     */
    @Override
    public LogReplicationEntryMetadata getAckMetadata(LogReplicationEntry entry) {
        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(entry.getMetadata());

        // Set Snapshot Timestamp.
        entry.getMetadata().setSnapshotTimestamp(logReplicationMetadataManager.getLastSnapStartTimestamp());

        // Set ackValue.
        long lastProcessedSeq = getLastProcessed();

        metadata.setSnapshotSyncSeqNum(lastProcessedSeq);

        /*
         * If SNAPSHOT_END message has been processed, send back SNAPSHOT_END to notify
         * sender the completion of the snapshot replication.
         */
        if (lastProcessedSeq == (snapshotEndSeq -1)) {
            metadata.setMessageMetadataType(MessageType.SNAPSHOT_END);
            metadata.setSnapshotSyncSeqNum(snapshotEndSeq);
        } else {
            metadata.setMessageMetadataType(MessageType.SNAPSHOT_REPLICATED);
        }

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

    public boolean shouldAck(LogReplicationEntry entry) {
        // If it has different baseSnapshot, ignore it.
        if (entry.getMetadata().getSnapshotTimestamp() != logReplicationMetadataManager.getLastSnapStartTimestamp()) {
            log.warn("Get a message {} that has different snapshotTime with expecting {}", entry.getMetadata(),
                    logReplicationMetadataManager);
            return false;
        }


        // Always send an ACK for snapshot tranfer end marker.
        long lastProcessedSeq = logReplicationMetadataManager.getLastSnapSeqNum();
        log.debug("lastProccessedSeq {}  snapshotEndSeq {}", lastProcessedSeq, snapshotEndSeq);
        if (lastProcessedSeq == (snapshotEndSeq - 1)) {
            log.info("Snapshot End has been processed lastProccessedSeq {}  snapshotEndSeq {}", lastProcessedSeq, snapshotEndSeq);
            return true;
        }

        return super.shouldAck(entry);
    }
}
