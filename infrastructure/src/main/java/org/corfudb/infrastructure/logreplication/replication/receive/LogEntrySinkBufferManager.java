package org.corfudb.infrastructure.logreplication.replication.receive;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;

import static org.corfudb.protocols.wireprotocol.logreplication.MessageType.LOG_ENTRY_MESSAGE;
import static org.corfudb.protocols.wireprotocol.logreplication.MessageType.LOG_ENTRY_REPLICATED;

/**
 * It manages the log entry sink buffer.
 */
@Slf4j
public class LogEntrySinkBufferManager extends SinkBufferManager {

    /**
     *
     * @param ackCycleTime
     * @param ackCycleCnt
     * @param size
     * @param sinkManager
     */
    public LogEntrySinkBufferManager(int ackCycleTime, int ackCycleCnt, int size, LogReplicationSinkManager sinkManager) {
        super(LOG_ENTRY_MESSAGE, ackCycleTime, ackCycleCnt, size, sinkManager);
    }

    /**
     * Get the pre inorder log entry's timestamp.
     * @param entry
     * @return log entry message's previousTimestamp.
     */
    @Override
    long getPreSeq(LogReplicationEntry entry) {
        return entry.getMetadata().getPreviousTimestamp();
    }

    /**
     * Get the log entry's timestamp that guarantees the ordering.
     * @param entry
     * @return log entry message's timestamp.
     */
    @Override
    public long getCurrentSeq(LogReplicationEntry entry) {
        return entry.getMetadata().getTimestamp();
    }

    @Override
    public long getLastProcessed() {
        return logReplicationMetadataManager.getLastProcessedLogTimestamp();
    }

    /**
     * Make a ACK message according to the last processed message's timestamp.
     * @param entry
     * @return ackMessage's metadata.
     */
    @Override
    public LogReplicationEntryMetadata getAckMetadata(LogReplicationEntry entry) {
        long lastProcessedSeq = getLastProcessed();

        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(entry.getMetadata());
        metadata.setMessageMetadataType(LOG_ENTRY_REPLICATED);
        metadata.setSnapshotTimestamp(logReplicationMetadataManager.getLastSrcBaseSnapshotTimestamp());
        metadata.setTimestamp(lastProcessedSeq);

        log.debug("Sink Buffer lastProcessedSeq {}", lastProcessedSeq);
        return metadata;
    }

    /**
     * Verify if it is the correct message type for log entry replication
     * @param entry
     * @return
     */
    @Override
    public boolean verifyMessageType(LogReplicationEntry entry) {
        if (entry.getMetadata().getMessageMetadataType() != type) {
            log.warn("Got msg type {} but expecting type {}",
                    entry.getMetadata().getMessageMetadataType(), type);
            return false;
        }

        return true;
    }
}
