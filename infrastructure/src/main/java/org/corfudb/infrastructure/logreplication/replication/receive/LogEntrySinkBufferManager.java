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
     * @param lastProcessedSeq last processed log entry's timestamp
     * @param sinkManager
     */
    public LogEntrySinkBufferManager(int ackCycleTime, int ackCycleCnt, int size, long lastProcessedSeq, LogReplicationSinkManager sinkManager) {
        super(LOG_ENTRY_MESSAGE, ackCycleTime, ackCycleCnt, size, lastProcessedSeq, sinkManager);
    }

    /**
     * Get the pre inorder log entry's timestamp.
     * @param entry
     * @return log entry message's previousTimestamp.
     */
    @Override
    public long getPreSeq(LogReplicationEntry entry) {
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

    /**
     * Make a ACK message according to the last processed message's timestamp.
     * @param entry
     * @return ackMessage's metadata.
     */
    @Override
    public LogReplicationEntryMetadata generateAckMetadata(LogReplicationEntry entry) {
        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(entry.getMetadata());
        metadata.setMessageMetadataType(LOG_ENTRY_REPLICATED);
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

    public void processBuffer() {
        /**
         *  For each message in the buffer, if its timestamp is smaller than last processed log entry's timestamp,
         *  skip processing and remove it from buffer.
         *  If its preTs and currentTs is overlapping with the last processed log entry's timestamp, process it.
         */
        for (LogReplicationEntry entry : buffer.values()) {
            LogReplicationEntryMetadata metadata = entry.getMetadata();
            if (metadata.getTimestamp() <= lastProcessedSeq) {
                buffer.remove(metadata.getPreviousTimestamp());
            } else if (metadata.getPreviousTimestamp() <= lastProcessedSeq && metadata.getTimestamp() > lastProcessedSeq) {
                sinkManager.processMessage(entry);
                ackCnt++;
                buffer.remove(lastProcessedSeq);
                lastProcessedSeq = getCurrentSeq(entry);
            }
        }
    }
}
