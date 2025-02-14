package org.corfudb.infrastructure.logreplication.replication.receive;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;

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
        super(LogReplicationEntryType.LOG_ENTRY_MESSAGE, ackCycleTime, ackCycleCnt, size, lastProcessedSeq, sinkManager);
    }

    /**
     * Get the pre inorder log entry's timestamp.
     * @param entry
     * @return log entry message's previousTimestamp.
     */
    @Override
    public long getPreSeq(LogReplicationEntryMsg entry) {
        return entry.getMetadata().getPreviousTimestamp();
    }

    /**
     * Get the log entry's timestamp that guarantees the ordering.
     * @param entry
     * @return log entry message's timestamp.
     */
    @Override
    public long getCurrentSeq(LogReplicationEntryMsg entry) {
        return entry.getMetadata().getTimestamp();
    }

    /**
     * Make a ACK message according to the last processed message's timestamp.
     * @param entry
     * @return ackMessage's metadata
     */
    @Override
    public LogReplicationEntryMetadataMsg generateAckMetadata(LogReplicationEntryMsg entry) {
        LogReplicationEntryMetadataMsg metadata = LogReplicationEntryMetadataMsg.newBuilder()
                .mergeFrom(entry.getMetadata())
                .setEntryType(LogReplicationEntryType.LOG_ENTRY_REPLICATED)
                .setTimestamp(lastProcessedSeq).build();
        log.debug("Sink Buffer lastProcessedSeq {}", lastProcessedSeq);
        return metadata;
    }

    /**
     * Verify if it is the correct message type for log entry replication
     * @param entry
     * @return
     */
    @Override
    public boolean verifyMessageType(LogReplicationEntryMsg entry) {
        if (entry.getMetadata().getEntryType() != type) {
            log.warn("Got msg type {} but expecting type {}",
                    entry.getMetadata().getEntryType(), type);
            return false;
        }

        return true;
    }

    public void processBuffer() {
        /*
         *  For each message in the buffer, if its timestamp is smaller than last processed log entry's timestamp,
         *  skip processing and remove it from buffer.
         *  If its preTs and currentTs is overlapping with the last processed log entry's timestamp, process it.
         */
        for (LogReplicationEntryMsg entry : buffer.values()) {
            LogReplicationEntryMetadataMsg metadata = entry.getMetadata();
            if (metadata.getTimestamp() <= lastProcessedSeq) {
                buffer.remove(metadata.getPreviousTimestamp());
                log.warn("Remove entry without processing, ts={}, lastProcessed={}", metadata.getTimestamp(), lastProcessedSeq);
            } else if (metadata.getPreviousTimestamp() <= lastProcessedSeq && sinkManager.processMessage(entry)) {
                ackCnt++;
                buffer.remove(lastProcessedSeq);
                lastProcessedSeq = getCurrentSeq(entry);
            }
        }
    }
}
