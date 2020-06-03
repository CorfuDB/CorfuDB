package org.corfudb.infrastructure.logreplication.receive;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;

import static org.corfudb.protocols.wireprotocol.logreplication.MessageType.LOG_ENTRY_MESSAGE;
import static org.corfudb.protocols.wireprotocol.logreplication.MessageType.LOG_ENTRY_REPLICATED;

/**
 * For log entry sink buffer, tit uses the log entry message's previous timestamp
 */
@Slf4j
public class LogEntrySinkBufferManager extends SinkBufferManager {
    public LogEntrySinkBufferManager(int ackCycleTime, int ackCycleCnt, int size, long nextKey, LogReplicationSinkManager sinkManager) {
        super(LOG_ENTRY_MESSAGE, ackCycleTime, ackCycleCnt, size, nextKey, sinkManager);
    }

    @Override
    long getPreTs(LogReplicationEntry entry) {
        return entry.getMetadata().getPreviousTimestamp();
    }

    @Override
    long getCurrentTs(LogReplicationEntry entry) {
        return entry.getMetadata().getTimestamp();
    }

    @Override
    public LogReplicationEntryMetadata makeAckMessage(LogReplicationEntry entry) {
        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(entry.getMetadata());
        metadata.setMessageMetadataType(LOG_ENTRY_REPLICATED);
        metadata.setTimestamp(lastProcessedTs);
        return metadata;
    }

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
