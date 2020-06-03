package org.corfudb.infrastructure.logreplication.receive;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.view.Address;

import static org.corfudb.protocols.wireprotocol.logreplication.MessageType.SNAPSHOT_END;
import static org.corfudb.protocols.wireprotocol.logreplication.MessageType.SNAPSHOT_MESSAGE;

@Slf4j
public class SnapshotSinkBufferManager extends SinkBufferManager {
    public SnapshotSinkBufferManager(int ackCycleTime, int ackCycleCnt, int size,
                                     long lastProcessedTs, LogReplicationSinkManager sinkManager) {
        super(SNAPSHOT_MESSAGE, ackCycleTime, ackCycleCnt, size, lastProcessedTs, sinkManager);
    }

    @Override
    long getPreTs(LogReplicationEntry entry) {
        return entry.getMetadata().getSnapshotSyncSeqNum() -1;
    }

    @Override
    long getCurrentTs(LogReplicationEntry entry) {
        return entry.getMetadata().getSnapshotSyncSeqNum();
    }

    @Override
    public LogReplicationEntryMetadata makeAckMessage(LogReplicationEntry entry) {
        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(entry.getMetadata());
        System.out.print("\nsink buffer size " + buffer.size());
        if (entry.getMetadata().getMessageMetadataType() == SNAPSHOT_END && buffer.isEmpty()) {
            metadata.setMessageMetadataType(MessageType.SNAPSHOT_END);
        } else {
            metadata.setMessageMetadataType(MessageType.SNAPSHOT_REPLICATED);
        }

        metadata.setSnapshotSyncSeqNum(lastProcessedTs);
        return metadata;
    }

    @Override
    public boolean verifyMessageType(LogReplicationEntry entry) {
        switch (entry.getMetadata().getMessageMetadataType()) {
            case SNAPSHOT_MESSAGE:
            case SNAPSHOT_START:
            case SNAPSHOT_END:
                return true;
            default:
                log.error("wrong message type ", entry.getMetadata());
                return false;
        }
    }
}
