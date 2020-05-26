package org.corfudb.protocols.wireprotocol.logreplication;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.Messages;
import org.corfudb.runtime.view.Address;

import java.util.UUID;

@Data
@Slf4j
public class LogReplicationEntryMetadata {
    /*
     * Used to keep track of the site config history.
     */
    private long siteConfigID;

    /*
     * Used to determine the type of the metadata:
     * - snapshot type for reading/writing the snapshot timestamp,
     * - log entry type for reading/writing the entry and previous entry timestamps,
     */
    private MessageType messageMetadataType;

    /*
     * Max timestamp of all log entries in the current message
     * for full sync when the timestamp == snapshotTimestamp, it means the end of the stream.
     */
    public long timestamp;

    /*
     * Used to chain sparse sequence for ordering
     */
    private long previousTimestamp;

    /*
     * Used to correlate snapshot and log entry sync ACKs to the actual state
     */
    private UUID syncRequestId;

    /*
     * Used to keep track of the time used for snapshots.
     * Read by the log entry shipper to determine which point to read the log from.
     */
    private long snapshotTimestamp;

    private long snapshotSyncSeqNum; //used by snapshot full sync stream only, zero means the start of the stream.


    public LogReplicationEntryMetadata(MessageType type, long epoch, UUID syncRequestId, long entryTimeStamp, long previousEntryTimestamp, long snapshotTimestamp, long sequence) {
        this(type, epoch, syncRequestId, entryTimeStamp, snapshotTimestamp);
        this.previousTimestamp = previousEntryTimestamp;
        this.snapshotSyncSeqNum = sequence;
    }

    public LogReplicationEntryMetadata() { }

    // Constructor for log entry ACK
    public LogReplicationEntryMetadata(MessageType type, long siteConfigID, UUID syncRequestId, long entryTimeStamp, long snapshotTimestamp) {
        this.messageMetadataType = type;
        this.siteConfigID = siteConfigID;
        this.syncRequestId = syncRequestId;
        this.timestamp = entryTimeStamp;
        this.snapshotTimestamp = snapshotTimestamp;
    }

    // Constructor used for snapshot sync
    public LogReplicationEntryMetadata(MessageType type, long epoch,  UUID syncRequestId, long entryTimeStamp, long snapshotTimestamp, UUID snapshotRequestId) {
        this(type, epoch, syncRequestId,  entryTimeStamp, Address.NON_EXIST, snapshotTimestamp, Address.NON_EXIST);
        this.syncRequestId = snapshotRequestId;
    }

    public static LogReplicationEntryMetadata fromProto(Messages.LogReplicationEntryMetadata proto) {
        // Parse protoBuf Message
        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata();
        metadata.setMessageMetadataType(getType(proto.getType()));
        metadata.setSyncRequestId(new UUID(proto.getSyncRequestId().getMsb(), proto.getSyncRequestId().getLsb()));
        metadata.setTimestamp(proto.getTimestamp());
        metadata.setPreviousTimestamp(proto.getPreviousTimestamp());
        metadata.setSnapshotTimestamp(proto.getSnapshotTimestamp());
        metadata.setSnapshotSyncSeqNum(proto.getSnapshotSyncSeqNum());

        return metadata;
    }

    private static MessageType getType(Messages.LogReplicationEntryType type) {
        switch(type) {
            case LOG_ENTRY_MESSAGE:
                return MessageType.LOG_ENTRY_MESSAGE;
            case SNAPSHOT_MESSAGE:
                return MessageType.SNAPSHOT_MESSAGE;
            case SNAPSHOT_START:
                return MessageType.SNAPSHOT_START;
            case SNAPSHOT_END:
                return MessageType.SNAPSHOT_END;
            case SNAPSHOT_REPLICATED:
                return MessageType.SNAPSHOT_REPLICATED;
            case LOG_ENTRY_REPLICATED:
                return MessageType.LOG_ENTRY_REPLICATED;
            default:
                log.error("Found unknown log entry message type {}", type);
                return null;
        }
    }
}


