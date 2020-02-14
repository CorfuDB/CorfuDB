package org.corfudb.logreplication.message;

import lombok.Data;
import org.corfudb.runtime.view.Address;

import java.util.UUID;

@Data
public class MessageMetadata {
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
     * Used to correlate completed snapshot sync to the actual request
     */
    private UUID snapshotRequestId;

    /*
     * Used to keep track of the time used for snapshots.
     * Read by the log entry shipper to determine which point to read the log from.
     */
    private long snapshotTimestamp;

    private long snapshotSyncSeqNum; //used by snapshot fullsync stream only, zero means the start of the stream.

    public MessageMetadata(MessageType type, long entryTimeStamp, long previousEntryTimestamp, long snapshotTimestamp, long sequence) {
        this(type, entryTimeStamp, snapshotTimestamp);
        this.previousTimestamp = previousEntryTimestamp;
        this.snapshotSyncSeqNum = sequence;
    }

    // Constructor for log entry ACK
    public MessageMetadata(MessageType type, long entryTimeStamp, long snapshotTimestamp) {
        this.messageMetadataType = type;
        this.timestamp = entryTimeStamp;
        this.snapshotTimestamp = snapshotTimestamp;
    }

    // Constructor used for snapshot sync
    public MessageMetadata(MessageType type, long entryTimeStamp, long snapshotTimestamp, UUID snapshotRequestId) {
        this(type, entryTimeStamp, Address.NON_EXIST, snapshotTimestamp, Address.NON_EXIST);
        this.snapshotRequestId = snapshotRequestId;
    }
}


