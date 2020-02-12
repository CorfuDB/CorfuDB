package org.corfudb.logreplication.message;

import lombok.Data;

@Data
public class MessageMetadata {
    /*
     * Used to determine the type of the metadata:
     * - snapshot type for reading/writing the snapshot timestamp,
     * - log entry type for reading/writing the entry and previous entry timestamps,
     */
    private MessageType messageMetadataType;

    /*
     * Max timestamp of all log entries in the current messge
     */
    public long timestamp;
    //for full sync when the timestamp == snapshotTimestamp, it means the end of the stream.

    /*
     * Used to chain sparse sequence for ordering
     */
    private long previousTimestamp;

    /*
     * Used to keep track of the time used for snapshots.
     * Read by the log entry shipper to determine which point to read the log from.
     */
    private long snapshotTimestamp;

    private long snapshotSyncSeqNum; //used by snapshot fullsync stream only, zero means the start of the stream.

    public MessageMetadata(MessageType type, long entryTimeStamp, long previousEntryTimestamp, long snapshotTimestamp, long sequence) {
        this.messageMetadataType = type;
        this.timestamp = entryTimeStamp;
        this.previousTimestamp = previousEntryTimestamp;
        this.snapshotTimestamp = snapshotTimestamp;
        this.snapshotSyncSeqNum = sequence;
    }
}


