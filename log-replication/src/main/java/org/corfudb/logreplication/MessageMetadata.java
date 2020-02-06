package org.corfudb.logreplication;

import lombok.Data;
import lombok.Getter;
@Data
public class MessageMetadata {
    /*
     * Used to determine the type of the metadata:
     * - snapshot type for reading/writing the snapshot timestamp,
     * - log entry type for reading/writing the entry and previous entry timestamps,
     */
    private MessageType messageMetadataType;

    /*
     * From Tx -> Rx: timestamp of the entry enqueued for shipping
     * From Rx -> Tx: timestamp of the entry applied on the receiving side
     */
    public long entryTimeStamp;
    //for full sync when the entryTimeStamp == snapshotTimestamp, it means the end of the stream.

    /*
     * Used to chain sparse entries for ordering
     */
    private long previousEntryTimestamp;

    /*
     * Used to keep track of the time used for snapshots.
     * Read by the log entry shipper to determine which point to read the log from.
     */
    private long snapshotTimestamp;

    private long fullSyncSeqNum; //used by fullsync only, zero means the start of the stream.

    public MessageMetadata(MessageType type, long entryTimeStamp, long previousEntryTimestamp, long snapshotTimestamp, long sequence) {
        this.messageMetadataType = type;
        this.entryTimeStamp = entryTimeStamp;
        this.previousEntryTimestamp = previousEntryTimestamp;
        this.snapshotTimestamp = snapshotTimestamp;
        this.fullSyncSeqNum = sequence;
    }
}


