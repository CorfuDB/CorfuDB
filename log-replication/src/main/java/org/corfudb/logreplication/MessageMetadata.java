package org.corfudb.logreplication;

import lombok.Getter;

public class MessageMetadata {
    /*
     * Used to determine the type of the metadata:
     * - snapshot type for reading/writing the snapshot timestamp,
     * - log entry type for reading/writing the entry and previous entry timestamps,
     */
    @Getter
    private MessageType messageMetadataType;

    /*
     * From Tx -> Rx: timestamp of the entry enqueued for shipping
     * From Rx -> Tx: timestamp of the entry applied on the receiving side
     */
    @Getter
    public long entryTimeStamp;

    /*
     * Used to chain sparse entries for ordering
     */
    @Getter
    private long previousEntryTimestamp;

    /*
     * Used to keep track of the time used for snapshots.
     * Read by the log entry shipper to determine which point to read the log from.
     */
    @Getter
    private long snapshotTimestamp;

    public MessageMetadata(MessageType type, long entryTimeStamp, long previousEntryTimestamp, long snapshotTimestamp) {
        this.messageMetadataType = type;
        this.entryTimeStamp = entryTimeStamp;
        this.previousEntryTimestamp = previousEntryTimestamp;
        this.snapshotTimestamp = snapshotTimestamp;
    }
}


