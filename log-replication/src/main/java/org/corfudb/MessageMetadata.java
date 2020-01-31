package org.corfudb;

import lombok.Getter;
import lombok.Setter;

public class MessageMetadata {

    /*
     * Used to determine the type of the metadata:
     * - snapshot type for reading/writing the snapshot timestamp,
     * - log entry type for reading/writing the entry and previous entry timestamps,
     */
    @Getter
    @Setter
    public MessageType type;

    /*
     * From Tx -> Rx: timestamp of the entry enqueued for shipping
     * From Rx -> Tx: timestamp of the entry applied on the receiving side
     */
    private long entryTimeStamp;

    /*
     * Used to chain sparse entries for ordering
     */
    private long previousEntryTimestamp;

    /*
     * Used to keep track of the time used for snapshots.
     * Read by the log entry shipper to determine which point to read the log from.
     */
    private long snapshotTimestamp;

    void init(MessageType type, long entryTimeStamp, long previousEntryTimestamp, long snapshotTimestamp) {
        this.type = type;
        this.entryTimeStamp = entryTimeStamp;
        this.previousEntryTimestamp = previousEntryTimestamp;
        this.snapshotTimestamp = snapshotTimestamp;
    }
}


