package org.corfudb;

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

}


