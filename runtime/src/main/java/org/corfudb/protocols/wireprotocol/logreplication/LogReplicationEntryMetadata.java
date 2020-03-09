package org.corfudb.protocols.wireprotocol.logreplication;

import lombok.Data;
import org.corfudb.runtime.view.Address;

import java.util.UUID;

@Data
public class LogReplicationEntryMetadata {
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


    public LogReplicationEntryMetadata(MessageType type, UUID syncRequestId, long entryTimeStamp, long previousEntryTimestamp, long snapshotTimestamp, long sequence) {
        this(type, syncRequestId, entryTimeStamp, snapshotTimestamp);
        this.previousTimestamp = previousEntryTimestamp;
        this.snapshotSyncSeqNum = sequence;
    }

    public LogReplicationEntryMetadata() { }

    // Constructor for log entry ACK
    public LogReplicationEntryMetadata(MessageType type, UUID syncRequestId, long entryTimeStamp, long snapshotTimestamp) {
        this.messageMetadataType = type;
        this.syncRequestId = syncRequestId;
        this.timestamp = entryTimeStamp;
        this.snapshotTimestamp = snapshotTimestamp;
    }

    // Constructor used for snapshot sync
    public LogReplicationEntryMetadata(MessageType type, UUID syncRequestId, long entryTimeStamp, long snapshotTimestamp, UUID snapshotRequestId) {
        this(type, syncRequestId,  entryTimeStamp, Address.NON_EXIST, snapshotTimestamp, Address.NON_EXIST);
        this.syncRequestId = snapshotRequestId;
    }
}


