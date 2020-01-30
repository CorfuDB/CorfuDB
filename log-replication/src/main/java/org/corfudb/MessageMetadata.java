package org.corfudb;

public class MessageMetadata {

    private long entryTimeStamp;

    private long previousEntryTimestamp;

    // Used for snapshot Sync
    private long snapshotTimestamp;

}


