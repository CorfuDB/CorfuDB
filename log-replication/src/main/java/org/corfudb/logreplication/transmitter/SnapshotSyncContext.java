package org.corfudb.logreplication.transmitter;

/**
 * A class that holds context information for a snapshot sync from the log replication consumer (application)
 */
public class SnapshotSyncContext {

    // Event Identifier
    private String identifier;


    public SnapshotSyncContext(String identifier) {
        this.identifier = identifier;
    }
}
