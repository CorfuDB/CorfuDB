package org.corfudb.infrastructure.logreplication;

/**
 * This Interface represents the application callback for handling control messages.
 *
 */
public interface DataControl {

    /**
     * Due to internal errors during Snapshot or Log Entry Sync, request application
     * to start a Snapshot Sync to recover.
     */
    void requestSnapshotSync();

}
