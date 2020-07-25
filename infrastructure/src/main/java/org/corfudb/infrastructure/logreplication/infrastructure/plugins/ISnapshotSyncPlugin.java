package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.runtime.CorfuRuntime;

/**
 * This interface must be implemented to plug any system-specific logic upon start and end of a snapshot sync.
 *
 * The expectation is that any external checkpoint/trim process is stopped upon snapshot sync start
 * and resumed on snapshot sync end, aiming to prevent data loss by trimming non-checkpointed shadow streams.
 *
 * @author annym
 */
public interface ISnapshotSyncPlugin {

    /**
     * On Snapshot Sync Start, stop any checkpoint / trim processes and return once it has been stopped.
     *
     */
    void onSnapshotSyncStart(CorfuRuntime runtime);

    /**
     * On Snapshot Sync End, resume any checkpoint / trim processes and return once it has been restarted.
     *
     */
    void onSnapshotSyncEnd(CorfuRuntime runtime);
}
