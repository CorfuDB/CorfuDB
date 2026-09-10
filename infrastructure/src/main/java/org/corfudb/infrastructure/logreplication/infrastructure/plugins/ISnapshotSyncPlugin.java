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

    /** Opt in only if acquire/release are idempotent and fence delayed generations at the effect target. */
    default boolean supportsOwnedSnapshotLifecycle() {
        return false;
    }

    default void acquireSnapshot(CorfuRuntime runtime,
                                 org.corfudb.runtime.LogReplication.SnapshotSyncLeaseRecord lease) {
        throw new UnsupportedOperationException("Plugin does not implement owned snapshot protection");
    }

    /** Releasing an older protectionId must never release or resurrect a newer generation. */
    default void releaseSnapshot(CorfuRuntime runtime,
                                 org.corfudb.runtime.LogReplication.SnapshotSyncLeaseRecord lease) {
        throw new UnsupportedOperationException("Plugin does not implement owned snapshot protection");
    }
}
