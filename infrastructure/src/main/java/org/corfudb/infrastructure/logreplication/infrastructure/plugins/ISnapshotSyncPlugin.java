package org.corfudb.infrastructure.logreplication.infrastructure.plugins;

import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.CorfuRuntime;

/**
 * This interface can be implemented to plug system-specific logic around a snapshot sync on the sink.
 *
 * <p>Corfu's own checkpoint and trim are no longer paused through this plugin. While a snapshot sync
 * holds the snapshot lease ({@link org.corfudb.runtime.SnapshotSyncLeaseStore}), the compactor and
 * every checkpointer yield to that record directly, and the record also bounds how long they yield.
 * A plugin must therefore NOT write the compactor's freeze token or touch the compaction controls
 * table: a token left behind would only delay the checkpoint that reopens snapshot admission.
 *
 * <p>Implement {@link #acquireSnapshot} and {@link #releaseSnapshot} only for effects outside Corfu
 * (for example pausing an external process that would discard replicated data). Both hooks are:
 * <ul>
 *     <li>idempotent: they run again on retry, after a restart and under a new sink leader;</li>
 *     <li>fenced: a release carrying an older {@code protectionId} must never undo a newer acquire;</li>
 *     <li>bounded: they return or throw, they never block forever. A throwing release is retried
 *     every second, a release that never returns keeps snapshot admission closed.</li>
 * </ul>
 *
 * @author annym
 */
public interface ISnapshotSyncPlugin {

    /**
     * No longer invoked. Kept so existing plugin implementations keep compiling; move any effect that
     * is still needed into {@link #acquireSnapshot}.
     *
     * @deprecated replaced by {@link #acquireSnapshot(CorfuRuntime, SnapshotSyncLeaseRecord)}
     */
    @Deprecated
    void onSnapshotSyncStart(CorfuRuntime runtime);

    /**
     * No longer invoked. Kept so existing plugin implementations keep compiling; move any effect that
     * is still needed into {@link #releaseSnapshot}.
     *
     * @deprecated replaced by {@link #releaseSnapshot(CorfuRuntime, SnapshotSyncLeaseRecord)}
     */
    @Deprecated
    void onSnapshotSyncEnd(CorfuRuntime runtime);

    /**
     * Invoked once an attempt has been admitted and before any snapshot data is accepted. The lease
     * identifies the attempt ({@code generation}, {@code attemptId}) and its {@code protectionId}.
     */
    default void acquireSnapshot(CorfuRuntime runtime, SnapshotSyncLeaseRecord lease) {
        // Corfu's checkpointer is paused by the lease itself; nothing else to do by default.
    }

    /**
     * Invoked after the attempt has completed or was abandoned and its writers have stopped, before
     * the lease stops protecting the log. Releasing an older {@code protectionId} must never release
     * or resurrect a newer generation.
     */
    default void releaseSnapshot(CorfuRuntime runtime, SnapshotSyncLeaseRecord lease) {
        // Corfu's checkpointer is resumed by the lease itself; nothing else to do by default.
    }
}
