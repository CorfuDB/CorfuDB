package org.corfudb.runtime.object;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Map;
import java.util.function.LongConsumer;

@NotThreadSafe
@Slf4j
public class SnapshotProxy<T> implements ICorfuSMRSnapshotProxy<T> {

    private final ISMRSnapshot<T> snapshotWrapper;

    private T snapshot;

    private final long baseSnapshotVersion;

    private final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;

    public SnapshotProxy(@NonNull final T snapshot, final long baseSnapshotVersion,
                         @NonNull final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap) {
        this.snapshotWrapper = null;
        this.snapshot = snapshot;
        this.baseSnapshotVersion = baseSnapshotVersion;
        this.upcallTargetMap = upcallTargetMap;
    }

    public SnapshotProxy(@NonNull final ISMRSnapshot<T> snapshot, final long baseSnapshotVersion,
                         @NonNull final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap) {
        // TODO(Zach): Where to call release()?
        this.snapshotWrapper = snapshot;
        this.snapshot = snapshot.consume();
        this.baseSnapshotVersion = baseSnapshotVersion;
        this.upcallTargetMap = upcallTargetMap;
    }

    public <R> R access(@NonNull ICorfuSMRAccess<R, T> accessFunction, @NonNull LongConsumer versionAccessed) {
        final R ret = accessFunction.access(snapshot);
        versionAccessed.accept(baseSnapshotVersion);
        return ret;
    }

    public void logUpdate(@NonNull SMREntry updateEntry) {
        final ICorfuSMRUpcallTarget<T> target = upcallTargetMap.get(updateEntry.getSMRMethod());

        if (target == null) {
            throw new RuntimeException("Unknown upcall " + updateEntry.getSMRMethod());
        }

        snapshot = (T) target.upcall(snapshot, updateEntry.getSMRArguments());
    }

    public void release() {
        // TODO(Zach): better alternative for this - How to ensure that resources are cleaned?
        // Implement closeable?
        snapshotWrapper.release();
    }
}
