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

    private final SMRSnapshot<T> snapshot;
    private T snapshotView;
    private final long baseSnapshotVersion;

    private final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;

    public SnapshotProxy(@NonNull final SMRSnapshot<T> snapshot, final long baseSnapshotVersion,
                         @NonNull final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap) {
        this.snapshot = snapshot;
        this.snapshotView = snapshot.consume();
        this.baseSnapshotVersion = baseSnapshotVersion;
        this.upcallTargetMap = upcallTargetMap;
    }

    public <R> R access(@NonNull ICorfuSMRAccess<R, T> accessFunction,
                        @NonNull LongConsumer versionAccessed) {
        final R ret = accessFunction.access(snapshotView);
        versionAccessed.accept(baseSnapshotVersion);
        return ret;
    }

    public void logUpdate(@NonNull SMREntry updateEntry) {
        final ICorfuSMRUpcallTarget<T> target = upcallTargetMap.get(updateEntry.getSMRMethod());

        if (target == null) {
            throw new RuntimeException("Unknown upcall " + updateEntry.getSMRMethod());
        }

        snapshotView = (T) target.upcall(snapshotView, updateEntry.getSMRArguments());
    }

    public void release() {
        snapshot.release();
    }
}
