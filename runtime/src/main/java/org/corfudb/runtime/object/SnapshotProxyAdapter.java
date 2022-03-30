package org.corfudb.runtime.object;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;

import javax.annotation.concurrent.NotThreadSafe;
import java.lang.ref.Reference;
import java.util.Map;
import java.util.function.LongConsumer;

@NotThreadSafe
@Slf4j
public class SnapshotProxyAdapter<T extends ICorfuSMR<T>> {

    private final Reference<T> snapshotReference;

    private final long baseSnapshotVersion;

    private final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;

    public SnapshotProxyAdapter(@NonNull final Reference<T> snapshotReference, final long snapshotVersion,
                                @NonNull final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap) {
        this.snapshotReference = snapshotReference;
        this.baseSnapshotVersion = snapshotVersion;
        this.upcallTargetMap = upcallTargetMap;
    }

    public <R> R access(@NonNull ICorfuSMRAccess<R, T> accessFunction, @NonNull LongConsumer versionAccessed) {
        final T snapshot = snapshotReference.get();

        if (snapshotReference == null) {
            // TODO: Throw custom exception.
            throw new RuntimeException("Snapshot reference not available");
        }

        final R ret = accessFunction.access(snapshot);
        versionAccessed.accept(baseSnapshotVersion);
        return ret;
    }

    public void logUpdate(@NonNull SMREntry updateEntry) {
        final ICorfuSMRUpcallTarget<T> target = upcallTargetMap.get(updateEntry.getSMRMethod());

        if (target == null) {
            throw new RuntimeException("Unknown upcall " + updateEntry.getSMRMethod());
        }

        final T snapshot = snapshotReference.get();

        if (snapshot == null) {
            // TODO: Throw custom exception.
            throw new RuntimeException("Snapshot reference not available");
        }

        target.upcall(snapshot, updateEntry.getSMRArguments());
    }

    // TODO: getUpcall().
}
