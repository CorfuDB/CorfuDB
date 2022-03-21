package org.corfudb.runtime.object;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collection;
import java.util.Map;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

@NotThreadSafe
@Slf4j
public class SnapshotProxy<T> implements ICorfuSMRSnapshotProxy<T> {

    private T snapshot;

    private long baseSnapshotVersion;

    private final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;

    public SnapshotProxy(@NonNull final T snapshot, final long baseSnapshotVersion,
                         @NonNull final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap) {
        this.snapshot = snapshot;
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

        // TODO: update ICorfuSMRUpcallTarget interface to avoid cast
        snapshot = (T) target.upcall(snapshot, updateEntry.getSMRArguments());
    }

    public void logUpdate(@NonNull Collection<SMREntry> updateEntries, @NonNull LongSupplier updateVersion) {
        updateEntries.forEach(this::logUpdate);
        baseSnapshotVersion = updateVersion.getAsLong();
    }

    public long getVersion() {
        return baseSnapshotVersion;
    }

    public T get() {
        return snapshot;
    }
}
