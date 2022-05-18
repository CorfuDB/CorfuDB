package org.corfudb.runtime.object;

import lombok.NonNull;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.exceptions.StaleObjectVersionException;

import java.lang.ref.Reference;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

public class SnapshotReferenceProxy<T> implements ICorfuSMRSnapshotProxy<T> {

    private final Reference<ICorfuSMRSnapshotProxy<T>> snapshotProxyReference;

    private long baseSnapshotVersion;

    private final UUID id;

    SnapshotReferenceProxy(@NonNull final UUID id, final long baseSnapshotVersion, @NonNull final T snapshot,
                           @NonNull final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap,
                           @NonNull final Function<ICorfuSMRSnapshotProxy<T>, Reference<ICorfuSMRSnapshotProxy<T>>> refFn) {
        this.snapshotProxyReference = refFn.apply(new SnapshotProxy<>(snapshot, baseSnapshotVersion, upcallTargetMap));
        this.id = id;
        this.baseSnapshotVersion = baseSnapshotVersion;
    }

    public <R> R access(@NonNull ICorfuSMRAccess<R, T> accessFunction, @NonNull LongConsumer versionAccessed) {
        return getSnapshotProxyOrThrow().access(accessFunction, versionAccessed);
    }

    public void logUpdate(@NonNull SMREntry updateEntry) {
        getSnapshotProxyOrThrow().logUpdate(updateEntry);
    }

    public void logUpdate(@NonNull Collection<SMREntry> updateEntries, @NonNull LongSupplier updateVersion) {
        getSnapshotProxyOrThrow().logUpdate(updateEntries, updateVersion);
        baseSnapshotVersion = updateVersion.getAsLong();
    }

    public long getVersion() {
        return baseSnapshotVersion;
    }

    public T get() {
        return getSnapshotProxyOrThrow().get();
    }

    private ICorfuSMRSnapshotProxy<T> getSnapshotProxyOrThrow() {
        final ICorfuSMRSnapshotProxy<T> snapshotProxy = snapshotProxyReference.get();

        if (snapshotProxy == null) {
            throw new StaleObjectVersionException(id, baseSnapshotVersion);
        }

        return snapshotProxy;
    }
}
