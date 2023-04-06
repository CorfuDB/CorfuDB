package org.corfudb.runtime.object;

import lombok.NonNull;

public interface RocksDbSnapshotGenerator<S extends SnapshotGenerator<S>> {
    SMRSnapshot<S> getSnapshot(@NonNull ViewGenerator<S> viewGenerator,
                               @NonNull VersionedObjectIdentifier version);

    SMRSnapshot<S> getImplicitSnapshot(
            @NonNull ViewGenerator<S> viewGenerator,
            @NonNull VersionedObjectIdentifier version);
}
