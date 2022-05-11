package org.corfudb.runtime.object;

import lombok.NonNull;

@FunctionalInterface
public interface ISnapshotProxyGenerator<T> {

    ICorfuSMRSnapshotProxy<T> generate(@NonNull VersionedObjectIdentifier voId, @NonNull T object);

}
