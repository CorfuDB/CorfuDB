package org.corfudb.runtime.object;

import lombok.NonNull;

public interface ViewGenerator<S extends SnapshotGenerator<S>> {
    S newView(@NonNull RocksDbApi<S> rocksApi);
}
