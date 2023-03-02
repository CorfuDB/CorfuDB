package org.corfudb.runtime.object;

import lombok.NonNull;

public interface ViewGenerator<T extends ICorfuSMR<T>> {
    T newView(@NonNull RocksDbApi<T> rocksApi);
}
