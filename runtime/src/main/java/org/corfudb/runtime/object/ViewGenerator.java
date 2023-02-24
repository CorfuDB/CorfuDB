package org.corfudb.runtime.object;

import org.rocksdb.Snapshot;

public interface ViewGenerator<T extends ICorfuSMR<T>> {
    T generateView(RocksTableApi<T> view);

    T generateTx(Snapshot snapshot);
}
