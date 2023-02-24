package org.corfudb.runtime.object;

import lombok.NonNull;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteOptions;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class DiskBackedSMRSnapshot<T extends ICorfuSMR<T> & ViewGenerator<T>> implements ISMRSnapshot<T>{

    private final Snapshot snapshot;
    private final AtomicInteger refCnt;
    private final T instance;

    public DiskBackedSMRSnapshot(@NonNull OptimisticTransactionDB rocksDb,
                                 @NonNull T instance) {
        this.snapshot = rocksDb.getSnapshot();
        this.instance = instance;
        this.refCnt = new AtomicInteger(1); // TODO(Zach):
    }

    public T consume() {
        T newView = instance.generateTx(snapshot);
        refCnt.incrementAndGet();
        return newView;
    }

    public void release() {
        // decrement ref-count
        // if ref-count == 0
        // rocksDb.releaseSnapshot(snapshot);
    }
}
