package org.corfudb.runtime.object;

import lombok.NonNull;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Snapshot;

import java.util.concurrent.atomic.AtomicInteger;

public class DiskBackedSMRSnapshot<T extends ICorfuSMR<T> & ViewGenerator<T>> implements ISMRSnapshot<T>{

    private final OptimisticTransactionDB rocksDb;
    private final Snapshot snapshot;
    private final AtomicInteger refCnt;
    private final T instance;

    public DiskBackedSMRSnapshot(@NonNull OptimisticTransactionDB rocksDb,
                                 @NonNull T instance) {
        this.rocksDb = rocksDb;
        this.snapshot = rocksDb.getSnapshot();
        this.instance = instance;
        this.refCnt = new AtomicInteger(1); // TODO(Zach):
    }

    public T consume() {
        T newView = instance.newView(snapshot);
        refCnt.incrementAndGet();
        return newView;
    }

    public void release() {
        rocksDb.releaseSnapshot(snapshot);
    }
}
