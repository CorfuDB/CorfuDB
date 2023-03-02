package org.corfudb.runtime.object;

import lombok.NonNull;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteOptions;

import java.util.concurrent.atomic.AtomicInteger;

public class DiskBackedSMRSnapshot<T extends ICorfuSMR<T>> implements ISMRSnapshot<T>{

    private final OptimisticTransactionDB rocksDb;
    private final WriteOptions writeOptions;
    private final ConsistencyOptions consistencyOptions;
    private final ViewGenerator<T> viewGenerator;
    private final Snapshot snapshot;
    private final AtomicInteger refCnt;

    public DiskBackedSMRSnapshot(@NonNull OptimisticTransactionDB rocksDb,
                                 @NonNull WriteOptions writeOptions,
                                 @NonNull ConsistencyOptions consistencyOptions,
                                 @NonNull ViewGenerator<T> viewGenerator) {
        this.rocksDb = rocksDb;
        this.writeOptions = writeOptions;
        this.viewGenerator = viewGenerator;
        this.consistencyOptions = consistencyOptions;
        this.snapshot = rocksDb.getSnapshot();

        this.refCnt = new AtomicInteger(1); // TODO(Zach):
    }

    public T consume() {
        RocksDbApi<T> rocksTx;

        if (consistencyOptions.isReadYourWrites()) {
            rocksTx = new RocksDbTx<>(rocksDb, writeOptions, snapshot);
        } else {
            rocksTx = new RocksDbStubTx<>(rocksDb, snapshot);
        }

        final T view = viewGenerator.newView(rocksTx);
        refCnt.incrementAndGet();
        return view;
    }

    public void release() {
        // TODO(Zach): release when 0?
        rocksDb.releaseSnapshot(snapshot);
    }
}
