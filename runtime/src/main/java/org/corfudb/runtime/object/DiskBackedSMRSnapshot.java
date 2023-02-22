package org.corfudb.runtime.object;

import lombok.NonNull;
import org.corfudb.runtime.collections.DiskBackedCorfuTable;
import org.corfudb.runtime.collections.PersistedStreamingMap;
import org.corfudb.util.ReflectionUtils;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.Snapshot;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class DiskBackedSMRSnapshot<T extends ICorfuSMR<T>> implements ISMRSnapshot<T> {

    private final OptimisticTransactionDB rocksDb;
    private final WriteOptions writeOptions;
    private final Snapshot snapshot;
    private final Function<IRocksDBContext<T>, T> instanceProducer;

    // Book-keeping
    private final AtomicInteger refCnt;

    public DiskBackedSMRSnapshot(@NonNull OptimisticTransactionDB rocksDb, @NonNull WriteOptions writeOptions,
                                 @NonNull Function<IRocksDBContext<T>, T> instanceProducer) {
        this.rocksDb = rocksDb;
        this.writeOptions = writeOptions;
        this.snapshot = rocksDb.getSnapshot();


        this.instanceProducer = instanceProducer;

        // TODO(Zach):
        this.refCnt = new AtomicInteger(1);
    }

    public T consume() {
        // TODO(Zach): ReadOptions cleanup
        final RocksDBTxnContext<T> txnContext = new RocksDBTxnContext<>(rocksDb, writeOptions,
                snapshot, new ReadOptions().setSnapshot(snapshot));

        final T instance = instanceProducer.apply(txnContext);
        refCnt.incrementAndGet();
        return instance;
    }

    public void release() {
        // decrement ref-count
        // if ref-count == 0
        // rocksDb.releaseSnapshot(snapshot);
    }
}
