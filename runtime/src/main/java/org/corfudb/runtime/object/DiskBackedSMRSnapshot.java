package org.corfudb.runtime.object;

import lombok.NonNull;
import org.corfudb.runtime.collections.RocksDbEntryIterator;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Function;

public class DiskBackedSMRSnapshot<S extends SnapshotGenerator<S>> implements ISMRSnapshot<S>{
    private final StampedLock lock = new StampedLock();
    private final OptimisticTransactionDB rocksDb;

    private final ReadOptions readOptions;
    private final WriteOptions writeOptions;
    private final Snapshot snapshot;

    private final ConsistencyOptions consistencyOptions;
    private final ViewGenerator<S> viewGenerator;
    private final AtomicInteger refCnt;
    public final VersionedObjectIdentifier version;

    private final Set<RocksDbEntryIterator> set;

    public DiskBackedSMRSnapshot(@NonNull OptimisticTransactionDB rocksDb,
                                 @NonNull WriteOptions writeOptions,
                                 @NonNull ConsistencyOptions consistencyOptions,
                                 @NonNull VersionedObjectIdentifier version,
                                 @NonNull ViewGenerator<S> viewGenerator) {
        this.rocksDb = rocksDb;
        this.writeOptions = writeOptions;
        this.viewGenerator = viewGenerator;
        this.consistencyOptions = consistencyOptions;
        this.snapshot = rocksDb.getSnapshot();
        this.refCnt = new AtomicInteger(1); // TODO(Zach):
        this.readOptions = new ReadOptions().setSnapshot(this.snapshot);
        this.version = version;
        this.set = Collections.newSetFromMap(new WeakHashMap<>());
    }

    public <V> V executeInSnapshot(Function<ReadOptions, V> function) {
        long stamp = lock.readLock();
        try {
            if (!this.readOptions.isOwningHandle()) {
                throw new TrimmedException("Snapshot is not longer active " + version);
            }
            return function.apply(this.readOptions);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public void executeInSnapshot(Consumer<ReadOptions> function) {
        long stamp = lock.readLock();
        try {
            if (!this.readOptions.isOwningHandle()) {
                throw new TrimmedException("Snapshot is not longer active " + version);
            }
            function.accept(this.readOptions);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public S consume() {
        RocksDbApi<S> rocksTx;

        if (consistencyOptions.isReadYourWrites()) {
            rocksTx = new RocksDbTx<>(rocksDb, writeOptions, this);
        } else {
            rocksTx = new RocksDbStubTx<>(rocksDb, this);
        }

        final S view = viewGenerator.newView(rocksTx);
        refCnt.incrementAndGet();
        return view;
    }

    public void release() {
        long stamp = lock.writeLock();
        rocksDb.releaseSnapshot(snapshot);
        set.forEach(RocksDbEntryIterator::invalidateIterator);
        readOptions.close();
        lock.unlockWrite(stamp);
    }

    public <K, V> RocksDbEntryIterator<K, V> newIterator(ISerializer serializer, Transaction transaction) {
        RocksDbEntryIterator<K, V> iterator = new RocksDbEntryIterator<>(
                transaction.getIterator(readOptions),
                serializer, readOptions, lock);
        set.add(iterator);
        return iterator;
    }
}
