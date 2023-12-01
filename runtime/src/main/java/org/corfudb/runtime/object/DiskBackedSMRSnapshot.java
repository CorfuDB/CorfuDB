package org.corfudb.runtime.object;

import lombok.Getter;
import lombok.NonNull;
import org.corfudb.runtime.collections.RocksDbEntryIterator;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.util.serializer.ISerializer;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.ReadOptions;
import org.rocksdb.Snapshot;
import org.rocksdb.Transaction;
import org.rocksdb.WriteOptions;

import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.corfudb.runtime.collections.RocksDbEntryIterator.LOAD_VALUES;

public class DiskBackedSMRSnapshot<S extends SnapshotGenerator<S>> implements SMRSnapshot<S> {

    private final VersionedObjectIdentifier version;
    private final StampedLock lock = new StampedLock();
    private final OptimisticTransactionDB rocksDb;
    private final Snapshot snapshot;
    private final ViewGenerator<S> viewGenerator;
    private final ReadOptions readOptions;
    private final WriteOptions writeOptions;
    private final ColumnFamilyRegistry columnFamilyRegistry;

    @Getter
    private final VersionedObjectStats metrics;

    // A set of iterators associated with this snapshot.
    private final Set<RocksDbEntryIterator<?, ?>> set;

    public DiskBackedSMRSnapshot(@NonNull OptimisticTransactionDB rocksDb,
                                 @NonNull WriteOptions writeOptions,
                                 @NonNull VersionedObjectIdentifier version,
                                 @NonNull ViewGenerator<S> viewGenerator,
                                 @NonNull ColumnFamilyRegistry columnFamilyRegistry) {
        this.rocksDb = rocksDb;
        this.writeOptions = writeOptions;
        this.viewGenerator = viewGenerator;
        this.snapshot = rocksDb.getSnapshot();
        this.readOptions = new ReadOptions().setSnapshot(this.snapshot);
        this.version = version;
        this.columnFamilyRegistry = columnFamilyRegistry;
        this.set = Collections.newSetFromMap(new WeakHashMap<>());
        this.metrics = new VersionedObjectStats();
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

    public void executeInSnapshot(Consumer<ReadOptions> consumer) {
        long stamp = lock.readLock();
        try {
            if (!this.readOptions.isOwningHandle()) {
                throw new TrimmedException("Snapshot is not longer active " + version);
            }
            consumer.accept(this.readOptions);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public S consume() {
        return viewGenerator.newView(new RocksDbTx<>(rocksDb, writeOptions, this, columnFamilyRegistry));
    }

    public void release() {
        long stamp = lock.writeLock();
        try {
            if (!this.readOptions.isOwningHandle()) {
                return; // The snapshot has already been released.
            }
            rocksDb.releaseSnapshot(snapshot);
            set.forEach(RocksDbEntryIterator::invalidateIterator);
            set.clear();
            readOptions.close();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public <K, V> RocksDbEntryIterator<K, V> newIterator(ISerializer serializer, Transaction transaction) {
        // When newIterator is invoked, it's possible that this snapshot has since been invalidated.
        // Requesting an iterator from the RocksDB transaction with an invalid snapshot/readOptions causes
        // an internal assertion failure. Hence, CheckedRocksIterator performs necessary validation before
        // creating the new iterator.
        return executeInSnapshot(readOptions -> {
            RocksDbEntryIterator<K, V> iterator = new RocksDbEntryIterator<>(
                    transaction.getIterator(readOptions),
                    serializer, readOptions, lock, LOAD_VALUES);
            set.add(iterator);
            return iterator;
        });
    }
}
