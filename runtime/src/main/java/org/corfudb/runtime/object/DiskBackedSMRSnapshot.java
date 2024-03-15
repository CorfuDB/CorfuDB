package org.corfudb.runtime.object;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
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
import java.util.function.Supplier;

import static org.corfudb.runtime.collections.RocksDbEntryIterator.LOAD_VALUES;

@Slf4j
public class DiskBackedSMRSnapshot<S extends SnapshotGenerator<S>> implements SMRSnapshot<S> {

    private final VersionedObjectIdentifier version;
    private final StampedLock lock = new StampedLock();
    private final OptimisticTransactionDB rocksDb;
    private final Snapshot snapshot;
    private final ViewGenerator<S> viewGenerator;
    private final ReadOptions readOptions;
    private final WriteOptions writeOptions;
    private final ColumnFamilyRegistry columnFamilyRegistry;

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
    }

    private boolean isInvalid() {
        if (!this.rocksDb.isOwningHandle()) {
            log.error("Invalid RocksDB instance {} for snapshot {}.",
                    rocksDb.getNativeHandle(), version);
            return true; // RocksDB instance has already been closed.
        }

        // Check if the snapshot has already been released.
        return !this.readOptions.isOwningHandle();
    }

    public <V> V executeInSnapshot(Function<ReadOptions, V> function) {
        long stamp = lock.readLock();
        try {
            if (isInvalid()) {
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
            if (isInvalid()) {
                throw new TrimmedException("Snapshot is not longer active " + version);
            }
            consumer.accept(this.readOptions);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    public <V> V executeUnderWriteLock(Supplier<V> supplier) {
        long stamp = lock.writeLock();
        try {
            if (isInvalid()) {
                throw new TrimmedException("Snapshot is not longer active " + version);
            }
            return supplier.get();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    public S consume() {
        return executeUnderWriteLock(() ->
                viewGenerator.newView(new RocksDbTx<>(rocksDb, writeOptions, this, columnFamilyRegistry)));
    }

    public void release() {
        long stamp = lock.writeLock();
        try {
            if (isInvalid()) {
                return;
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
        // When getIterator is invoked, it's possible that this snapshot has since been invalidated.
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
