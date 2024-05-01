package org.corfudb.runtime.collections;

import com.google.common.base.Preconditions;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksIteratorInterface;
import org.rocksdb.Snapshot;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;


/**
 * This class is meant to ensure that RocksDB iterator is accessed in a
 * safe, consistent and side effect free manner.
 */
public class CheckedRocksIterator implements RocksIteratorInterface, AutoCloseable {

    private final RocksIterator rocksIterator;
    private final StampedLock lock;
    private final AtomicBoolean isValid;

    public CheckedRocksIterator(RocksIterator rocksIterator, StampedLock lock, ReadOptions readOptions) {
        // NOTE: Ensure that the read lock is acquired when called from DiskBackedSMRSnapshot.

        // {@link ReadOptions::isOwningHandle} is atomically set to false on
        // {@link ReadOptions::close}. Therefore, if we do not own the handle,
        // the existing snapshot is no longer valid.
        if (!readOptions.isOwningHandle()) {
            throw new TrimmedException("The snapshot is no longer valid.");
        }

        this.isValid = new AtomicBoolean(true);
        this.rocksIterator = rocksIterator;
        this.lock = lock;

        this.rocksIterator.seekToFirst();
    }

    /**
     * A helper method that is called before accessing the iterator.
     * This is necessary to make sure that the iterator hasn't been closed.
     */
    private void checkHandle() {
        if (!lock.isReadLocked()) {
            throw new IllegalStateException("Read lock should have been acquired.");
        }

        if (!rocksIterator.isOwningHandle()) {
            throw new IllegalStateException("RocksIterator has been closed.");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid() {
        return underReadLock(() -> {
            checkHandle();
            boolean res = rocksIterator.isValid();
            status();
            return res;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekToFirst() {
        underReadLock(() -> {
            checkHandle();
            rocksIterator.seekToFirst();
            status();
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekToLast() {
        throw new UnsupportedOperationException("seekToLast");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seek(byte[] target) {
        throw new UnsupportedOperationException("seek");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seek(ByteBuffer target) {
        throw new UnsupportedOperationException("seek");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekForPrev(byte[] target) {
        throw new UnsupportedOperationException("seekForPrev");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seekForPrev(ByteBuffer target) {
        throw new UnsupportedOperationException("seekForPrev");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void next() {
        underReadLock(() -> {
            checkHandle();
            rocksIterator.next();
            status();
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void prev() {
        throw new UnsupportedOperationException("prev");
    }

    /**
     * Returns the key of the current position of the iterator.
     */
    public byte[] key() {
        return underReadLock(() -> {
            checkHandle();
            byte[] res = rocksIterator.key();
            status();
            return res;
        });
    }

    /**
     * Returns the value of the current position of the iterator.
     */
    public byte[] value() {
        return underReadLock(() -> {
            checkHandle();
            byte[] res = rocksIterator.value();
            status();
            return res;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void status() {
        Preconditions.checkState(lock.isWriteLocked() || lock.isReadLocked(),
                "The iterator needs to be accessed under a read or a write lock.");

        try {
            rocksIterator.status();
        } catch (RocksDBException e) {
            throw new UnrecoverableCorfuError("The iterator is in an invalid state", e);
        }
    }

    @Override
    public void refresh() {
        throw new UnsupportedOperationException("refresh");
    }

    @Override
    public void refresh(Snapshot snapshot) throws RocksDBException {
        throw new UnsupportedOperationException("refresh");
    }

    public boolean isOpen() {
        return underReadLock(rocksIterator::isOwningHandle);
    }

    public void invalidateIterator() {
        // {@link DiskBackedSMRSnapshot} should have acquired the lock.
        // {@link StampedLock} is not reentrant.
        if (!lock.isWriteLocked()) {
            throw new IllegalStateException("Write lock should have been acquired.");
        }
        isValid.set(false);
    }

    @Override
    public void close() {
        // All mutators need to acquire the write-lock.
        underWriteLock(rocksIterator::close);
    }

    private void thrownInvalidIterator() {
        throw new TrimmedException("The iterator is no longer valid.");
    }

    private void underWriteLock(Runnable runnable) {
        long stamp = lock.writeLock();
        try {
            if (!isValid.get()) {
                thrownInvalidIterator();
            }
            runnable.run();
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    private void underReadLock(Runnable runnable) {
        long stamp = lock.readLock();
        try {
            if (!isValid.get()) {
                thrownInvalidIterator();
            }
            runnable.run();
        } finally {
            lock.unlockRead(stamp);
        }
    }

    private <T> T underReadLock(Supplier<T> supplier) {
        long stamp = lock.readLock();
        try {
            if (!isValid.get()) {
                thrownInvalidIterator();
            }
            return supplier.get();
        } finally {
            lock.unlockRead(stamp);
        }
    }
}
