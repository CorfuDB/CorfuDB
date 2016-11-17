package org.corfudb.runtime.object;

import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.StreamView;

import java.util.ConcurrentModificationException;
import java.util.Deque;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Created by mwei on 11/13/16.
 */
public class VersionLockedObject<T> {

    T object;
    long version;
    long globalVersion;
    StampedLock lock;
    StreamView sv;
    Deque<AbstractTransactionalContext> txContext;
    AbstractTransactionalContext modifyingContext;

    public VersionLockedObject(T obj, long version, StreamView sv) {
        this.object = obj;
        this.version = version;
        this.sv = sv;
        lock = new StampedLock();
    }

    public void writeReturnVoid(BiConsumer<Long, T> writeFunction) {
        long ts = lock.writeLock();
        try {
            writeFunction.accept(ts, object);
        } finally {
            lock.unlock(ts);
        }
    }

    public <R> R write(BiFunction<Long, T, R> writeFunction) {
        long ts = lock.writeLock();
        try {
            return writeFunction.apply(ts, object);
        } finally {
            lock.unlock(ts);
        }
    }

    public boolean isWriteLocked() {
        return lock.tryOptimisticRead() == 0;
    }

    public <R> R optimisticallyReadAndRetry(BiFunction<Long, T, R> readFunction) {
        long ts = lock.tryOptimisticRead();
        if (ts != 0) {
                R ret = readFunction.apply(version, object);
                if (lock.validate(ts)) {
                    return ret;
                }
        }

        // Optimistic reading failed, retry with a full lock
        ts = lock.readLock();
        try {
            return readFunction.apply(version, object);
        } finally {
            lock.unlockRead(ts);
        }
    }

    public <R> R optimisticallyReadAndWriteOnFail(BiFunction<Long, T, R> readFunction,
                                        BiFunction<Long, T, R> retryFunction) {
        long ts = lock.tryOptimisticRead();
        if (ts != 0) {
            try {
                R ret = readFunction.apply(version, object);
                if (lock.validate(ts)) {
                    return ret;
                }
            } catch (ConcurrentModificationException cme) {
                // thrown by read function to force a full lock.
            }
        }

        // Optimistic reading failed, retry with a full lock
        ts = lock.writeLock();
        try {
            return retryFunction.apply(version, object);
        } finally {
            lock.unlockWrite(ts);
        }
    }

    public <R> R optimisticallyReadThenReadLockThenWriteOnFail
            (BiFunction<Long, T, R> readFunction,
             BiFunction<Long, T, R> retryWriteFunction
             ) {
        long ts = lock.tryOptimisticRead();
        if (ts != 0) {
            try {
                R ret = readFunction.apply(version, object);
                if (lock.validate(ts)) {
                    return ret;
                }
            } catch (ConcurrentModificationException cme) {
                // thrown by read function to force a full lock.
            }
        }
        // Optimistic reading failed, retry with a full lock
        ts = lock.readLock();
        try {
            try {
                return readFunction.apply(version, object);
            } finally {
                lock.unlock(ts);
            }
        } catch (ConcurrentModificationException cme) {
            // throw by read function to force a write lock...
        }
        // reading failed, retry with a full lock
        ts = lock.writeLock();
        try {
            return retryWriteFunction.apply(version, object);
        } finally {
            lock.unlock(ts);
        }
    }

    public void waitOnLock() {
        long ls = lock.readLock();
        lock.unlockRead(ls);
    }
    public T getObjectUnsafe() {
        return object;
    }

    public void setObjectUnsafe(T object) { this.object = object; }

    public void setVersionUnsafe(long version) {
        this.version = version;
    }

    public long getVersionUnsafe() {
        return version;
    }

    public void setGlobalVersionUnsafe(long globalVersion) {
        this.globalVersion = globalVersion;
    }

    public long getGlobalVersionUnsafe() {
        return globalVersion;
    }

    public void setTXContextUnsafe
            (Deque<AbstractTransactionalContext> context) {
        this.txContext = context;
        if (context == null) {
            this.modifyingContext = null;
        } else {
            this.modifyingContext = context.getFirst();
        }
    }

    public AbstractTransactionalContext getModifyingContextUnsafe() {
        return this.modifyingContext;
    }

    public Deque<AbstractTransactionalContext> getTXContextUnsafe() {
        return txContext;
    }

    public boolean isTransactionallyModifiedUnsafe() {
        return txContext != null;
    }

    /** Returns whether this object is owned transactionally by this
     * thread. This is always safe because only the owning thread
     * can acquire/release its own TX lock.
     * @return  True, if this thread owns this object and is
     *          executing transactionally.
     */
    public boolean isTXOwnedByThisThread() {
        return TransactionalContext.getTransactionStack()
                == this.txContext;
    }

    public StreamView getStreamViewUnsafe() { return sv; }

}
