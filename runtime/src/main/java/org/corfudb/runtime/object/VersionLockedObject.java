package org.corfudb.runtime.object;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.exceptions.NoRollbackException;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.view.stream.IStreamView;

import java.util.*;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

/**
 * A version locked object. The version locked object contains a
 * the state of an object as well as a history of modifications to
 * the object, which can be optimistic.
 * <p>
 * Created by mwei on 11/13/16.
 */
@Slf4j
public class VersionLockedObject<T> {

    // maybe this shouldn't be constant.
    static final int MAX_UNDO_SIZE = 50;

    /**
     * The actual underlying object.
     */
    T object;

    /** The version of the underlying object.
     *
     */
    long version;
    long globalVersion;

    /** A lock, which controls access to modifications to
     * the object. Any access to unsafe methods should
     * obtain the lock.
     */
    private StampedLock lock;

    /** The stream view this object is backed by.
     *
     */
    private IStreamView sv;

    /** If the object reflects optimistic updates, the
     * context which made those updates.
     */
    private AbstractTransactionalContext modifyingContext;

    /** An undo log, which records undo entries for the object.
     *
     */
    private Deque<SMREntry> undoLog;

    /** An optimistic undo log, which records undo entries for
     * optimistic changes to the object.
     */
    private Deque<SMREntry> optimisticUndoLog;

    /** True, if the object is optimistically modified.
     *
     */
    private boolean optimisticallyModified;

    /** True, if optimistic changes to this object can be undone.
     *
     */
    private boolean optimisticallyUndoable;

    /** The number of optimistic changes made to this object.
     *
     */
    private int optimisticVersion;


    /** The upcall map for this object. */
    private final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;

    /** The undo record function map for this object. */
    private final Map<String, IUndoRecordFunction<T>> undoRecordFunctionMap;

    /** The undo target map for this object. */
    private final Map<String, IUndoFunction<T>> undoFunctionMap;

    public VersionLockedObject(T obj, long version, IStreamView sv,
                  Map<String, ICorfuSMRUpcallTarget<T>> upcallTargets,
                  Map<String, IUndoRecordFunction<T>> undoRecordTargets,
                  Map<String, IUndoFunction<T>> undoTargets)
    {
        this.object = obj;
        this.version = version;
        this.sv = sv;

        this.undoLog = new LinkedList<>();
        this.optimisticUndoLog = new LinkedList<>();

        this.optimisticallyUndoable = true;
        this.optimisticallyModified = false;

        this.optimisticVersion = 0;

        this.upcallTargetMap = upcallTargets;
        this.undoRecordFunctionMap = undoRecordTargets;
        this.undoFunctionMap = undoTargets;
        lock = new StampedLock();
    }

    public int getOptimisticVersionUnsafe() {
        return this.optimisticVersion;
    }

    public void clearOptimisticVersionUnsafe() {
        this.optimisticVersion = 0;
    }

    public void optimisticVersionIncrementUnsafe() {
        this.optimisticVersion++;
    }

    /** Commits all optimistic changes as a new version.
     *
     */
    public void optimisticCommitUnsafe(long version) {
        // TODO: validate the caller actually has a write lock.
        // TODO: merge the optimistic undo log into the undo log
        optimisticUndoLog.clear();
        optimisticVersion = 0;
        optimisticallyModified = false;
        this.version = version;
        // TODO: fix the stream view pointer seek, for now
        // read will read the tx commit entry.
        sv.next();
        modifyingContext = null;
    }

    /** Rollback any optimistic changes, if possible.
     *  Unsafe, requires that the caller has acquired a write lock.
     */
    public void optimisticRollbackUnsafe() {
        // TODO: validate the caller actually has a write lock.

        if (!optimisticallyModified) {
            log.debug("nothing to roll");
            return;
        }
        if (!optimisticallyUndoable) {
            throw new NoRollbackException();
        }
        // The undo log is a stack, where the last entry applied
        // is at the front, which is the same order stream() returns
        // entries.
        optimisticUndoLog.stream()
                .forEachOrdered(x -> {
                    if (!x.isUndoable()) {
                        throw new NoRollbackException(x);
                    }
                    undoFunctionMap.get(x.getSMRMethod())
                            .doUndo(object, x.getUndoRecord(), x.getSMRArguments());
                });
        optimisticUndoLog.clear();
        optimisticallyModified = false;
        optimisticVersion = 0;
        modifyingContext = null;
    }


    /** Apply an SMR update to the object, possibly optimistically,
     * if set.
     * @param entry         The entry to apply.
     * @param isOptimistic  Whether the update is optimistic or not.
     * @return              The upcall result, if available.
     */
    public Object applyUpdateUnsafe(SMREntry entry, boolean isOptimistic) {
        // TODO: validate the caller actually has a write lock.
        try {
            ICorfuSMRUpcallTarget<T> target =
                    upcallTargetMap
                            .get(entry.getSMRMethod());
            if (target == null) {
                throw new Exception("Unknown upcall " + entry.getSMRMethod());
            }
            // Can we generate an undo record?
            IUndoRecordFunction<T> undoRecordTarget =
                    undoRecordFunctionMap
                            .get(entry.getSMRMethod());
            if (undoRecordTarget != null) {
                entry.setUndoRecord(undoRecordTarget
                        .getUndoRecord(object, entry.getSMRArguments()));
                entry.setUndoable(true);
                if (isOptimistic && optimisticallyUndoable) {
                    optimisticUndoLog.addFirst(entry);
                }
            } else {
                if (isOptimistic) {
                    optimisticallyUndoable = false;
                }
            }
            if (isOptimistic) {
                optimisticallyModified = true;
                optimisticVersionIncrementUnsafe();
            }
            return target.upcall(object, entry.getSMRArguments());
        } catch (Exception e) {
            log.error("Error: Couldn't execute upcall due to {}", e);
            throw new RuntimeException(e);
        }
    }

    /** Execute the given function under a write lock, not returning
     * anything.
     * @param writeFunction The function to execute under the write lock.
     */
    public void writeReturnVoid(BiConsumer<Long, T> writeFunction) {
        write((a,v) -> {writeFunction.accept(a,v); return null; });
    }

    /** Execute the given function under a write lock.
     *
     * @param writeFunction The function to execute under a write lock.
     * @param <R>           The type of the return value of the write function.
     * @return              The return value of the write function.
     */
    public <R> R write(BiFunction<Long, T, R> writeFunction) {
        long ts = lock.writeLock();
        try {
            return writeFunction.apply(ts, object);
        } finally {
            lock.unlock(ts);
        }
    }

    /** Return whether or not the object is locked for write.
     *
     * @return  True, if the object is locked for write.
     */
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
            // throw by read function to force a append lock...
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

    public void setObjectUnsafe(T object) {
        this.object = object;
    }

    public long getVersionUnsafe() {
        return version;
    }

    public void setVersionUnsafe(long version) {
        this.version = version;
    }

    public long getGlobalVersionUnsafe() {
        return globalVersion;
    }

    public void setGlobalVersionUnsafe(long globalVersion) {
        this.globalVersion = globalVersion;
    }

    public AbstractTransactionalContext getModifyingContextUnsafe() {
        return this.modifyingContext;
    }

    public void setTXContextUnsafe
            (AbstractTransactionalContext context) {
        this.modifyingContext = context;
    }

    public IStreamView getStreamViewUnsafe() {
        return sv;
    }

    public boolean isOptimisticallyUndoableUnsafe() {
        return optimisticallyUndoable;
    }

    public boolean isOptimisticallyModifiedUnsafe() {
        return optimisticallyModified;
    }

    public void resetStreamViewUnsafe() {
        sv.reset();
    }
}
