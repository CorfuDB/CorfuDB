package org.corfudb.runtime.object;

import io.netty.util.internal.ConcurrentSet;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.exceptions.NoRollbackException;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.WriteSetSMRStream;
import org.corfudb.runtime.view.Address;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * The version locked object keeps track of where -- in the history of updates -- is the current state of the underying object.
 *
 * it maintains a shallow undo-log, back to the earliest open-transaction's snapshot.
 * it also maintains an optimistic undo-log, for the current open-transaction, if any.
 *
 * <p>
 * Created by mwei on 11/13/16.
 */
@Slf4j
public class VersionLockedObject<T> {

    /**
     * The actual underlying object.
     */
    T object;

    /** Whether or not the object was optimistically
     * modified or not.
     */
    boolean optimisticallyModified;

    /** A list of upcalls pending in the system. The proxy keeps this
     * set so it can remember to save the upcalls for pending requests.
     */
    final Set<Long> pendingUpcalls;

    // This enum is necessary because null cannot be inserted
    // into a ConcurrentHashMap.
    enum NullValue {
        NULL_VALUE
    }

    /** A list of upcall results, keyed by the address they were
     * requested.
     */
    final Map<Long, Object> upcallResults;


    /** A lock, which controls access to modifications to
     * the object. Any access to unsafe methods should
     * obtain the lock.
     */
    private final StampedLock lock;

    /** The stream view this object is backed by.
     *
     */
    private final ISMRStream smrStream;

    /** The optimistic SMR stream on this object, if any
     *
     */
    private WriteSetSMRStream optimisticStream;

    /** If the object reflects optimistic updates, the
     * context which made those updates.
     */
    private AbstractTransactionalContext modifyingContext;

    /** The upcall map for this object. */
    private final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;

    /** The undo record function map for this object. */
    private final Map<String, IUndoRecordFunction<T>> undoRecordFunctionMap;

    /** The undo target map for this object. */
    private final Map<String, IUndoFunction<T>> undoFunctionMap;

    /** The reset set for this object. */
    private final Set<String> resetSet;

    /** A function that generates a new instance of this object. */
    private final Supplier<T> newObjectFn;

    public VersionLockedObject(Supplier<T> newObjectFn,
                               StreamViewSMRAdapter smrStream,
                  Map<String, ICorfuSMRUpcallTarget<T>> upcallTargets,
                  Map<String, IUndoRecordFunction<T>> undoRecordTargets,
                  Map<String, IUndoFunction<T>> undoTargets,
                  Set<String> resetSet)
    {
        this.smrStream = smrStream;

        this.upcallTargetMap = upcallTargets;
        this.undoRecordFunctionMap = undoRecordTargets;
        this.undoFunctionMap = undoTargets;
        this.resetSet = resetSet;

        this.newObjectFn = newObjectFn;
        this.object = newObjectFn.get();

        this.pendingUpcalls = new ConcurrentSet<>();
        this.upcallResults = new ConcurrentHashMap<>();

        lock = new StampedLock();
    }

    /** Given a SMR entry with an undo record, undo the update.
     *
      * @param record   The record to undo.
     */
    public void applyUndoRecordUnsafe(SMREntry record) {
        IUndoFunction<T> undoFunction =
                undoFunctionMap.get(record.getSMRMethod());
        // If the undo function exists, apply it.
        if (undoFunction != null) {
            undoFunction.doUndo(object, record.getUndoRecord(),
                    record.getSMRArguments());
            return;
        }
        // If this is a reset, undo by restoring the
        // previous state.
        else if (resetSet.contains(record.getSMRMethod())) {
            object = (T) record.getUndoRecord();
            // clear the undo record, since it is now
            // consumed (the object may change)
            record.clearUndoRecord();
            return;
        }
        // Otherwise we don't know how to undo,
        // throw a runtime exception, because
        // this is a bug, undoRecords we don't know
        // how to process shouldn't be in the log.
        throw new RuntimeException("Unknown undo record in undo log");
    }


    /** Apply an SMR update to the object, possibly optimistically.
     * @param entry         The entry to apply.
     * @param timestamp     The timestamp the update should be applied at,
     *                      or Address.OPTIMISTIC if the update is optimistic.
     * @return              The upcall result, if available.
     */
    public Object applyUpdateUnsafe(SMREntry entry, long timestamp) {
        ICorfuSMRUpcallTarget<T> target = upcallTargetMap.get(entry.getSMRMethod());
        if (target == null) {
            throw new RuntimeException("Unknown upcall " + entry.getSMRMethod());
        }

        // No undo record is present
        if (!entry.isUndoable()) {
            // Can we generate an undo record?
            IUndoRecordFunction<T> undoRecordTarget =
                    undoRecordFunctionMap
                            .get(entry.getSMRMethod());
            if (undoRecordTarget != null) {
                // calculate the undo record
                entry.setUndoRecord(undoRecordTarget
                        .getUndoRecord(object, entry.getSMRArguments()));
            } else if (resetSet.contains(entry.getSMRMethod())) {
                // This entry actually resets the object. So here
                // we can safely get a new instance, and add the
                // previous instance to the undo log.
                entry.setUndoRecord(object);
                object = newObjectFn.get();
            }
        }

        // now invoke the upcall
        Object ret = target.upcall(object, entry.getSMRArguments());
        if (timestamp != Address.OPTIMISTIC) {
        } else {
            optimisticallyModified = true;
        }
        return ret;
    }

    /** Roll the object back to the supplied version if possible.
     * This function may roll back to a point prior to the requested version.
     * Otherwise, throws a NoRollbackException.
     *
     * Unsafe, requires that the caller has acquired a write lock.
     *
     * @param  rollbackVersion      The version to rollback to.
     * @throws NoRollbackException  If the object cannot be rolled back to
     *                              the supplied version.
     */
    public void rollbackObjectUnsafe(long rollbackVersion) {
        // If we're already at or before the given version, there's
        // nothing to do
        if (getVersionUnsafe() <= rollbackVersion) {
            return;
        }

        List<SMREntry> entries =  smrStream.current();

        while(entries != null) {
            if (entries.stream().allMatch(x -> x.isUndoable())) {
                // start from the end, process one at a time
                ListIterator<SMREntry> it =
                        entries.listIterator(entries.size());
                while (it.hasPrevious()) {
                    applyUndoRecordUnsafe(it.previous());
                }
            }
            else {
                throw new NoRollbackException();
            }

            entries = smrStream.previous();

            if (getVersionUnsafe() <= rollbackVersion) {
                return;
            }
        }

        throw new NoRollbackException();
    }

    /** Move the pointer for this object (effectively, forcefuly
     * change the version of this object without playing
     * any updates).
     * @param globalAddress     The global address to set the pointer to
     */
    public void seek(long globalAddress) {
        smrStream.seek(globalAddress);
    }

    /** Update the object. Ensure that you have the write lock before calling
     * this function...
     * @param timestamp         The timestamp to update the object to.
     */
    public void syncObjectUnsafe(long timestamp) {
        smrStream.remainingUpTo(timestamp)
            .stream()
            .forEachOrdered(entry -> {
                try {
                    Object res = applyUpdateUnsafe(entry, entry.getEntry().getGlobalAddress());
                    if (pendingUpcalls.contains(entry.getEntry().getGlobalAddress())) {
                        log.debug("upcall result for {}", entry.getEntry().getGlobalAddress());
                        upcallResults.put(entry.getEntry().getGlobalAddress(), res == null ?
                                NullValue.NULL_VALUE : res);
                        pendingUpcalls.remove(entry.getEntry().getGlobalAddress());
                    }
                } catch (Exception e) {
                    log.error("Error: Couldn't execute upcall due to {}", e);
                    throw new RuntimeException(e);
                }
            });
    }

    public long logUpdate(SMREntry entry, boolean saveUpcall) {
        return smrStream.append(entry, t -> {
            if (saveUpcall) {
                pendingUpcalls.add(t.getToken());
            }
            return true;
        }, t -> {
            if (saveUpcall) {
                pendingUpcalls.remove(t.getToken());
            }
            return true;
        });
    }

    public WriteSetSMRStream getOptimisticStreamUnsafe() {
        return optimisticStream;
    }

    public void clearOptimisticStreamUnsafe() {
        optimisticStream = null;
    }

    public void setOptimisticStreamUnsafe(WriteSetSMRStream optimisticStream) {
        this.optimisticStream = optimisticStream;
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
            R ret = readFunction.apply(getVersionUnsafe(), object);
            if (lock.validate(ts)) {
                return ret;
            }
        }

        // Optimistic reading failed, retry with a full lock
        ts = lock.readLock();
        try {
            return readFunction.apply(getVersionUnsafe(), object);
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
                R ret = readFunction.apply(getVersionUnsafe(), object);
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
                return readFunction.apply(getVersionUnsafe(), object);
            } finally {
                lock.unlock(ts);
            }
        } catch (ConcurrentModificationException cme) {
            // throw by read function to force a append lock...
        }
        // reading failed, retry with a full lock
        ts = lock.writeLock();
        try {
            return retryWriteFunction.apply(getVersionUnsafe(), object);
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

    public long getVersionUnsafe() {
        return smrStream.pos();
    }

    public AbstractTransactionalContext getModifyingContextUnsafe() {
        return this.modifyingContext;
    }

    public void setTXContextUnsafe
            (AbstractTransactionalContext context) {
        this.modifyingContext = context;
    }

    public boolean isOptimisticallyModifiedUnsafe() {
        return optimisticallyModified;
    }

    public void clearOptimisticallyModifiedUnsafe() {
        optimisticallyModified = false;
    }

    /** Reset this object to the uninitialized state. */
    public void resetUnsafe() {
        object = newObjectFn.get();
        optimisticallyModified = false;
        modifyingContext = null;
        smrStream.reset();
    }

    /** Generate the summary string for this version locked object.
     *
     * The format of this string is [type]@[version][+]
     * (where + is the optimistic flag)
     *
     * @return  The summary string for this version locked object
     */
    @Override
    public String toString() {
        return object.getClass().getSimpleName() + "@"
                + (getVersionUnsafe() == Address.NEVER_READ ? "NR" : getVersionUnsafe())
                + (optimisticallyModified ? "+" : "");
    }
}
