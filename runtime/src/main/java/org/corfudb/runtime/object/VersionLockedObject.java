package org.corfudb.runtime.object;

import io.netty.util.internal.ConcurrentSet;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.exceptions.NoRollbackException;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.WriteSetSMRStream;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.Utils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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
        log.trace("Undo[{}] of {}@{} ({})", this, record.getSMRMethod(),
                record.getEntry() != null ? record.getEntry().getGlobalAddress() : "OPT",
                record.getUndoRecord());
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
     */
    public Object applyUpdateUnsafe(SMREntry entry) {
        log.trace("Apply[{}] of {}@{} ({})", this, entry.getSMRMethod(),
                entry.getEntry() != null ? entry.getEntry().getGlobalAddress() : "OPT",
                entry.getSMRArguments());

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
        log.trace("Rollback[{}] to {}", this, rollbackVersion);
        rollbackStreamUnsafe(smrStream, rollbackVersion);
        log.trace("Rollback[{}] completed", this);
    }

    protected void rollbackStreamUnsafe(ISMRStream stream, long rollbackVersion) {
        // If we're already at or before the given version, there's
        // nothing to do
        if (stream.pos() <= rollbackVersion) {
            return;
        }

        List<SMREntry> entries =  stream.current();

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

            entries = stream.previous();

            if (stream.pos() <= rollbackVersion) {
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

    protected void syncStreamUnsafe(ISMRStream stream, long timestamp) {
        log.trace("Sync[{}] {}", this, (timestamp == Address.OPTIMISTIC)
                                            ? "Optimistic" : "to " + timestamp);
        long syncTo = (timestamp == Address.OPTIMISTIC) ? Address.MAX : timestamp;
        stream.remainingUpTo(syncTo)
                .stream()
                .forEachOrdered(entry -> {
                    try {
                        Object res = applyUpdateUnsafe(entry);
                        if (timestamp == Address.OPTIMISTIC) {
                            entry.setUpcallResult(res);
                        }
                        else if (pendingUpcalls.contains(entry.getEntry().getGlobalAddress())) {
                            log.debug("Sync[{}] Upcall Result {}", entry.getEntry().getGlobalAddress());
                            upcallResults.put(entry.getEntry().getGlobalAddress(), res == null ?
                                    NullValue.NULL_VALUE : res);
                            pendingUpcalls.remove(entry.getEntry().getGlobalAddress());
                        }
                        entry.setUpcallResult(res);
                    } catch (Exception e) {
                        log.error("Sync[{}] Error: Couldn't execute upcall due to {}", this, e);
                        throw new RuntimeException(e);
                    }
                });
    }
    /** Update the object. Ensure that you have the write lock before calling
     * this function...
     * @param timestamp         The timestamp to update the object to.
     */
    public void syncObjectUnsafe(long timestamp) {
        // If there is an optimistic stream attached,
        // and it belongs to this thread use that
        if (optimisticStream != null &&
                optimisticStream.isStreamForThisTransaction()) {
            // If there are no updates, ensure we are at the right snapshot
            if (optimisticStream.pos() == Address.NEVER_READ) {
                final WriteSetSMRStream currentOptimisticStream =
                        optimisticStream;
                // It's necessary to roll back optimistic updates before
                // doing a sync of the regular log
                optimisticRollbackUnsafe();
                // If we are too far ahead, roll back to the past
                if (getVersionUnsafe() > timestamp) {
                    try {
                        rollbackObjectUnsafe(timestamp);
                    } catch (NoRollbackException nre) {
                        resetUnsafe();
                    }
                }
                // Now sync the regular log
                syncStreamUnsafe(smrStream, timestamp);
                // It's possible that due to reset,
                // the optimistic stream is no longer
                // present. Restore it.
                optimisticStream = currentOptimisticStream;
            }
            syncStreamUnsafe(optimisticStream, Address.OPTIMISTIC);
        } else {
            // If there is an optimistic stream for another
            // transaction, remove it by rolling it back first
            if (this.optimisticStream != null) {
                optimisticRollbackUnsafe();
                this.optimisticStream = null;
            }
            // If we are too far ahead, roll back to the past
            if (getVersionUnsafe() > timestamp) {
                try {
                    rollbackObjectUnsafe(timestamp);
                    // Rollback successfully got us to the right
                    // version, we're done.
                    if (getVersionUnsafe() == timestamp) {
                        return;
                    }
                } catch (NoRollbackException nre) {
                    resetUnsafe();
                }
            }
            // TODO: remember furthest sync to avoid unnecessary
            // extra syncs
            syncStreamUnsafe(smrStream, timestamp);
        }
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

    public void optimisticCommitUnsafe() {
        optimisticStream = null;
    }

    public boolean optimisticallyOwnedByThreadUnsafe() {
        return optimisticStream == null ? false : optimisticStream.isStreamForThisTransaction();
    }

    protected void optimisticRollbackUnsafe() {
        try {
            log.trace("OptimisticRollback[{}] started", this);
            rollbackStreamUnsafe(this.optimisticStream, Address.NEVER_READ);
            log.trace("OptimisticRollback[{}] complete", this);
        } catch (NoRollbackException nre) {
            log.debug("OptimisticRollback[{}] failed", this);
            resetUnsafe();
        }
    }

    public void setOptimisticStreamUnsafe(WriteSetSMRStream optimisticStream) {
        if (this.optimisticStream != null) {
            optimisticRollbackUnsafe();
        }
        this.optimisticStream = optimisticStream;
    }

    /** Execute the given function under a write lock, not returning
     * anything.
     * @param writeFunction The function to execute under the write lock.
     */
    public void writeReturnVoid(BiConsumer<Long, VersionLockedObject<T>> writeFunction) {
        write((a,v) -> {writeFunction.accept(a,v); return null; });
    }

    /** Execute the given function under a write lock.
     *
     * @param writeFunction The function to execute under a write lock.
     * @param <R>           The type of the return value of the write function.
     * @return              The return value of the write function.
     */
    public <R> R write(BiFunction<Long, VersionLockedObject<T>, R> writeFunction) {
        long ts = lock.writeLock();
        try {
            return writeFunction.apply(ts, this);
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

    public <R> R optimisticallyReadThenReadLockThenWriteOnFail
            (BiFunction<Long, VersionLockedObject<T>, R> readFunction,
             BiFunction<Long, VersionLockedObject<T>, R> retryWriteFunction
            ) {
        long ts = lock.tryOptimisticRead();
        if (ts != 0) {
            try {
                R ret = readFunction.apply(getVersionUnsafe(), this);
                if (lock.validate(ts)) {
                    return ret;
                }
            } catch (ConcurrentModificationException cme) {
                // thrown by read function to force a full lock.
            }
        }
        // Optimistic reading failed, retry with a full lock
        try {
            ts = lock.tryReadLock(1, TimeUnit.SECONDS);
                try {
                    return readFunction.apply(getVersionUnsafe(), this);
                } finally {
                    lock.unlock(ts);
                }
        } catch (InterruptedException ie) {
            log.debug("ReadLock[{}] Timed out, retrying with write lock");
            throw new ConcurrentModificationException();
        } catch (ConcurrentModificationException cme) {
            // throw by read function to force a append lock...
        }
        // reading failed, retry with a full lock
        ts = lock.writeLock();
        try {
            return retryWriteFunction.apply(getVersionUnsafe(), this);
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

    public boolean isOptimisticallyModifiedUnsafe() {
        return optimisticStream != null &&
                optimisticStream.pos() != Address.NEVER_READ;
    }

    /** Reset this object to the uninitialized state. */
    public void resetUnsafe() {
        log.debug("Reset[{}]", this);
        object = newObjectFn.get();
        smrStream.reset();
        optimisticStream = null;
    }

    public UUID getID() {
        return smrStream.getID();
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
        return object.getClass().getSimpleName() + "[" + Utils.toReadableID(smrStream.getID()) + "]@"
                + (getVersionUnsafe() == Address.NEVER_READ ? "NR" : getVersionUnsafe())
                + (optimisticStream == null ? "" : "+" + optimisticStream.pos());
    }
}
