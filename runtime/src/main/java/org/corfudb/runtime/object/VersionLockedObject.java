package org.corfudb.runtime.object;

import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.exceptions.NoRollbackException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.transactions.WriteSetSMRStream;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO Discard TransactionStream for building maps but not for constructing tails

/**
 * The VersionLockedObject maintains a versioned object which is
 * backed by an ISMRStream, and is optionally backed by an additional
 * optimistic update stream.
 *
 * <p>Users of the VersionLockedObject cannot access the versioned object
 * directly, rather they use the access() and update() methods to
 * read and manipulate the object.
 *
 * <p>access() and update() allow the user to provide functions to execute
 * under locks. These functions execute various "unsafe" methods provided
 * by this object which inspect and manipulate the object state.
 *
 * <p>syncObjectUnsafe() enables the user to bring the object to a given version,
 * and the VersionLockedObject manages any sync or rollback of updates
 * necessary.
 *
 * <p>Created by mwei on 11/13/16.
 */
@Slf4j
public class VersionLockedObject<T> {

    /**
     * The actual underlying object.
     */
    T object;

    /**
     * A list of upcalls pending in the system. The proxy keeps this
     * set so it can remember to save the upcalls for pending requests.
     */
    final Set<Long> pendingUpcalls;

    // This enum is necessary because null cannot be inserted
    // into a ConcurrentHashMap.
    enum NullValue {
        NULL_VALUE
    }

    /**
     * A list of upcall results, keyed by the address they were
     * requested.
     */
    final Map<Long, Object> upcallResults;


    /**
     * A lock, which controls access to modifications to
     * the object. Any access to unsafe methods should
     * obtain the lock.
     */
    private final StampedLock lock;

    /**
     * The stream view this object is backed by.
     */
    private final ISMRStream smrStream;

    /**
     * The optimistic SMR stream on this object, if any.
     */
    private WriteSetSMRStream optimisticStream;

    /**
     * The upcall map for this object.
     */
    private final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;

    /**
     * The undo record function map for this object.
     */
    private final Map<String, IUndoRecordFunction<T>> undoRecordFunctionMap;

    /**
     * The undo target map for this object.
     */
    private final Map<String, IUndoFunction<T>> undoFunctionMap;

    /**
     * The reset set for this object.
     */
    private final Set<String> resetSet;

    /**
     * A function that generates a new instance of this object.
     */
    private final Supplier<T> newObjectFn;

    /**
     * Correctness Logging
     */
    private final Logger correctnessLogger = LoggerFactory.getLogger("correctness");

    /**
     * The VersionLockedObject maintains a versioned object which is backed by an ISMRStream,
     * and is optionally backed by an additional optimistic update stream.
     *
     * @param newObjectFn       A function passed to instantiate a new instance of this object.
     * @param smrStream         Stream View backing this object.
     * @param upcallTargets     UpCall map for this object.
     * @param undoRecordTargets Undo record function map for this object.
     * @param undoTargets       Undo functions map.
     * @param resetSet          Reset set for this object.
     */
    public VersionLockedObject(Supplier<T> newObjectFn,
                               StreamViewSMRAdapter smrStream,
                               Map<String, ICorfuSMRUpcallTarget<T>> upcallTargets,
                               Map<String, IUndoRecordFunction<T>> undoRecordTargets,
                               Map<String, IUndoFunction<T>> undoTargets,
                               Set<String> resetSet) {
        this.smrStream = smrStream;

        this.upcallTargetMap = upcallTargets;
        this.undoRecordFunctionMap = undoRecordTargets;
        this.undoFunctionMap = undoTargets;
        this.resetSet = resetSet;

        this.newObjectFn = newObjectFn;
        this.object = newObjectFn.get();
        this.pendingUpcalls = ConcurrentHashMap.newKeySet();
        this.upcallResults = new ConcurrentHashMap<>();

        lock = new StampedLock();
    }

    /**
     * Access the internal state of the object, trying first to optimistically access
     * the object, then obtaining a write lock the optimistic access fails.
     *
     * <p>If the directAccessCheckFunction returns true, then we execute the accessFunction
     * without running updateFunction. If false, we execute the updateFunction to
     * allow the user to modify the state of the object before calling accessFunction.
     *
     * <p>directAccessCheckFunction is executed under an optimistic read lock. Read-only
     * unsafe operations are permitted.
     *
     * <p>updateFunction is executed under a write lock. Both read and write unsafe operations
     * are permitted.
     *
     * <p>accessFunction is accessed either under a read lock or write lock depending on
     * whether an update was necessary or not.
     *
     * @param directAccessCheckFunction A function which returns True if the object can be
     *                                  accessed without being updated.
     * @param updateFunction            A function which is executed when direct access
     *                                  is not allowed and the object must be updated.
     * @param accessFunction            A function which allows the user to directly access
     *                                  the object while locked in the state enforced by
     *                                  either the directAccessCheckFunction or updateFunction.
     * @param <R>                       The type of the access function return.
     * @return Returns the access function.
     */
    public <R> R access(Function<VersionLockedObject<T>, Boolean> directAccessCheckFunction,
                        Consumer<VersionLockedObject<T>> updateFunction,
                        Function<T, R> accessFunction) {
        // First, we try to do an optimistic read on the object, in case it
        // meets the conditions for direct access.
        long ts = lock.tryOptimisticRead();
        if (ts != 0) {
            try {
            if (directAccessCheckFunction.apply(this)) {
                log.trace("Access [{}] Direct (optimistic-read) access at {}",
                        this, getVersionUnsafe());
                    R ret = accessFunction.apply(object);

                    long versionForCorrectness = getVersionUnsafe();
                    if (lock.validate(ts)) {
                        correctnessLogger.trace("Version, {}", versionForCorrectness);
                        return ret;
                    }
                }
            } catch (Exception e) {
                // If we have an exception, we didn't get a chance to validate the lock.
                // If it's still valid, then we should re-throw the exception since it was
                // on a correct view of the object.
                if (lock.validate(ts)) {
                    throw e;
                }
                // Otherwise, it is not on a correct view of the object (the object was
                // modified) and we should try again by upgrading the lock.
                log.warn("Access [{}] Direct (optimistic-read) exception, upgrading lock",
                        this);
            }
        }
        // Next, we just upgrade to a full write lock if the optimistic
        // read fails, since it means that the state of the object was
        // updated.
        try {
            // Attempt an upgrade
            ts = lock.tryConvertToWriteLock(ts);
            // Upgrade failed, try conversion again
            if (ts == 0) {
                ts = lock.writeLock();
            }
            // Check if direct access is possible (unlikely).
            if (directAccessCheckFunction.apply(this)) {
                log.trace("Access [{}] Direct (writelock) access at {}", this, getVersionUnsafe());
                R ret = accessFunction.apply(object);

                long versionForCorrectness = getVersionUnsafe();
                if (lock.validate(ts)) {
                    correctnessLogger.trace("Version, {}", versionForCorrectness);
                    return ret;
                }
            }
            // If not, perform the update operations
            updateFunction.accept(this);
            correctnessLogger.trace("Version, {}", getVersionUnsafe());
            log.trace("Access [{}] Updated (writelock) access at {}", this, getVersionUnsafe());
            return accessFunction.apply(object);
            // And perform the access
        } finally {
            lock.unlock(ts);
        }
    }

    /**
     * Update the object under a write lock.
     *
     * @param updateFunction A function to execute once the write lock has been acquired.
     * @param <R>            The type of the return of the updateFunction.
     * @return The return value of the update function.
     */
    public <R> R update(Function<VersionLockedObject<T>, R> updateFunction) {
        long ts = 0;
        try {
            ts = lock.writeLock();
            log.trace("Update[{}] (writelock)", this);
            return updateFunction.apply(this);
        } finally {
            lock.unlock(ts);
        }
    }

    /**
     * Roll the object back to the supplied version if possible.
     * This function may roll back to a point prior to the requested version.
     * Otherwise, throws a NoRollbackException.
     *
     * <p>Unsafe, requires that the caller has acquired a write lock.
     *
     * @param rollbackVersion The version to rollback to.
     * @throws NoRollbackException If the object cannot be rolled back to
     *                             the supplied version.
     */
    public void rollbackObjectUnsafe(long rollbackVersion) {
        log.trace("Rollback[{}] to {}", this, rollbackVersion);
        rollbackStreamUnsafe(smrStream, rollbackVersion);
        log.trace("Rollback[{}] completed", this);
    }

    /**
     * Move the pointer for this object (effectively, forcefuly
     * change the version of this object without playing
     * any updates).
     *
     * @param globalAddress The global address to set the pointer to
     */
    public void seek(long globalAddress) {
        smrStream.seek(globalAddress);
    }

    /**
     * Bring the object to the requested version, rolling back or syncing
     * the object from the log if necessary to reach the requested version.
     *
     * @param timestamp The timestamp to update the object to.
     */
    public void syncObjectUnsafe(long timestamp) {
        // If there is an optimistic stream attached,
        // and it belongs to this thread use that
        if (optimisticallyOwnedByThreadUnsafe()) {
            // If there are no updates, ensure we are at the right snapshot
            if (optimisticStream.pos() == Address.NEVER_READ) {
                final WriteSetSMRStream currentOptimisticStream =
                        optimisticStream;
                // If we are too far ahead, roll back to the past
                if (getVersionUnsafe() > timestamp) {
                    try {
                        rollbackObjectUnsafe(timestamp);
                    } catch (NoRollbackException nre) {
                        log.warn("SyncObjectUnsafe[{}] to {} failed {}", this, timestamp, nre);
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
            this.rollBackToTimestamp(timestamp);
            // Attempt to sync stream to timestamp, it might have to sync to the tail to build the
            // history
            syncStreamUnsafe(smrStream, timestamp);
            // Check if stream was synced ahead and roll back to timestamp if necessary
            this.rollBackToTimestamp(timestamp);
        }
    }

    private void rollBackToTimestamp(long timestamp) {
        if (getVersionUnsafe() > timestamp) {
            try {
                rollbackObjectUnsafe(timestamp);
            } catch (NoRollbackException nre) {
                log.warn("Rollback[{}] to {} failed {}", this, timestamp, nre);
                resetUnsafe();
            }
        }
    }

    /**
     * Log an update to this object, noting a request to save the
     * upcall result if necessary.
     *
     * @param entry      The entry to log.
     * @param saveUpcall True, if the upcall result should be
     *                   saved, false otherwise.
     * @return The address the update was logged at.
     */
    public long logUpdate(SMREntry entry, boolean saveUpcall) {
        return smrStream.append(entry,
                t -> {
                    if (saveUpcall) {
                        pendingUpcalls.add(t.getToken().getTokenValue());
                    }
                    return true;
                },
                t -> {
                    if (saveUpcall) {
                        pendingUpcalls.remove(t.getToken().getTokenValue());
                    }
                    return true;
                });
    }

    /**
     * Get a handle to the optimistic stream.
     */
    public WriteSetSMRStream getOptimisticStreamUnsafe() {
        return optimisticStream;
    }

    /**
     * Drop the optimistic stream, effectively making optimistic updates
     * to this object permanent.
     */
    public void optimisticCommitUnsafe() {
        optimisticStream = null;
    }

    /**
     * Check whether or not this object was modified by this thread.
     *
     * @return True, if the object was modified by this thread. False otherwise.
     */
    public boolean optimisticallyOwnedByThreadUnsafe() {
        WriteSetSMRStream optimisticStream = this.optimisticStream;

        return optimisticStream == null ? false : optimisticStream.isStreamForThisThread();
    }

    /**
     * Set the optimistic stream for this thread, rolling back
     * any previous threads if they were present.
     *
     * @param optimisticStream The new optimistic stream to install.
     */
    public void setOptimisticStreamUnsafe(WriteSetSMRStream optimisticStream) {
        if (this.optimisticStream != null) {
            optimisticRollbackUnsafe();
        }
        this.optimisticStream = optimisticStream;
    }

    /**
     * Get the version of this object. This corresponds to the position
     * of the pointer into the SMR stream.
     *
     * @return Returns the pointer position to the object in the stream.
     */
    public long getVersionUnsafe() {
        return smrStream.pos();
    }

    /**
     * Check whether this object is currently under optimistic modifications.
     */
    public boolean isOptimisticallyModifiedUnsafe() {
        return optimisticStream != null && optimisticStream.pos() != Address.NEVER_READ;
    }

    /**
     * Reset this object to the uninitialized state.
     */
    public void resetUnsafe() {
        log.debug("Reset[{}]", this);
        object = newObjectFn.get();
        smrStream.reset();
        optimisticStream = null;
    }

    /**
     * Get the ID of the stream backing this object.
     *
     * @return The ID of the stream backing this object.
     */
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
    public UUID getID() {
        return smrStream.getID();
    }

    /**
     * Generate the summary string for this version locked object.
     *
     * <p>The format of this string is [type]@[version][+]
     * (where + is the optimistic flag)
     *
     * @return The summary string for this version locked object
     */
    @Override
    public String toString() {
        WriteSetSMRStream optimisticStream = this.optimisticStream;

        return object.getClass().getSimpleName()
                + "[" + Utils.toReadableId(smrStream.getID()) + "]@"
                + (getVersionUnsafe() == Address.NEVER_READ ? "NR" : getVersionUnsafe())
                + (optimisticStream == null ? "" : "+" + optimisticStream.pos());
    }


    /**
     * Given a SMR entry with an undo record, undo the update.
     *
     * @param record The record to undo.
     */
    protected void applyUndoRecordUnsafe(SMREntry record) {
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
        } else if (resetSet.contains(record.getSMRMethod())) {
            // If this is a reset, undo by restoring the
            // previous state.
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


    /**
     * Apply an SMR update to the object, possibly optimistically.
     *
     * @param entry The entry to apply.
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
        // -OR- there this is an optimistic entry, calculate
        // an undo record.
        // (in the case of optimistic entries, the snapshot
        // may have changed since the last time they were
        // applied, so we need to recalculate undo) --- this
        // is the case without snapshot isolation
        if (!entry.isUndoable() || entry.getEntry() == null) {
            // Can we generate an undo record?
            IUndoRecordFunction<T> undoRecordTarget =
                    undoRecordFunctionMap
                            .get(entry.getSMRMethod());
            // If there was no previously calculated undo entry
            if (undoRecordTarget != null) {
                // calculate the undo record
                entry.setUndoRecord(undoRecordTarget
                        .getUndoRecord(object, entry.getSMRArguments()));
                log.trace("Apply[{}] Undo->{}", this, entry.getUndoRecord());
            } else if (resetSet.contains(entry.getSMRMethod())) {
                // This entry actually resets the object. So here
                // we can safely get a new instance, and add the
                // previous instance to the undo log.
                entry.setUndoRecord(object);
                object = newObjectFn.get();
                log.trace("Apply[{}] Undo->RESET", this);
            }
        }

        // now invoke the upcall
        Object ret = target.upcall(object, entry.getSMRArguments());
        return ret;
    }

    /**
     * Roll back the given stream by applying undo records in reverse order
     * from the current stream position until rollbackVersion.
     *
     * @param stream          The stream of SMR updates to apply in
     *                        reverse order.
     * @param rollbackVersion The version to stop roll back at.
     * @throws NoRollbackException If an entry in the stream did not contain
     *                             undo information.
     */
    protected void rollbackStreamUnsafe(ISMRStream stream, long rollbackVersion) {
        // If we're already at or before the given version, there's
        // nothing to do
        if (stream.pos() <= rollbackVersion) {
            return;
        }

        List<SMREntry> entries = stream.current();

        while (entries != null) {
            if (entries.stream().allMatch(x -> x.isUndoable())) {
                // start from the end, process one at a time
                ListIterator<SMREntry> it =
                        entries.listIterator(entries.size());
                while (it.hasPrevious()) {
                    applyUndoRecordUnsafe(it.previous());
                }
            } else {
                Optional<SMREntry> entry = entries.stream().findFirst();
                throw new NoRollbackException(entry, stream.pos(), rollbackVersion);
            }

            entries = stream.previous();

            if (stream.pos() <= rollbackVersion) {

                return;
            }
        }

        throw new NoRollbackException(stream.pos(), rollbackVersion);
    }

    /**
     * Sync this stream by playing updates forward in the stream until
     * the given timestamp. If Address.MAX is given, updates will be
     * applied until the current tail of the stream. If Address.OPTIMISTIC
     * is given, updates will be applied to the end of the stream, and
     * upcall results will be stored in the resulting entries.
     *
     * <p>When the stream is trimmed, this exception is passed up to the caller,
     * unless the timestamp was Address.MAX, in which the entire object is
     * reset and re-try the sync, which should pick up any checkpoint that
     * was inserted.
     *
     * @param stream    The stream to sync forward
     * @param timestamp The timestamp to sync up to.
     */
    protected void  syncStreamUnsafe(ISMRStream stream, long timestamp) {
        log.trace("Sync[{}] {}", this, (timestamp == Address.OPTIMISTIC)
                ? "Optimistic" : "to " + timestamp);
        long syncTo = (timestamp == Address.OPTIMISTIC) ? Address.MAX : timestamp;

        stream.streamUpTo(syncTo)
                .forEachOrdered(entry -> {
                    try {
                        Object res = applyUpdateUnsafe(entry);
                        if (timestamp == Address.OPTIMISTIC) {
                            entry.setUpcallResult(res);
                        } else if (pendingUpcalls.contains(entry.getEntry().getGlobalAddress())) {
                            log.debug("Sync[{}] Upcall Result {}",
                                    this, entry.getEntry().getGlobalAddress());
                            upcallResults.put(entry.getEntry().getGlobalAddress(), res == null
                                    ? NullValue.NULL_VALUE : res);
                            pendingUpcalls.remove(entry.getEntry().getGlobalAddress());
                        }
                        entry.setUpcallResult(res);
                    } catch (Exception e) {
                        log.error("Sync[{}] Error: Couldn't execute upcall due to {}", this, e);
                        throw new UnrecoverableCorfuError(e);
                    }
                });
    }

    /**
     * Roll back the optimistic stream, resetting the object if it can not
     * be restored.
     */
    protected void optimisticRollbackUnsafe() {
        try {
            log.trace("OptimisticRollback[{}] started", this);
            rollbackStreamUnsafe(this.optimisticStream,
                    Address.NEVER_READ);
            log.trace("OptimisticRollback[{}] complete", this);
        } catch (NoRollbackException nre) {
            log.warn("OptimisticRollback[{}] failed", this);
            resetUnsafe();
        }
    }

    /** Apply an SMREntry to the version object, while
     * doing bookkeeping for the underlying stream.
     *
     * @param entry
     */
    public void applyUpdateToStreamUnsafe(SMREntry entry, long globalAddress) {
        applyUpdateUnsafe(entry);
        seek(globalAddress);
    }
}
