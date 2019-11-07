package org.corfudb.runtime.object;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.logprotocol.SMRRecord;
import org.corfudb.protocols.logprotocol.SMRRecordLocator;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.GarbageInformer;
import org.corfudb.runtime.exceptions.NoRollbackException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.object.transactions.WriteSetSMRStream;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

//TODO Discard TransactionStream for building maps but not for constructing tails

/**
 * The VersionLockedObject maintains a versioned object which is backed by an ISMRStream, and is
 * optionally backed by an additional optimistic update stream.
 *
 * <p>Users of the VersionLockedObject cannot access the versioned object directly, rather they
 * use the access() and update() methods to read and manipulate the object.
 *
 * <p>access() and update() allow the user to provide functions to execute under locks. These
 * functions execute various "unsafe" methods provided by this object which inspect and
 * manipulate the object state.
 *
 * <p>syncObjectUnsafe() enables the user to bring the object to a given version, and the
 * VersionLockedObject manages any sync or rollback of updates necessary.
 *
 * <p>Created by mwei on 11/13/16.
 */
@Slf4j
public class VersionLockedObject<T extends ICorfuSMR<T>> {
    /**
     * The actual underlying object.
     */
    @Getter
    private T object;

    /**
     * A list of upcalls pending in the system. The proxy keeps this set so it can remember to
     * save the upcalls for pending requests.
     */
    @Getter
    private final Set<Long> pendingUpcalls;

    // This enum is necessary because null cannot be inserted
    // into a ConcurrentHashMap.
    enum NullValue {
        NULL_VALUE
    }

    /**
     * A list of upcall results, keyed by the address they were requested.
     */
    @Getter
    private final Map<Long, Object> upcallResults;


    /**
     * A lock, which controls access to modifications to the object. Any access to unsafe
     * methods should obtain the lock.
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
     * The garbage identification function map for this object.
     */
    private final Map<String, IGarbageIdentificationFunction> garbageIdentifyFunctionMap;

    /**
     * The garbage identification decisions are exposed outside runtime via garbageInformer.
     */
    private final GarbageInformer garbageInformer;

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
     * The largest address that the stream is synced to.
     */
    private long syncTail = Address.NON_ADDRESS;

    /**
     * A flag indicates that the object is just reset and during its syncing the locator information should be
     * updated but not be added to garbage informer.
     */
    private boolean resetSyncMode = false;

    /**
     * Corfu runtime
     */
    private final CorfuRuntime rt;

    /**
     * The VersionLockedObject maintains a versioned object which is backed by an ISMRStream,
     * and is optionally backed by an additional optimistic update stream.
     *
     * @param newObjectFn       A function passed to instantiate a new instance of this object.
     * @param smrStream         Stream View backing this object.
     */
    public VersionLockedObject(Supplier<T> newObjectFn,
                               StreamViewSMRAdapter smrStream,
                               ICorfuSMR<T> wrapperObject,
                               GarbageInformer garbageInformer,
                               CorfuRuntime rt) {
        this.smrStream = smrStream;

        this.upcallTargetMap = wrapperObject.getCorfuSMRUpcallMap();
        this.undoRecordFunctionMap = wrapperObject.getCorfuUndoRecordMap();
        this.undoFunctionMap = wrapperObject.getCorfuUndoMap();
        this.resetSet = wrapperObject.getCorfuResetSet();
        this.garbageIdentifyFunctionMap = wrapperObject.getCorfuGarbageIdentificationMap();
        this.garbageInformer = garbageInformer;

        wrapperObject.closeWrapper();
        this.newObjectFn = newObjectFn;
        this.object = newObjectFn.get();
        this.pendingUpcalls = ConcurrentHashMap.newKeySet();
        this.upcallResults = new ConcurrentHashMap<>();
        this.rt = rt;

        lock = new StampedLock();
    }

    /**
     * Execute a method on the version locked object without synchronization
     */
    public <R> R passThrough(Function<T, R> method) {
        return method.apply(object);
    }

    /**
     * Run gc on this object. Since the stream that backs this object is not thread-safe:
     * synchronization between gc and external object access is needed.
     */
    public void gc(long trimMark) {
        long ts = 0;

        try (Timer.Context vloGcDuration = VloMetricsHelper.getVloGcContext()) {
            ts = lock.writeLock();

            // pengdingUpcalls and upcallResults before compaction mark will no longer
            // used by the client
            pendingUpcalls.removeIf(e -> e < trimMark);
            upcallResults.entrySet().removeIf(e -> e.getKey() < trimMark);

            smrStream.gc(trimMark);
        } finally {
            lock.unlock(ts);
        }
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
            try (Timer.Context optimistReadDuration = VloMetricsHelper.getOptimisticReadContext()) {
                if (directAccessCheckFunction.apply(this)) {
                    log.trace("Access [{}] Direct (optimistic-read) access at {}",
                            this, getVersionUnsafe());
                    R ret = accessFunction.apply(object.getContext(ICorfuExecutionContext.DEFAULT));

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
                log.warn("Access [{}] Direct (optimistic-read) exception, upgrading lock. Exception ",
                        this, e);
            }
        }
        // Next, we just upgrade to a full write lock if the optimistic
        // read fails, since it means that the state of the object was
        // updated.
        try (Timer.Context updateObjectReadDuration = VloMetricsHelper.getUpdatedObjectReadContext()) {
            // Attempt an upgrade
            ts = lock.tryConvertToWriteLock(ts);
            // Upgrade failed, try conversion again
            if (ts == 0) {
                ts = lock.writeLock();
            }
            // Check if direct access is possible (unlikely).
            if (directAccessCheckFunction.apply(this)) {
                log.trace("Access [{}] Direct (writelock) access at {}", this, getVersionUnsafe());
                R ret = accessFunction.apply(object.getContext(ICorfuExecutionContext.DEFAULT));

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
            return accessFunction.apply(object.getContext(ICorfuExecutionContext.DEFAULT));
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

        try (Timer.Context updateDuration = VloMetricsHelper.getVloUpdateContext()) {
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
     * @param timestamp The version to rollback to.
     * @throws NoRollbackException If the object cannot be rolled back to
     *                             the supplied version.
     */
    private void rollbackObjectUnsafe(long timestamp) {

        if (object.getVersionPolicy() == ICorfuVersionPolicy.MONOTONIC) {
            return; // We are not allowed to go back in time.
        }

        if (getVersionUnsafe() <= timestamp) {
            return; // We are already behind the timestamp.
        }

        log.trace("Rollback[{}] to {}", this, timestamp);


        // An TrimmedException is thrown for some reasons:
        //
        // 1. Addresses before compaction mark may have been trim to prevent memory leak.
        //    In this case, rolling back is limit by compaction mark to insure correctness.
        //
        // 2. Let us assume we do not trim resolved addresses in the stream layer.
        // Aggressively throwing trimmedException is still beneficial to some extent.
        // Rolling back is implemented by applying undo record one by one. In the middle
        // of rolling back, if the client needs to read data from the log unit if the
        // data is not presented in the cache, the client will throw trimmedException
        // which makes the previous effort in vain.
        long compactionMark = rt.getAddressSpaceView().getCompactionMark().get();
        if (Address.isAddress(timestamp) && compactionMark > timestamp) {
            log.trace("Rollback[{}] aborted", this);
            throw new TrimmedException(String.format("snapshot: %d, compaction mark: %d",
                    timestamp, compactionMark));
        }

        try {
            rollbackStreamUnsafe(smrStream, timestamp);
            log.trace("Rollback[{}] completed", this);
        } catch (NoRollbackException ex) {
            log.warn("Rollback[{}] failed", this);
            resetUnsafe();
        }
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
     * @see VersionLockedObject#syncObjectUnsafeInner
     */
    public void syncObjectUnsafe(long timestamp) {
        try (Timer.Context updateDuration = VloMetricsHelper.getVloSyncContext()) {
            syncObjectUnsafeInner(timestamp);
        }
    }

    /**
     * Bring the object to the requested version, rolling back or syncing
     * the object from the log if necessary to reach the requested version.
     *
     * @param timestamp The timestamp to update the object to.
     */
    private void syncObjectUnsafeInner(long timestamp) {
        // If there is an optimistic stream attached,
        // and it belongs to this thread use that
        if (optimisticallyOwnedByThreadUnsafe()) {
            // If there are no updates, ensure we are at the right snapshot
            if (optimisticStream.pos() == Address.NEVER_READ) {
                final WriteSetSMRStream currentOptimisticStream = optimisticStream;
                // If we are too far ahead, roll back to the past
                rollbackObjectUnsafe(timestamp);

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
                rollbackUncommittedChangesUnsafe();
                this.optimisticStream = null;
            }
            // If we are too far ahead, roll back to the past
            rollbackObjectUnsafe(timestamp);
            syncStreamUnsafe(smrStream, timestamp);
        }
    }

    /** Set the correct optimistic stream for this transaction (if not already).
     *
     * If the Optimistic stream doesn't reflect the current transaction context,
     * we create the correct WriteSetSMRStream and pick the latest context as the
     * current context.
     */
    public void setUncommittedChanges() {
        WriteSetSMRStream stream = getOptimisticStreamUnsafe();
        if (stream == null
                || !stream.isStreamCurrentContextThreadCurrentContext()) {

            // We are setting the current context to the root context of nested transactions.
            // Upon sync forward
            // the stream will replay every entries from all parent transactional context.
            WriteSetSMRStream newSmrStream =
                    new WriteSetSMRStream(TransactionalContext.getTransactionStackAsList(), getID());

            setUncommittedChanges(newSmrStream);
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
    public long logUpdate(SMRRecord entry, boolean saveUpcall) {
        return smrStream.append(entry,
                t -> {
                    if (saveUpcall) {
                        pendingUpcalls.add(t.getToken().getSequence());
                    }
                    return true;
                },
                t -> {
                    if (saveUpcall) {
                        pendingUpcalls.remove(t.getToken().getSequence());
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
     * Check whether or not this object was modified by this thread.
     *
     * @return True, if the object was modified by this thread. False otherwise.
     */
    private boolean optimisticallyOwnedByThreadUnsafe() {
        return optimisticStream != null && optimisticStream.isStreamForThisThread();
    }

    /**
     * Set the optimistic stream for this thread, rolling back
     * any previous threads if they were present.
     *
     * @param optimisticStream The new optimistic stream to install.
     */
    public void setUncommittedChanges(WriteSetSMRStream optimisticStream) {
        if (this.optimisticStream != null) {
            rollbackUncommittedChangesUnsafe();
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
        object.close();
        object = newObjectFn.get();
        smrStream.reset();
        optimisticStream = null;
        resetSyncMode = true;
    }

    /**
     * Get the ID of the stream backing this object.
     *
     * @return The ID of the stream backing this object.
     */
    @SuppressWarnings("checkstyle:abbreviation")
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
     * Given a stream, return the context under which the underlying
     * object should be updated.
     *
     * @param stream stream on which we are currently operating
     * @return an appropriate context
     */
    private ICorfuExecutionContext.Context getContext(ISMRStream stream) {
        if (stream == optimisticStream) {
            return ICorfuExecutionContext.OPTIMISTIC;
        }

        return ICorfuExecutionContext.DEFAULT;
    }

    /**
     * Given a SMR entry with an undo entry, undo the update.
     *
     * @param record The record to undo.
     */
    private void applyUndoRecordUnsafe(SMRRecord record, ISMRStream stream) {
        log.trace("Undo[{}] of {}@{} ({})", this, record.getSMRMethod(),
                Address.isAddress(record.getGlobalAddress()) ? record.getGlobalAddress() : "OPT",
                record.getUndoRecord());
        IUndoFunction<T> undoFunction = undoFunctionMap.get(record.getSMRMethod());
        ICorfuExecutionContext.Context context = getContext(stream);

        // If the undo function exists, apply it.
        if (undoFunction != null) {
            undoFunction.doUndo(
                    object.getContext(context),
                    record.getUndoRecord(), record.getSMRArguments());
            return;
        } else if (resetSet.contains(record.getSMRMethod())) {
            // If this is a reset, undo by restoring the previous state.
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
     * Given a timestamp, return the context under which the underlying
     * object should be updated.
     *
     * @param timestamp timestamp on which we are currently operating
     * @return an appropriate context
     */
    private ICorfuExecutionContext.Context getContext(long timestamp) {
        if (timestamp == Address.OPTIMISTIC) {
            return ICorfuExecutionContext.OPTIMISTIC;
        }

        return ICorfuExecutionContext.DEFAULT;
    }

    /**
     * Apply an SMR update to the object, possibly optimistically.
     *
     * @param record The SMR record to apply.
     */
    private Object applyUpdateUnsafe(SMRRecord record, long timestamp) {
        log.trace("Apply[{}] of {}@{} ({})", this, record.getSMRMethod(),
                Address.isAddress(record.getGlobalAddress()) ? record.getGlobalAddress() : "OPT",
                record.getSMRArguments());

        ICorfuSMRUpcallTarget<T> target = upcallTargetMap.get(record.getSMRMethod());
        if (target == null) {
            throw new RuntimeException("Unknown upcall " + record.getSMRMethod());
        }


        ICorfuExecutionContext.Context context = getContext(timestamp);
        // calculate an undo record if no undo record is present and its
        // global address is smaller than compaction mark -OR- there
        // is an optimistic record, (which has no valid global address).
        // In the case of optimistic entries, the snapshot may have changed
        // since the last time they were applied, so we need to recalculate
        // undo -- this is the case without snapshot isolation.
        if (!Address.isAddress(record.getGlobalAddress()) ||
                !record.isUndoable() &&
                        record.getGlobalAddress() >= rt.getAddressSpaceView().getCompactionMark().get()) {
            // Can we generate an undo record?
            IUndoRecordFunction<T> undoRecordTarget =
                    undoRecordFunctionMap.get(record.getSMRMethod());
            // If there was no previously calculated undo record
            if (undoRecordTarget != null) {
                // Calculate the undo record.
                record.setUndoRecord(undoRecordTarget
                        .getUndoRecord(object.getContext(context), record.getSMRArguments()));
                log.trace("Apply[{}] Undo->{}", this, record.getUndoRecord());
            } else if (resetSet.contains(record.getSMRMethod())) {
                // This record actually resets the object. So here
                // we can safely get a new instance, and add the
                // previous instance to the undo log.
                record.setUndoRecord(object);
                object.close();
                object = newObjectFn.get();
                log.trace("Apply[{}] Undo->RESET", this);
            }
        }

        // Now invoke the upcall
        return target.upcall(object.getContext(context), record.getSMRArguments());
    }

    /**
     * Roll back the given stream by applying undo records in reverse order
     * from the current stream position until rollbackVersion.
     *
     * @param stream          The stream of SMR updates to apply in
     *                        reverse order.
     * @param rollbackVersion The version to stop roll back at.
     * @throws NoRollbackException If an record in the stream did not contain
     *                             undo information.
     */
    private void rollbackStreamUnsafe(ISMRStream stream, long rollbackVersion) {
        // If we're already at or before the given version, there's
        // nothing to do
        if (stream.pos() <= rollbackVersion) {
            return;
        }

        List<SMRRecord> entries = stream.current();

        while (!entries.isEmpty()) {
            if (entries.stream().allMatch(SMRRecord::isUndoable)) {
                // start from the end, process one at a time
                ListIterator<SMRRecord> it = entries.listIterator(entries.size());
                while (it.hasPrevious()) {
                    applyUndoRecordUnsafe(it.previous(), stream);
                }
            } else {
                Optional<SMRRecord> entry = entries.stream().findFirst();
                if (log.isTraceEnabled()) {
                    log.trace("rollbackStreamUnsafe: one or more stream entries in address @{} are not undoable. " +
                                    "Undoable entries: {}/{}", stream.pos(),
                            (int) entries.stream().filter(SMRRecord::isUndoable).count(),
                            entries.size());
                }
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
    private void syncStreamUnsafe(ISMRStream stream, long timestamp) {
        log.trace("Sync[{}] {}", this, (timestamp == Address.OPTIMISTIC)
                ? "Optimistic" : "to " + timestamp);
        long syncTo = (timestamp == Address.OPTIMISTIC) ? Address.MAX : timestamp;

        Stream<SMRRecord> smrRecordStream = stream.streamUpTo(syncTo);
        applyUpdateStreamUnsafe(smrRecordStream, timestamp);
    }

    private void applyUpdateStreamUnsafe(Stream<SMRRecord> smrRecordStream, long timestamp) {
        // AtomicLong is used as a container to bypass the restriction of lambda expression
        final AtomicLong lastSyncAddress = new AtomicLong(Address.NON_ADDRESS);
        boolean isOptimistic = timestamp == Address.OPTIMISTIC;
        List<SMRRecordLocator> garbageLocators = new ArrayList<>();


        smrRecordStream
                .forEachOrdered(record -> {
                    try {
                        Object res = applyUpdateUnsafe(record, timestamp);
                        if (isOptimistic) {
                            record.setUpcallResult(res);
                        } else { // materialized log
                            if (pendingUpcalls.contains(record.getGlobalAddress())) {
                                log.debug("Sync[{}] Upcall Result {}",
                                        this, record.getGlobalAddress());
                                upcallResults.put(record.getGlobalAddress(), res == null
                                        ? NullValue.NULL_VALUE : res);
                                pendingUpcalls.remove(record.getGlobalAddress());
                            }

                            // Record with global address larger than lastSyncAddress indicates that all SMRRecords
                            // from last global address has been applied. Therefore, syncTail could be updated.
                            if (lastSyncAddress.get() != Address.NON_ADDRESS &&
                                    record.getGlobalAddress() > lastSyncAddress.get()) {
                                syncTail = Math.max(syncTail, lastSyncAddress.get());

                                // flush garbage if there is garbage
                                if (!garbageLocators.isEmpty()) {
                                    // vlo prevent concurrent access
                                    // to garbageReceivingQueue to maintain order by marker address.
                                    garbageInformer.addUnsafe(syncTail, garbageLocators);
                                    garbageLocators.clear();
                                }
                            }

                            // to check whether the SMRRecord is first encountered by VLO
                            if (record.getGlobalAddress() > syncTail || resetSyncMode) {
                                IGarbageIdentificationFunction garbageIdentifyFunction =
                                        garbageIdentifyFunctionMap.get(record.getSMRMethod());
                                if (garbageIdentifyFunction != null) {
                                    List<Object> garbage =
                                            garbageIdentifyFunction.identify(record.locator, record.getSMRArguments());

                                    // not adds garbage decisions if at resetSync mode because the garbage decisions
                                    // may be wrong.
                                    if (record.getGlobalAddress() > syncTail) {
                                        // reset the flag after reaching syncTail
                                        resetSyncMode = false;
                                        garbage.forEach(o ->
                                                garbageLocators.add((SMRRecordLocator) o));
                                    }
                                }

                                lastSyncAddress.set(record.getGlobalAddress());
                            }
                        }

                        record.setUpcallResult(res);
                    } catch (Exception e) {
                        log.error("Sync[{}] Error: Couldn't execute upcall due to {}", this, e);
                        throw new UnrecoverableCorfuError(e);
                    }
                });


        // update syncTail after applying all SMRRecords. 
        if (!isOptimistic) {
            syncTail = Math.max(syncTail, lastSyncAddress.get());

            // flush garbage if there is garbage
            if (!garbageLocators.isEmpty()) {
                // vlo prevent concurrent access
                // to garbageReceivingQueue to maintain order by marker address.
                garbageInformer.addUnsafe(syncTail, garbageLocators);
            }
        }
    }

    /**
     * Roll back the optimistic stream, resetting the object if it can not
     * be restored.
     */
    protected void rollbackUncommittedChangesUnsafe() {
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

    /** Apply an SMRRecord to the version object, while
     * doing bookkeeping for the underlying stream.
     *
     * @param updates smr records
     */
    public void applyUpdatesToStreamUnsafe(List<SMRRecord> updates, long globalAddress) {
        applyUpdateStreamUnsafe(updates.stream(), globalAddress);
        seek(globalAddress + 1);
    }

    /**
     * This class includes the metrics registry and the timer names used within VersionLockedObject
     * methods
     */
    private static class VloMetricsHelper {
        private static final MetricRegistry metrics = CorfuRuntime.getDefaultMetrics();
        private static final String VLO_OPTIMISTIC_READ = CorfuComponent.OBJECT.toString() +
                "vlo.optimistic-read";
        private static final String VLO_UPDATED_OBJECT_READ = CorfuComponent.OBJECT.toString() +
                "vlo.updated-object-read";
        private static final String VLO_UPDATE = CorfuComponent.OBJECT.toString() + "vlo.update";
        private static final String VLO_SYNC = CorfuComponent.OBJECT.toString() + "vlo.sync";
        private static final String VLO_GC = CorfuComponent.OBJECT.toString() + "vlo.gc";

        private static Timer.Context getVloSyncContext() {
            return MetricsUtils.getConditionalContext(metrics.timer(VLO_SYNC));
        }

        private static Timer.Context getOptimisticReadContext() {
            return MetricsUtils.getConditionalContext(metrics.timer(VLO_OPTIMISTIC_READ));
        }

        private  static Timer.Context getUpdatedObjectReadContext() {
            return MetricsUtils.getConditionalContext(metrics.timer(VLO_UPDATED_OBJECT_READ));
        }

        private  static Timer.Context getVloUpdateContext() {
            return MetricsUtils.getConditionalContext(metrics.timer(VLO_UPDATE));
        }

        private  static Timer.Context getVloGcContext() {
            return MetricsUtils.getConditionalContext(metrics.timer(VLO_GC));
        }
    }
}
