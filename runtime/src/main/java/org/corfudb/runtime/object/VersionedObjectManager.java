package org.corfudb.runtime.object;

import static java.lang.Math.min;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.NoRollbackException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.TrimmedUpcallException;
import org.corfudb.runtime.exceptions.UnrecoverableCorfuException;
import org.corfudb.runtime.object.transactions.AbstractTransaction;
import org.corfudb.runtime.object.transactions.Transactions;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectBuilder;
import org.corfudb.util.Utils;

/**
 * The VersionedObjectManager maintains a versioned state-machine object {@code object}.
 * This object can time-travel and be optimistically updated.
 *
 * <p>To access object state, users call {@link #access(IStateMachineAccess, Object[])} with
 * a function that is executed when the object is "sync'd". The definition of sync is context
 * dependent. For example, if a transaction is active, it will depend on the type of transaction.
 * Typically, the VersionedObjectManager is instantiated with a
 * {@link LinearizableStateMachineStream}, which means that an access will reflect any updates
 * added via the {@link #logUpdate(String, boolean, Object[], Object...)} operation ordered before
 * it (in wall-clock time).
 *
 * <p>To modify an object, users call {@link #logUpdate(String, boolean, Object[], Object...)} with
 * a proposed state machine update. This function returns an address, which can be used in a
 * later call to {@link #getUpcallResult(long, Object[])}. This is used if the update returns a
 * value.
 *
 */
@Slf4j
public class VersionedObjectManager<T> implements IObjectManager<T> {

    /**
     * The actual underlying object.
     */
    T object;

    /**
     * A lock, which controls access to modifications to
     * the object. Any access to unsafe methods should
     * obtain the lock.
     */
    private final StampedLock lock = new StampedLock();

    /**
     * The stream view this object is backed by.
     */
    private IStateMachineStream smrStream;

    /**
     * The wrapper for this object.
     */
    @Getter
    private final ICorfuWrapper<T> wrapper;

    /** The builder for the object.
     *
     */
    @Getter
    private final ObjectBuilder<T> builder;

    /** Pending requests, which are used to "jump" linearized reads forward whenever possible. */
    private final LongAccumulator pendingRequest =
            new LongAccumulator(Math::max, Address.NEVER_READ);

    /**
     * Construct a new versioned object manager with the given stream, wrapper and builder.
     */
    public VersionedObjectManager(@Nonnull IStateMachineStream smrStream,
                                  @Nonnull ICorfuWrapper<T> wrapper,
                                  @Nonnull ObjectBuilder<T> builder) {
        this.builder = builder;
        this.smrStream = smrStream;
        this.wrapper = wrapper;
        this.object = builder.getRawInstance();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long logUpdate(@Nonnull String smrUpdateFunction,
                          boolean keepUpcallResult,
                          @Nullable Object[] conflictObject,
                          Object... args) {
        final IStateMachineStream stream = getActiveStream();
        long address = stream.append(smrUpdateFunction, args, conflictObject, keepUpcallResult);
        // Update the pending request queue, in case another thread is syncing.
        pendingRequest.accumulate(address);
        return address;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public <R> R getUpcallResult(long address, @Nullable Object[] conflictObject) {
        long ts = 0;
        try {
            IStateMachineStream stream = getActiveStream();
            IStateMachineOp entry = stream.consumeEntry(address);

            // If there is no upcall present, we must obtain it (via syncing).
            if (entry == null || !entry.isUpcallResultPresent()) {
                // If the object is write-locked, this means some other thread is syncing
                // the object forward. This might have our update, so we wait until the write
                // lock is removed.
                if (lock.isWriteLocked()) {
                    // We don't have a way to "park" or wait for the lock yet,
                    // so we will spin.
                    for (int i = 0; i < 1000000; i++) {
                        if (entry == null) {
                            entry = stream.consumeEntry(address);
                        }
                        if (entry != null && entry.isUpcallResultPresent()) {
                            return (R) entry.getUpcallResult();
                        }
                        if (!lock.isWriteLocked()) {
                            break;
                        }
                    }
                }
                // We need to sync the object forward in order to get the upcall result.
                try {
                    ts = lock.writeLock();
                    switchToActiveStreamUnsafe();
                    // Potentially pick up other updates.
                    syncObjectUnsafe(Address.MAX, conflictObject);
                    entry = stream.consumeEntry(address);
                } finally {
                    lock.unlock(ts);
                }
            }

            if (entry != null && entry.isUpcallResultPresent()) {
                return (R) entry.getUpcallResult();
            }
            throw new RuntimeException("Attempted to get the result "
                    + "of an upcall@" + address + " but we are @"
                    + getVersionUnsafe()
                    + " and we don't have a copy");
        } catch (TrimmedException te) {
            throw new TrimmedUpcallException(address);
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public <R> R access(@Nonnull IStateMachineAccess<R, T> accessFunction,
                           @Nullable Object[] conflictObject) {
        // Try an optimistic read first.
        // An optimistic read is possible if the correct stream is present
        // and that stream has no updates.
        long checkAddress = Address.MAX;
        long ts = 0;
        if (checkIsActiveStreamUnsafe()) {
            ts = lock.tryOptimisticRead();
            if (ts != 0) {
                checkAddress = smrStream.check();
                if (checkAddress == Address.UP_TO_DATE
                        || checkAddress <= smrStream.pos()) {
                    try {
                        // Up to date, don't need to sync
                        R ret = accessFunction.access(object);
                        if (lock.validate(ts)) {
                            return ret;
                        }
                    } catch (Exception ex) {
                        // An exceptional condition occurred during the optimistic read
                        // We catch the exception but don't throw it, since it may have
                        // been a result of inconsistent state.
                    }
                }

            }
        }
        final long syncAddress = checkAddress;
        if (syncAddress != Address.MAX) {
            pendingRequest.accumulate(syncAddress);
        }
        try {
            ts = lock.writeLock();
            switchToActiveStreamUnsafe();
            syncObjectUnsafe(syncAddress == Address.UP_TO_DATE ? Address.MAX :
                    Math.max(checkAddress, pendingRequest.get()), conflictObject);
            return accessFunction.access(object);
        } finally {
            lock.unlock(ts);
        }
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
     * Reset this object to the uninitialized state.
     */
    public void resetUnsafe() {
        log.debug("Reset[{}]", this);
        object = builder.getRawInstance();
        rollbackToParentUnsafe();
        smrStream.reset();
    }

    @Override
    public int hashCode() {
        return builder.getStreamId().hashCode();
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
        return object.getClass().getSimpleName()
                + "[" + Utils.toReadableId(smrStream.getId()) + "]@"
                + (getVersionUnsafe() == Address.NEVER_READ ? "NR" : getVersionUnsafe());
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
    * @param timestamp The timestamp to sync up to.
    */
    void syncObjectUnsafe(long timestamp, @Nullable Object[] conflictObjects) {
        log.trace("syncObjectUnsafe[{}] to {}", this, timestamp);
        for (int tries = 0; tries < builder.getRuntime().trimRetry; tries++) {
            try {
                smrStream.sync(timestamp, conflictObjects)
                        .forEachOrdered(op -> {
                            try {
                                object = op.apply(wrapper, object);
                            } catch (Exception e) {
                                log.error("syncObjectUnsafe[{}]: Couldn't execute upcall due to "
                                        + "{}", this, e);
                                throw new RuntimeException(e);
                            }
                        });
                return;
            } catch (NoRollbackException nre) {
                log.info("syncObjectUnsafe[{}]: Failed to rollback, resetting and retrying", this);
                resetUnsafe();
            } catch (TrimmedException te) {
                if (!te.isRetriable()) {
                    log.warn("syncObjectUnsafe[{}]: Trim is not retriable, aborting sync", this);
                    throw te;
                } else if (tries + 1 >= builder.getRuntime().trimRetry) {
                    if (Transactions.active()) {
                        Transactions.abort(new TransactionAbortedException(
                                new TxResolutionInfo(
                                       Transactions.current().getTransactionID(),
                                       Transactions.getReadSnapshot()), null, builder.getStreamId(),
                                AbortCause.TRIM, te, Transactions.current()));
                    } else {
                        throw te;
                    }
                }
                log.info("syncObjectUnsafe[{}]: Encountered trim during playback, "
                        + "resetting and retrying");
                resetUnsafe();
            }

            // We must restore the transactional stream if we were operating transactionally
            switchToActiveStreamUnsafe();
        }
        throw new UnrecoverableCorfuException("Unable to sync object and retry limit exhausted");
    }

    /** Install a new stream on this object manager.
     *
     * @param streamGenerator   A function used to generate a new stream. This function will
     *                          receive to current stream, and should return the new stream.
     * @return                  The stream which was installed.
     */
    @Nonnull
    public IStateMachineStream installNewStream(@Nonnull
                            Function<IStateMachineStream, IStateMachineStream> streamGenerator) {
        long ts = lock.writeLock();
        try {
            rollbackToParentUnsafe();
            smrStream = streamGenerator.apply(smrStream.getRoot());
            return smrStream;
        } finally {
            lock.unlock(ts);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getVersion() {
        return getVersionUnsafe();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UUID getId() {
        return builder.getStreamId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R> R txExecute(Supplier<R> txFunction) {
        // Don't nest transactions if we are already running transactionally
        if (Transactions.active()) {
            try {
                return txFunction.get();
            } catch (Exception e) {
                log.warn("TXExecute[{}] Abort with Exception: {}", this, e);
                abortTransaction(e);
            }
        }
        long sleepTime = 1L;
        final long maxSleepTime = 1000L;
        int retries = 1;
        while (true) {
            try {
                builder.getRuntime().getObjectsView().TXBegin();
                R ret = txFunction.get();
                builder.getRuntime().getObjectsView().TXEnd();
                return ret;
            } catch (TransactionAbortedException e) {
                // If TransactionAbortedException is due to a 'Network Exception' do not keep
                // retrying a nested transaction indefinitely (this could go on forever).
                // If this is part of an outer transaction abort and remove from context.
                // Re-throw exception to client.
                log.warn("TXExecute[{}] Abort with exception {}", this, e);
                if (e.getAbortCause() == AbortCause.NETWORK) {
                    if (Transactions.current() != null) {
                        try {
                            Transactions.abort();
                        } catch (TransactionAbortedException tae) {
                            // discard manual abort
                        }
                        throw e;
                    }
                }

                log.debug("Transactional function aborted due to {}, retrying after {} msec",
                        e, sleepTime);
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    log.warn("TxExecuteInner retry sleep interrupted {}", ie);
                }
                sleepTime = min(sleepTime * 2L, maxSleepTime);
                retries++;
            } catch (Exception e) {
                log.warn("TXExecute[{}] Abort with Exception: {}", this, e);
                abortTransaction(e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sync() {
        long ts = lock.writeLock();
        try {
            switchToActiveStreamUnsafe();
            syncObjectUnsafe(Address.MAX, null);
        } finally {
            lock.unlock(ts);
        }
    }

    /** Get the "active" stream, without switching the stream that is attached to the manager.
     *
     *  The active stream is the stream that we should use to append
     * or sync based on the context (whether a transaction is active or not.
     *
     * @return  The active stream.
     */
    private @Nonnull IStateMachineStream getActiveStream() {
        return Transactions.active()
                ? Transactions.getStateMachineStream(this, smrStream.getRoot())
                : smrStream.getRoot();
    }


    /** Switch the stream attached to the manager to the "active" (based on context) stream.
     *
     * Unsafe, so must be performed under a write lock.
     */
    private void switchToActiveStreamUnsafe() {
        if (Transactions.active()) {
            IStateMachineStream txStream = Transactions
                    .getStateMachineStream(this, smrStream);
            if (!txStream.equals(smrStream)) {
                rollbackToParentUnsafe();
                smrStream = txStream;
            }
        } else if (smrStream.getParent() != null) {
            rollbackToParentUnsafe();
        }
    }

    /** Check if the current stream is the "active" stream (based on context).
     *
     * Unsafe as it may return inconsistent state if not used under a read lock or
     * write lock.
     *
     * @return  True, if the current stream is the active stream. False otherwise.
     */
    private boolean checkIsActiveStreamUnsafe() {
        if (Transactions.active()) {
            return Transactions.getStateMachineStream(this, smrStream)
                    .equals(smrStream);
        } else {
            return smrStream.getParent() == null;
        }
    }


    /** Roll back all "optimistic" updates on the stream attached to this object,
     * detaching all child streams until the "root" stream is the only stream left.
     *
     * Unsafe, as it changes the state of the object and must be executed under
     * a write lock.
     */
    private void rollbackToParentUnsafe() {
        while (smrStream.getParent() != null) {
            syncObjectUnsafe(Address.OPTIMISTIC, null);
            smrStream = smrStream.getParent();
        }
    }


    private void abortTransaction(Exception e) {
        long snapshotTimestamp;
        AbortCause abortCause;
        TransactionAbortedException tae;

        AbstractTransaction context = Transactions.current();

        if (e instanceof TransactionAbortedException) {
            tae = (TransactionAbortedException) e;
        } else {
            if (e instanceof NetworkException) {
                // If a 'NetworkException' was received within a transactional context, an attempt
                // to 'getSnapshotTimestamp' will also fail (as it requests it to the Sequencer).
                // A new NetworkException would prevent the earliest to be propagated and
                // encapsulated as a TransactionAbortedException.
                snapshotTimestamp = -1L;
                abortCause = AbortCause.NETWORK;
            } else if (e instanceof UnsupportedOperationException) {
                snapshotTimestamp = Transactions.getReadSnapshot();
                abortCause = AbortCause.UNSUPPORTED;
            } else {
                log.error("abort[{}] Abort Transaction with Exception {}", this, e);
                snapshotTimestamp = Transactions.getReadSnapshot();
                abortCause = AbortCause.UNDEFINED;
            }

            TxResolutionInfo txInfo = new TxResolutionInfo(
                    context.getTransactionID(), snapshotTimestamp);
            // TODO: fix...
            tae = new TransactionAbortedException(txInfo, null, UUID.randomUUID(),
                    abortCause, e, context);
            context.abort(tae);
        }


        // Discard the transaction chain, if present.
        try {
            Transactions.abort();
        } catch (TransactionAbortedException e2) {
            // discard manual abort
        }

        throw tae;
    }

}
