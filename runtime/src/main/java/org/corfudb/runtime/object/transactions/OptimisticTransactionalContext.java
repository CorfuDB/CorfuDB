package org.corfudb.runtime.object.transactions;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.exceptions.NoRollbackException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuSMRAccess;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;
import org.corfudb.runtime.object.VersionLockedObject;

import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.corfudb.runtime.view.ObjectsView.TRANSACTION_STREAM_ID;

/** A Corfu optimistic transaction context.
 *
 * Optimistic transactions in Corfu provide the following isolation guarantees:
 *
 * (1) Read-your-own Writes:
 *  Reads in a transaction are guaranteed to observe a write in the same
 *  transaction, if a write happens before
 *      the read.
 *
 * (2) Opacity:
 *  Read in a transaction observe the state of the system ("snapshot") as of the time of the
 *      first read which occurs in the transaction ("first read
 *      timestamp"), except in case (1) above where they observe the own tranasction's writes.
 *
 * (3) Atomicity:
 *  Writes in a transaction are guaranteed to commit atomically,
 *     and commit if and only if none of the objects which were
 *     read (the "read set") were modified between the first read
 *     ("first read timestamp") and the time of commit.
 *
 * Created by mwei on 4/4/16.
 */
@Slf4j
public class OptimisticTransactionalContext extends AbstractTransactionalContext {

    /** The proxies which were modified by this transaction. */
    @Getter
    private final Set<ICorfuSMRProxyInternal> modifiedProxies =
            new HashSet<>();

    /** micro-transaction lock.
     * used in order to allow tranasctions a certain uniterrupted period of
     * time to commit
     */
    @Getter
    private final Lock mTxLock = new ReentrantLock();


    OptimisticTransactionalContext(TransactionBuilder builder) {
        super(builder);
    }


    /**
     * Sync the state of the proxy to the snapshot time,
     * or to the latest update by this transaction.
     *
     * @param proxy             The proxy which we are playing forward.
     * @param <T>               The type of the proxy's underlying object.
     */
    private <T> void syncUnsafe(ICorfuSMRProxyInternal<T> proxy) {

        // Commonly called fields, for readability.
        final VersionLockedObject<T> object = proxy.getUnderlyingObject();
        final UUID streamID = proxy.getStreamID();

        try {
            OptimisticTransactionalContext oCtxt = (OptimisticTransactionalContext)
                    object.getModifyingContextUnsafe();

            // If we don't own this object, roll it back
            // to avoid interrupting short-lived micro TX, grab the lock
            if (oCtxt != null && oCtxt != this) {
                /**
                 *  comment this block out to increase concurrency, but also,
                 *  potential contention
                 *  */
                try {
                    oCtxt.getMTxLock().tryLock(
                            TransactionalContext.mTxDuration.toMillis(), TimeUnit.MILLISECONDS);
                } catch (InterruptedException ie) {
                    // it's ok, just means that we move on without the lock
                    log.debug("tx at {} proceeds without lock");
                }
                /**
                 *  to here */
                object.optimisticRollbackUnsafe();
            }

            // If the version of this object is ahead of what we expected,
            // we need to rollback...
            if (object.getVersionUnsafe() > getSnapshotTimestamp()) {
                // We don't yet support version rollback, but we would
                // perform that here when we do.
                throw new NoRollbackException();
            }
        } catch (NoRollbackException nre) {
            // Couldn't roll back the object, so we'll have
            // to start from scratch.
            // TODO: create a copy instead
            proxy.resetObjectUnsafe(object);
        }

        // next, if the version is older than what we need
        // sync.
        if (object.getVersionUnsafe() < getSnapshotTimestamp()) {
            proxy.syncObjectUnsafe(proxy.getUnderlyingObject(), getSnapshotTimestamp());
        }

        // Take ownership of the object.
        object.setTXContextUnsafe(this);

        // Collect all the optimistic updates for this object, in order
        // which they need to be applied.
        List<WriteSetEntry> allUpdates = new LinkedList<>();

        Iterator<AbstractTransactionalContext> contextIterator =
            TransactionalContext.getTransactionStack().descendingIterator();

        contextIterator.forEachRemaining(x -> { allUpdates.addAll(x.getWriteSetEntryList(streamID)); });

        // Record that we have modified this proxy.
        if (allUpdates.size() > 0) {
            modifiedProxies.add(proxy);
        }

        // Apply all the updates from the optimistic version to the end
        // of the list of all updates for this thread.
        IntStream.range(object.getOptimisticVersionUnsafe(), allUpdates.size())
                .mapToObj(allUpdates::get)
                .forEachOrdered(wrapper -> {
                    SMREntry entry  = wrapper.getEntry();
                    Object res = proxy.getUnderlyingObject()
                            .applyUpdateUnsafe(entry, true);
                    wrapper.setUpcallResult(res);
                });
    }

    /** {@inheritDoc}
     */
    @Override
    public <R, T> R access(ICorfuSMRProxyInternal<T> proxy,
                           ICorfuSMRAccess<R, T> accessFunction,
                           Object[] conflictObject) {

        // First, we add this access to the read set
        addToReadSet(proxy, conflictObject);

        // Next, we check if the write set has any
        // outstanding modifications.
        try {
            return proxy.getUnderlyingObject().optimisticallyReadAndRetry((v, o) -> {
                // to ensure snapshot isolation, we should only read from
                // the first read timestamp.
                if (v == getSnapshotTimestamp() && objectIsOptimisticallyUpToDateUnsafe(proxy))
                {
                    return accessFunction.access(o);
                }
                throw new ConcurrentModificationException();
            });
        } catch (ConcurrentModificationException cme) {
            // It turned out version was wrong, so we're going to have to do
            // some work.
        }

        // Now we're going to do some work to modify the object, so take the
        // write
        // lock.
        return proxy.getUnderlyingObject().write((v, o) -> {
            syncUnsafe(proxy);
            // We might have ended up with a _different_ object
            return accessFunction.access(proxy.getUnderlyingObject()
                    .getObjectUnsafe());
        });
    }

    /** {@inheritDoc}
     */
    @Override
    public <T> Object getUpcallResult(ICorfuSMRProxyInternal<T> proxy,
                                      long timestamp, Object[] conflictObject) {
        // Getting an upcall result adds the object to the conflict set.
        addToReadSet(proxy, conflictObject);

        // if we have a result, return it.
        WriteSetEntry wrapper = getWriteSetEntryList(proxy.getStreamID()).get((int)timestamp);
        if (wrapper != null && wrapper.isHaveUpcallResult()){
            return wrapper.getUpcallResult();
        }
        // Otherwise, we need to sync the object
        return proxy.getUnderlyingObject().write((v,o) -> {
            syncUnsafe(proxy);
            WriteSetEntry wrapper2 = getWriteSetEntryList(proxy.getStreamID()).get((int)timestamp);
            if (wrapper2 != null && wrapper2.isHaveUpcallResult()){
                return wrapper2.getUpcallResult();
            }
            // If we still don't have the upcall, this must be a bug.
            throw new RuntimeException("Tried to get upcall during a transaction but" +
            " we don't have it even after an optimistic sync (asked for " + timestamp +
            " we have 0-" + (writeSet.get(proxy.getStreamID()).getValue().size() - 1) + ")");
        });
    }

    /** Logs an update. In the case of an optimistic transaction, this update
     * is logged to the write set for the transaction.
     * @param proxy         The proxy making the request.
     * @param updateEntry   The timestamp of the request.
     * @param <T>           The type of the proxy.
     * @return              The "address" that the update was written to.
     */
    @Override
    public <T> long logUpdate(ICorfuSMRProxyInternal<T> proxy,
                              SMREntry updateEntry,
                              Object[] conflictObjects) {

        // Insert the modification into writeSet.
        addToWriteSet(proxy, updateEntry, conflictObjects);

        // Return the "address" of the update; used for retrieving results from operations via getUpcallRestult.
        return writeSet.get(proxy.getStreamID()).getValue().size() - 1;
    }

    /**
     * Commit a transaction into this transaction by merging the read/write
     * sets.
     *
     * @param tc The transaction to merge.
     */
    @SuppressWarnings("unchecked")
    public void addTransaction(AbstractTransactionalContext tc) {
        // merge the conflict maps
        mergeReadSetInto(tc.getReadSet());

        // merge the write-sets
        mergeWriteSetInto(tc.writeSet);

        // "commit" the optimistic writes (for each proxy we touched)
        // by updating the modifying context (as long as the context
        // is still the same).
        updateAllProxies(x ->
                x.getUnderlyingObject()
                    .setTXContextUnsafe(this));
    }

    /** Commit the transaction. If it is the last transaction in the stack,
     * append it to the log, otherwise merge it into a nested transaction.
     *
     * @return The address of the committed transaction.
     * @throws TransactionAbortedException  If the transaction was aborted.
     */
    @Override
    @SuppressWarnings("unchecked")
    public long commitTransaction() throws TransactionAbortedException {
        long ret = commitTransactionNoReleaseLock();

        // now unlock this transaction's contention lock
        try {
            getMTxLock().unlock();
        } catch (IllegalMonitorStateException me) {
            log.error("transaction fails to unlock its own contention lock");
        }

        return ret;
    }

    long commitTransactionNoReleaseLock() throws TransactionAbortedException {
        log.debug("attempt to commit optimistic tx from snapshot {}", getSnapshotTimestamp());


        // If the transaction is nested, fold the transaction.
        if (TransactionalContext.isInNestedTransaction()) {
            getParentContext().addTransaction(this);
            commitAddress = AbstractTransactionalContext.FOLDED_ADDRESS;
            return commitAddress;
        }

        // If the write set is empty, we're done and just return
        // NOWRITE_ADDRESS.
        if (writeSet.isEmpty()) {
            return NOWRITE_ADDRESS;
        }

        //TODO(Maithem): Since the actualy stream write doesn't happen in the parent class,
        // we end up duplicating the same code for transaction logging for each type of transaction.
        // This is superfluous, find a better way to factor this piece of code.

        // Write to the transaction stream if transaction logging is enabled
        Set<UUID> affectedStreams = new HashSet<>(writeSet.keySet());
        if (this.builder.runtime.getObjectsView().isTransactionLogging()) {
            affectedStreams.add(TRANSACTION_STREAM_ID);
        }

        // Now we obtain a conditional address from the sequencer.
        // This step currently happens all at once, and we get an
        // address of -1L if it is rejected.
        long address = this.builder.runtime.getStreamsView()
                .acquireAndWrite(

                        // a set of stream-IDs that contains the affected streams
                        affectedStreams,

                        // a MultiObjectSMREntry that contains the update(s) to objects
                        collectWriteSetEntries(),

                        // nothing to do after successful acquisition and after deacquisition
                        t->true, t->true,

                        // TxResolution info:
                        // 1. snapshot timestamp
                        // 2. a map of conflict params, arranged by streamID's
                        // 3. a map of write conflict-params, arranged by
                        // streamID's
                        new TxResolutionInfo(getSnapshotTimestamp(), getReadSet(), collectWriteConflictParams())
                );

        if (address == -1L) {
            log.debug("Transaction aborted due to sequencer rejecting request");
            abortTransaction();
            throw new TransactionAbortedException();
        }

        super.commitTransaction();
        commitAddress = address;

        // Update all proxies, committing the new address.
        updateAllProxies(x ->
                x.getUnderlyingObject()
                        .optimisticCommitUnsafe(commitAddress));

        return address;
    }

    @SuppressWarnings("unchecked")
    protected void updateAllProxies(Consumer<ICorfuSMRProxyInternal> function) {
        getModifiedProxies().forEach(x -> {
            // If we are on the same thread, this will hold true.
            if (x.getUnderlyingObject().getModifyingContextUnsafe()
                    == this) {
                x.getUnderlyingObject().writeReturnVoid((v,o) -> {
                    // Make sure we're still the modifying thread
                    // even after getting the lock.
                    if (x.getUnderlyingObject().getModifyingContextUnsafe()
                            == this) {
                        function.accept(x);
                    }
                });
            }
        });
    }

    /** Determine whether a proxy's object is optimistically "up to date".
     *
     * An object is optimistically up to date if
     *
     * (1) There are no writes in any transaction's write set and there
     * are no optimistic writes on the object.
     *
     * (2) There are optimistic writes on the object and the number of
     * optimistic writes is equal to the number of writes in the transactions
     * write set, and the optimistic writes were OUR modifications.
     *
     * @param proxy     The proxy containing the object to check.
     * @param <T>       The underlying type of the object for the proxy.
     * @return          True, if the object is optimistically up to date.
     *                  False otherwise.
     */
    private <T> boolean objectIsOptimisticallyUpToDateUnsafe(ICorfuSMRProxyInternal<T> proxy) {
        long numOptimisticModifications = TransactionalContext.getTransactionStack()
                .stream().flatMap(x -> x.getWriteSetEntryList(proxy.getStreamID()).stream()).count();

        return (numOptimisticModifications == 0 &&
                !proxy.getUnderlyingObject().isOptimisticallyModifiedUnsafe()) ||
                (proxy.getUnderlyingObject().getModifyingContextUnsafe() == this &&
                        proxy.getUnderlyingObject().getOptimisticVersionUnsafe() == numOptimisticModifications);
    }



    /** Get the root context (the first context of a nested txn)
     * which must be an optimistic transactional context.
     * @return  The root context.
     */
    private OptimisticTransactionalContext getRootContext() {
        AbstractTransactionalContext atc = TransactionalContext.getRootContext();
        if (!(atc instanceof OptimisticTransactionalContext)) {
            throw new RuntimeException("Attempted to nest two different transactional context types");
        }
        return (OptimisticTransactionalContext)atc;
    }

    /**
     * Get the first timestamp for this transaction.
     *
     * @return The first timestamp to be used for this transaction.
     */
    @Override
    public synchronized long obtainSnapshotTimestamp() {
        /** obtain micro-transaction lock */
        /* */
        try {
            getMTxLock().tryLock(
                    TransactionalContext.mTxDuration.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            // it's ok, just means that we move on without the lock
            log.debug("tx at {} proceeds without lock");
        }
        /* */


        final AbstractTransactionalContext atc = getRootContext();
        if (atc != null && atc != this) {
            // If we're in a nested transaction, the first read timestamp
            // needs to come from the root.
            return atc.getSnapshotTimestamp();
        } else {
            // Otherwise, fetch a read token from the sequencer the linearize
            // ourselves against.
            long currentTail = builder.runtime
                    .getSequencerView().nextToken(Collections.emptySet(), 0).getToken();
            log.trace("Set first read timestamp for tx {} to {}", transactionID, currentTail);
            return currentTail;
        }
    }
}
