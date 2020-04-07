package org.corfudb.runtime.object.transactions;

import static org.corfudb.runtime.view.ObjectsView.TRANSACTION_STREAM_ID;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.AppendException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRAccess;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;

/** A Corfu optimistic transaction context.
 *
 * <p>Optimistic transactions in Corfu provide the following isolation guarantees:
 *
 * <p>(1) Read-your-own Writes:
 *  Reads in a transaction are guaranteed to observe a write in the same
 *  transaction, if a write happens before
 *      the read.
 *
 * <p>(2) Opacity:
 *  Read in a transaction observe the state of the system ("snapshot") as of the time of the
 *      first read which occurs in the transaction ("first read
 *      timestamp"), except in case (1) above where they observe the own transaction's writes.
 *
 * <p>(3) Atomicity:
 *  Writes in a transaction are guaranteed to commit atomically,
 *     and commit if and only if none of the objects which were
 *     read (the "read set") were modified between the first read
 *     ("first read timestamp") and the time of commit.
 *
 * <p>Created by mwei on 4/4/16.
 */
@Slf4j
public class OptimisticTransactionalContext extends AbstractTransactionalContext {

    /** The proxies which were modified by this transaction. */
    @Getter
    private final Set<ICorfuSMRProxyInternal> modifiedProxies =
            new HashSet<>();


    OptimisticTransactionalContext(Transaction transaction) {
        super(transaction);
    }

    /**
     * Access within optimistic transactional context is implemented
     * in via proxy.access() as follows:
     *
     * <p>1. First, we try to grab a read-lock on the proxy, and hope to "catch" the proxy in the
     * snapshot version. If this succeeds, we invoke the corfu-object access method, and
     * un-grab the read-lock.
     *
     * <p>2. Otherwise, we grab a write-lock on the proxy and bring it to the correct
     * version
     * - Inside proxy.setAsOptimisticStream, if there are currently optimistic
     * updates on the proxy, we roll them back.  Then, we set this
     * transactional context as the proxy's new optimistic context.
     * - Then, inside proxy.syncObjectUnsafe, depending on the proxy version,
     * we may need to undo or redo committed changes, or apply forward committed changes.
     *
     * {@inheritDoc}
     */
    @Override
    public <R, T extends ICorfuSMR<T>> R access(ICorfuSMRProxyInternal<T> proxy,
                                                ICorfuSMRAccess<R, T> accessFunction,
                                                Object[] conflictObject) {
        log.debug("Access[{},{}] conflictObj={}", this, proxy, conflictObject);
        // First, we add this access to the read set
        addToReadSet(proxy, conflictObject);

        // Next, we sync the object, which will bring the object
        // to the correct version, reflecting any optimistic
        // updates.
        // Get snapshot timestamp in advance so it is not performed under the VLO lock
        long ts = getSnapshotTimestamp().getSequence();
        return proxy
                .getUnderlyingObject()
                .access(o -> {
                            WriteSetSMRStream stream = o.getOptimisticStreamUnsafe();

                            // Obtain the stream position as when transaction context last
                            // remembered it.
                            long streamReadPosition = getKnownStreamPosition()
                                    .getOrDefault(proxy.getStreamID(), ts);

                            return (
                                    (stream == null || stream.isStreamCurrentContextThreadCurrentContext())
                                    && (stream != null && getWriteSetEntrySize(proxy.getStreamID()) == stream.pos() + 1
                                       || (getWriteSetEntrySize(proxy.getStreamID()) == 0 /* No updates. */
                                          && o.getVersionUnsafe() == streamReadPosition) /* Match timestamp. */
                                    )
                            );
                        },
                        o -> {
                            // inside syncObjectUnsafe, depending on the object
                            // version, we may need to undo or redo
                            // committed changes, or apply forward committed changes.
                            syncWithRetryUnsafe(o, getSnapshotTimestamp(), proxy,
                                    o::setUncommittedChanges);

                            // Update the global positions map. The value obtained from underlying
                            // object must be under object's write-lock.
                            getKnownStreamPosition().put(proxy.getStreamID(), o.getVersionUnsafe());
                        },
                        accessFunction::access
        );
    }

    /**
     * if a Corfu object's method is an Accessor-Mutator, then although the mutation is delayed,
     * it needs to obtain the result by invoking getUpcallResult() on the optimistic stream.
     *
     * <p>This is similar to the second stage of access(), accept working
     * on the optimistic stream instead of the
     * underlying stream.- grabs the write-lock on the proxy.
     * - uses proxy.setAsOptimisticStream in order to set itself as the proxy optimistic context,
     *   including rolling-back current optimistic changes, if any.
     * - uses proxy.syncObjectUnsafe to bring the proxy to the desired version,
     *   which includes applying optimistic updates of the current
     *  transactional context.
     *
     * {@inheritDoc}
     */
    @Override
    public <T extends ICorfuSMR<T>> Object getUpcallResult(ICorfuSMRProxyInternal<T> proxy,
                                                            long timestamp, Object[] conflictObject) {
        // Getting an upcall result adds the object to the conflict set.
        addToReadSet(proxy, conflictObject);

        // if we have a result, return it.
        SMREntry wrapper = getWriteSetEntryList(proxy.getStreamID()).get((int)timestamp);
        if (wrapper != null && wrapper.isHaveUpcallResult()) {
            return wrapper.getUpcallResult();
        }
        // Otherwise, we need to sync the object
        // Get snapshot timestamp in advance so it is not performed under the VLO lock
        Token ts = getSnapshotTimestamp();
        return proxy.getUnderlyingObject().update(o -> {
            log.trace("Upcall[{}] {} Sync'd", this,  timestamp);
            syncWithRetryUnsafe(o, ts, proxy, o::setUncommittedChanges);
            SMREntry wrapper2 = getWriteSetEntryList(proxy.getStreamID()).get((int)timestamp);
            if (wrapper2 != null && wrapper2.isHaveUpcallResult()) {
                return wrapper2.getUpcallResult();
            }
            // If we still don't have the upcall, this must be a bug.
            throw new RuntimeException("Tried to get upcall during a transaction but"
                    + " we don't have it even after an optimistic sync (asked for " + timestamp
                    + " we have 0-" + (getWriteSetEntryList(proxy.getStreamID()).size() - 1) + ")");
        });
    }

    /** Logs an update. In the case of an optimistic transaction, this update
     * is logged to the write set for the transaction.
     *
     * <p>Return the "address" of the update; used for retrieving results
     * from operations via getUpcallResult.
     *
     * @param proxy         The proxy making the request.
     * @param updateEntry   The timestamp of the request.
     * @param <T>           The type of the proxy.
     * @return              The "address" that the update was written to.
     */
    @Override
    public <T extends ICorfuSMR<T>>long logUpdate(ICorfuSMRProxyInternal<T> proxy,
                                                   SMREntry updateEntry,
                                                   Object[] conflictObjects) {
        log.trace("LogUpdate[{},{}] {} ({}) conflictObj={}",
                this, proxy, updateEntry.getSMRMethod(),
                updateEntry.getSMRArguments(), conflictObjects);

        return addToWriteSet(proxy, updateEntry, conflictObjects);
    }

    /**
     * Commit a transaction into this transaction by merging the read/write
     * sets.
     *
     * @param tc The transaction to merge.
     */
    @SuppressWarnings("unchecked")
    public void addTransaction(AbstractTransactionalContext tc) {
        log.trace("Merge[{}] adding {}", this, tc);
        // merge the conflict maps
        mergeReadSetInto(tc.getReadSetInfo());

        // merge the write-sets
        mergeWriteSetInto(tc.getWriteSetInfo());

        // "commit" the optimistic writes (for each proxy we touched)
        // by updating the modifying context (as long as the context
        // is still the same).
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
        log.debug("TX[{}] request optimistic commit", this);

        return getConflictSetAndCommit(getReadSetInfo());
    }

    /**
     * Commit with a given conflict set and return the address.
     *
     * @param conflictSet  conflict set used to check whether transaction can commit
     * @return  the commit address
     */
    public long getConflictSetAndCommit(ConflictSetInfo conflictSet) {

        if (TransactionalContext.isInNestedTransaction()) {
            getParentContext().addTransaction(this);
            commitAddress = AbstractTransactionalContext.FOLDED_ADDRESS;
            log.trace("Commit[{}] Folded into {}", this, getParentContext());
            return commitAddress;
        }

        // If the write set is empty, we're done and just return
        // NOWRITE_ADDRESS.
        if (getWriteSetInfo().getWriteSet().getEntryMap().isEmpty()) {
            log.trace("Commit[{}] Read-only commit (no write)", this);
            return NOWRITE_ADDRESS;
        }

        // Write to the transaction stream if transaction logging is enabled
        Set<UUID> affectedStreamsIds = new HashSet<>(getWriteSetInfo().getWriteSet().getEntryMap().keySet());

        if (this.transaction.isLoggingEnabled()) {
            affectedStreamsIds.add(TRANSACTION_STREAM_ID);
        }

        UUID[] affectedStreams = affectedStreamsIds.toArray(new UUID[affectedStreamsIds.size()]);

        // Now we obtain a conditional address from the sequencer.
        // This step currently happens all at once, and we get an
        // address of -1L if it is rejected.
        long address = -1L;
        final TxResolutionInfo txInfo =
            // TxResolution info:
            // 1. snapshot timestamp
            // 2. a map of conflict params, arranged by streamID's
            // 3. a map of write conflict-params, arranged by
            // streamID's
            new TxResolutionInfo(getTransactionID(),
                getSnapshotTimestamp(),
                conflictSet.getHashedConflictSet(),
                getWriteSetInfo().getHashedConflictSet());

        try {
            address = this.transaction.runtime.getStreamsView()
                .append(
                    // a MultiObjectSMREntry that contains the update(s) to objects
                    collectWriteSetEntries(),
                    txInfo,
                    // a set of stream-IDs that contains the affected streams
                    affectedStreams
                );
        } catch (AppendException oe) {
            // We were overwritten (and the original snapshot is now conflicting),
            // which means we must abort.
            throw new TransactionAbortedException(txInfo, AbortCause.OVERWRITE, oe, this);
        }

        log.trace("Commit[{}] Acquire address {}", this, address);

        super.commitTransaction();
        commitAddress = address;

        log.trace("Commit[{}] Written to {}", this, address);
        return address;
    }

    @Override
    public void addPreCommitListener(TransactionalContext.PreCommitListener preCommitListener) {
        this.getPreCommitListeners().add(preCommitListener);
    }
}
