package org.corfudb.runtime.object.transactions;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.AppendException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ConsistencyView;
import org.corfudb.runtime.object.ICorfuSMRAccess;
import org.corfudb.runtime.object.MVOCorfuCompileProxy;
import org.corfudb.runtime.object.SnapshotGenerator;
import org.corfudb.runtime.object.SnapshotProxy;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.runtime.CorfuOptions.ConsistencyModel.READ_COMMITTED;


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
    public <R, S extends SnapshotGenerator<S> & ConsistencyView> R access(
            MVOCorfuCompileProxy<S> proxy, ICorfuSMRAccess<R, S> accessFunction, Object[] conflictObject) {
        long startAccessTime = System.nanoTime();
        try {
            log.trace("Access[{},{}] conflictObj={}", this, proxy, conflictObject);

            // First, we add this access to the read set
            addToReadSet(proxy, conflictObject);

            // Get snapshot timestamp in advance, so it is not performed under the VLO lock
            long ts = getSnapshotTimestamp().getSequence();
            SnapshotProxy<S> snapshotProxy = getAndCacheSnapshotProxy(proxy, ts);
            updateKnownStreamPosition(proxy, snapshotProxy.getSnapshotVersionSupplier().get());
            return snapshotProxy.access(accessFunction, conflictObject);
        } finally {
            dbNanoTime += (System.nanoTime() - startAccessTime);
        }
    }

    /** Logs an update. In the case of an optimistic transaction, this update
     * is logged to the write set for the transaction.
     *
     * <p>Return the "address" of the update; used for retrieving results
     * from operations via getUpcallResult.
     *
     * @param proxy         The proxy making the request.
     * @param updateEntry   The timestamp of the request.
     * @return              The "address" that the update was written to.
     */
    @Override
    public long logUpdate(
            MVOCorfuCompileProxy<?> proxy, SMREntry updateEntry, Object[] conflictObjects) {
        long startLogUpdateTime = System.nanoTime();

        try {
            log.trace("LogUpdate[{},{}] {} ({}) conflictObj={}",
                    this, proxy, updateEntry.getSMRMethod(),
                    updateEntry.getSMRArguments(), conflictObjects);

            getAndCacheSnapshotProxy(proxy, getSnapshotTimestamp().getSequence())
                    .logUpdate(updateEntry.getSMRMethod(), updateEntry.getSMRArguments());
            return addToWriteSet(proxy, updateEntry, conflictObjects);
        } finally {
            dbNanoTime += (System.nanoTime() - startLogUpdateTime);
        }
    }

    // TODO: below logUpdate methods.

    @Override
    public void logUpdate(UUID streamId, SMREntry updateEntry) {
        addToWriteSet(streamId, updateEntry);
    }

    @Override
    public void logUpdate(UUID streamId, SMREntry updateEntry, List<UUID> streamTags) {
        addToWriteSet(streamId, updateEntry, streamTags);
    }

    @Override
    public void logUpdate(UUID streamId, List<SMREntry> updateEntries) {
        addToWriteSet(streamId, updateEntries);
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
        log.trace("TX[{}] request optimistic commit", this);
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

        // If the write set is empty, this is a read-only transaction.
        // If the transaction has only read non-monotonic objects, then return the
        // max address of all accessed streams, this will provide a safe token of the
        // transaction's snapshot. Notice that, providing the snapshot of the tx (vs. max address)
        // can lead to data loss, as a sequencer reboot might incur in sequence regression.
        // This timestamp is aimed to provide clients with a secure point for delta/streaming
        // subscription, the later could lead to data loss scenarios.
        // If the transaction has read monotonic objects, we instead return the min address
        // of all accessed streams. Although this avoids data loss, clients subscribing at
        // this point for delta/streaming may observe duplicate data.
        if (getWriteSetInfo().getWriteSet().getEntryMap().isEmpty()) {
            log.trace("Commit[{}] Read-only commit (no write)", this);
            if (accessedReadCommittedObject) {
                return getMinAddressRead();
            } else {
                return getMaxAddressRead();
            }
        }

        Set<UUID> affectedStreamsIds = new HashSet<>(getWriteSetInfo()
                .getWriteSet().getEntryMap().keySet());

        // Write to streams corresponding to the streamTags
        affectedStreamsIds.addAll(getWriteSetInfo().getStreamTags());

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

        commitAddress = address;
        log.trace("Commit[{}] Written to {}", this, address);
        return address;
    }

    @Override
    public void addPreCommitListener(TransactionalContext.PreCommitListener preCommitListener) {
        this.getPreCommitListeners().add(preCommitListener);
    }
}
