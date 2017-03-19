package org.corfudb.runtime.object.transactions;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.ISMRConsumable;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.exceptions.NoRollbackException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuSMRAccess;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;
import org.corfudb.runtime.object.VersionLockedObject;
import org.corfudb.runtime.view.Address;

import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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


    OptimisticTransactionalContext(TransactionBuilder builder) {
        super(builder);
    }

    /**
     * Access within optimistic transactional context is implemented
     * in via proxy.access() as follows:
     *
     * 1. First, we try to grab a read-lock on the proxy, and hope to "catch" the proxy in the
     * snapshot version. If this succeeds, we invoke the corfu-object access method, and
     * un-grab the read-lock.
     *
     * 2. Otherwise, we grab a write-lock on the proxy and bring it to the correct
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
    public <R, T> R access(ICorfuSMRProxyInternal<T> proxy,
                           ICorfuSMRAccess<R, T> accessFunction,
                           Object[] conflictObject) {
        // First, we add this access to the read set
        addToReadSet(proxy, conflictObject);

        // Next, we sync the object, which will bring the object
        // to the correct version, reflecting any optimistic
        // updates.
        return proxy.getUnderlyingObject().access(
                o -> (writeSet.get(proxy.getStreamID()) == null && // No updates
                        o.getVersionUnsafe() == getSnapshotTimestamp() && // And at the correct timestamp
                        !o.isOptimisticallyModifiedUnsafe()),
                o -> {
                    // Swap ourselves to be the active optimistic stream.
                    // Inside setAsOptimisticStream, if there are
                    // currently optimistic updates on the object, we
                    // roll them back.  Then, we set this context as  the
                    // object's new optimistic context.
                    setAsOptimisticStream(o);

                    // inside syncObjectUnsafe, depending on the object
                    // version, we may need to undo or redo
                    // committed changes, or apply forward committed changes.
                    o.syncObjectUnsafe(getSnapshotTimestamp());
                },
                o -> accessFunction.access(o)
        );
    }

    /**
     * if a Corfu object's method is an Accessor-Mutator, then although the mutation is delayed,
     * it needs to obtain the result by invoking getUpcallResult() on the optimistic stream.
     *
     * This is similar to the second stage of access(), accept working on the optimistic stream instead of the
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
    public <T> Object getUpcallResult(ICorfuSMRProxyInternal<T> proxy,
                                      long timestamp, Object[] conflictObject) {
        // Getting an upcall result adds the object to the conflict set.
        addToReadSet(proxy, conflictObject);

        // if we have a result, return it.
        SMREntry wrapper = getWriteSetEntryList(proxy.getStreamID()).get((int)timestamp);
        if (wrapper != null && wrapper.isHaveUpcallResult()){
            return wrapper.getUpcallResult();
        }
        // Otherwise, we need to sync the object
        return proxy.getUnderlyingObject().update(o -> {
            setAsOptimisticStream(o);
            log.trace("Upcall[{}] {} Sync'd", this,  timestamp);
            o.syncObjectUnsafe(getSnapshotTimestamp());
            SMREntry wrapper2 = getWriteSetEntryList(proxy.getStreamID()).get((int)timestamp);
            if (wrapper2 != null && wrapper2.isHaveUpcallResult()){
                return wrapper2.getUpcallResult();
            }
            // If we still don't have the upcall, this must be a bug.
            throw new RuntimeException("Tried to get upcall during a transaction but" +
                    " we don't have it even after an optimistic sync (asked for " + timestamp +
                    " we have 0-" + (writeSet.get(proxy.getStreamID()).getValue().size() - 1) + ")");
        });
    }

    <T> void setAsOptimisticStream(VersionLockedObject<T> object) {
        if (object.getOptimisticStreamUnsafe() == null ||
                !object.getOptimisticStreamUnsafe()
                        .isStreamForThisTransaction()) {
            object.setOptimisticStreamUnsafe(
                            new WriteSetSMRStream(
                                    TransactionalContext.getTransactionStackAsList(),
                                    object.getID()));
        }
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
        log.trace("LogUpdate[{}] {} ({}) conflictObj={}",
                this, updateEntry.getSMRMethod(),
                updateEntry.getSMRArguments(), conflictObjects);

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
        log.trace("Merge[{}] adding {}", this, tc);
        // merge the conflict maps
        mergeReadSetInto(tc.getReadSet());

        // merge the write-sets
        mergeWriteSetInto(tc.writeSet);

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
        if (TransactionalContext.isInNestedTransaction()) {
            getParentContext().addTransaction(this);
            commitAddress = AbstractTransactionalContext.FOLDED_ADDRESS;
            log.trace("Commit[{}] Folded into {}", this, getParentContext());
            return commitAddress;
        }

        // If the write set is empty, we're done and just return
        // NOWRITE_ADDRESS.
        if (writeSet.isEmpty()) {
            log.trace("Commit[{}] Read-only commit (no write)", this);
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
        log.trace("Commit[{}] Acquire address {}", this, address);

        if (address == -1L) {
            log.trace("Commit[{}] Rejected by sequencer", this);
            abortTransaction();
            throw new TransactionAbortedException();
        }

        super.commitTransaction();
        commitAddress = address;

        tryCommitAllProxies();
        log.trace("Commit[{}] Written to {}", this, address);
        return address;
    }

    /** Try to commit the optimistic updates to each proxy. */
    protected void tryCommitAllProxies() {
        // First, get the committed entry
        // in order to get the backpointers
        // and the underlying SMREntries.
        ILogData committedEntry = this.builder.getRuntime()
                .getAddressSpaceView().read(commitAddress);

        updateAllProxies(x -> {
            log.trace("Commit[{}] Committing {}", this,  x);
            // Commit all the optimistic updates
            x.getUnderlyingObject().optimisticCommitUnsafe();
            // If some other client updated this object, sync
            // it forward to grab those updates
            x.getUnderlyingObject().syncObjectUnsafe(
                        commitAddress-1);
            // Also, be nice and transfer the undo
            // log from the optimistic updates
            // for this to work the write sets better
            // be the same
            List<SMREntry> committedWrites =
                    getWriteSetEntryList(x.getStreamID());
            List<SMREntry> entryWrites =
                    ((ISMRConsumable) committedEntry
                            .getPayload(this.getBuilder().runtime))
                    .getSMRUpdates(x.getStreamID());
            if (committedWrites.size() ==
                    entryWrites.size()) {
                IntStream.range(0, committedWrites.size())
                        .forEach(i -> {
                            if (committedWrites.get(i)
                                    .isUndoable()) {
                                entryWrites.get(i)
                                        .setUndoRecord(committedWrites.get(i)
                                                .getUndoRecord());
                            }
                        });
            }
            // and move the stream pointer to "skip" this commit entry
            x.getUnderlyingObject().seek(commitAddress + 1);
            log.trace("Commit[{}] Committed {}", this,  x);
        });

    }

    @SuppressWarnings("unchecked")
    protected void updateAllProxies(Consumer<ICorfuSMRProxyInternal> function) {
        getModifiedProxies().forEach(x -> {
            // If we are on the same thread, this will hold true.
            if (x.getUnderlyingObject()
                    .optimisticallyOwnedByThreadUnsafe()) {
                x.getUnderlyingObject().update(o -> {
                    // Make sure we're still the modifying thread
                    // even after getting the lock.
                    if (x.getUnderlyingObject()
                            .optimisticallyOwnedByThreadUnsafe()) {
                        function.accept(x);
                    }
                    return null;
                });
            }
        });
    }

    /** Get the root context (the first context of a nested txn)
     * which must be an optimistic transactional context.
     * @return  The root context.
     */
    private OptimisticTransactionalContext getRootContext() {
        AbstractTransactionalContext atc = TransactionalContext.getRootContext();
        if (atc != null && !(atc instanceof OptimisticTransactionalContext)) {
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
            log.trace("SnapshotTimestamp[{}] {}", this, currentTail);
            return currentTail;
        }
    }
}
