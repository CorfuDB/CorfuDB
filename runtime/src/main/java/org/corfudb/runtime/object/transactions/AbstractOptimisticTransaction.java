package org.corfudb.runtime.object.transactions;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.logprotocol.ISMRConsumable;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.ICorfuSMRAccess;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;
import org.corfudb.runtime.object.ISMRStream;
import org.corfudb.runtime.object.StreamViewSMRAdapter;
import org.corfudb.runtime.object.VersionLockedObject;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.Utils;

import static org.corfudb.runtime.view.ObjectsView.TRANSACTION_STREAM_ID;

import javax.annotation.Nonnull;

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
 *      timestamp"), except in case (1) above where they observe the own tranasction's writes.
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
public abstract class AbstractOptimisticTransaction extends
        AbstractTransaction {

    AbstractOptimisticTransaction(TransactionBuilder builder) {
        super(builder);
    }

    protected <T> void addToReadSet(ICorfuSMRProxyInternal<T> proxy,
                                      Object[] conflictObject) { }

    protected <T> long addToWriteSet(ICorfuSMRProxyInternal<T> proxy,
                                       SMREntry updateEntry,
                                       Object[] conflictObject) {
        return Transactions.getContext().getWriteSet().add(proxy, updateEntry, conflictObject);
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
    public <R, T> R access(ICorfuSMRProxyInternal<T> proxy,
                           ICorfuSMRAccess<R, T> accessFunction,
                           Object[] conflictObject) {
        log.debug("Access[{},{}] conflictObj={}", this, proxy, conflictObject);
        addToReadSet(proxy, conflictObject);
        // Next, we sync the object, which will bring the object
        // to the correct version, reflecting any optimistic
        // updates.
        return proxy
                .getUnderlyingObject()
                .access(o -> o.optimisticallyOwnedByThreadUnsafe()
                                    && Transactions.getContext().getWriteSet()
                                    .getWriteSet().getSMRUpdates(proxy.getStreamID()).size()
                                == o.getOptimisticStreamUnsafe().pos(),
                        o -> {
                            // inside syncObjectUnsafe, depending on the object
                            // version, we may need to undo or redo
                            // committed changes, or apply forward committed changes.
                            syncWithRetryUnsafe(o, obtainSnapshotTimestamp(),
                                    proxy, this::setAsOptimisticStream);
                        },
                    o -> accessFunction.access(o)
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
    public <T> Object getUpcallResult(ICorfuSMRProxyInternal<T> proxy,
                                      long timestamp, Object[] conflictObject) {
        // Getting an upcall result adds the object to the conflict set.
        addToReadSet(proxy, conflictObject);

        // if we have a result, return it.
        SMREntry wrapper = Transactions.getContext().getWriteSet().getWriteSet()
                .getSMRUpdates(proxy.getStreamID()).get((int)timestamp);
        if (wrapper != null && wrapper.isHaveUpcallResult()) {
            return wrapper.getUpcallResult();
        }
        // Otherwise, we need to sync the object
        return proxy.getUnderlyingObject().update(o -> {
            log.trace("Upcall[{}] {} Sync'd", this,  timestamp);
            syncWithRetryUnsafe(o, obtainSnapshotTimestamp(), proxy, this::setAsOptimisticStream);
            SMREntry wrapper2 = Transactions.getContext().getWriteSet().getWriteSet()
                    .getSMRUpdates(proxy.getStreamID()).get((int)timestamp);
            if (wrapper2 != null && wrapper2.isHaveUpcallResult()) {
                return wrapper2.getUpcallResult();
            }
            // If we still don't have the upcall, this must be a bug.
            throw new RuntimeException("Tried to get upcall during a transaction but"
                    + " we don't have it even after an optimistic sync (asked for " + timestamp
                    + " we have " + (Transactions.getContext().getWriteSet().getWriteSet()
                    .getSMRUpdates(proxy.getStreamID()).size()) + ", stream is at "
                    + proxy.getUnderlyingObject().getOptimisticStreamUnsafe().pos() + ")");
        });
    }

    /** Set the correct optimistic stream for this transaction (if not already).
     *
     * If the Optimistic stream doesn't reflect the current transaction context,
     * we create the correct WriteSetSmrStream and pick the latest context as the
     * current context.
     * @param object        Underlying object under transaction
     * @param <T>           Type of the underlying object
     */
    <T> void setAsOptimisticStream(VersionLockedObject<T> object) {
        WriteSetSmrStream stream = object.getOptimisticStreamUnsafe();
        if (stream == null
                || !stream.isStreamForThisThread()) {

            // We are setting the current context to the root context of nested transactions.
            // Upon sync forward
            // the stream will replay every entries from all parent transactional context.
            WriteSetSmrStream newSmrStream =
                    new WriteSetSmrStream(Transactions.getContext().getWriteSet(), object.getID());

            object.setOptimisticStreamUnsafe(newSmrStream);
        }
    }

    /** Logs an update. In the case of an optimistic transaction, this update
     * is logged to the write set for the transaction.
     *
     * <p>Return the "address" of the update; used for retrieving results
     * from operations via getUpcallRestult.
     *
     * @param proxy         The proxy making the request.
     * @param updateEntry   The timestamp of the request.
     * @param <T>           The type of the proxy.
     * @return              The "address" that the update was written to.
     */
    @Override
    public <T> long logUpdate(ICorfuSMRProxyInternal<T> proxy,
                              SMREntry updateEntry,
                              Object[] conflictObjects) {
        log.trace("LogUpdate[{},{}] {} ({}) conflictObj={}",
                this, proxy, updateEntry.getSMRMethod(),
                updateEntry.getSMRArguments(), conflictObjects);
        return addToWriteSet(proxy, updateEntry, conflictObjects);
    }

    /** Commit the transaction. If it is the last transaction in the stack,
     * append it to the log, otherwise merge it into a nested transaction.
     *
     * @return The address of the committed transaction.
     * @throws TransactionAbortedException  If the transaction was aborted.
     */
    @Override
    @SuppressWarnings("unchecked")
    public long commit() throws TransactionAbortedException {
        if (Transactions.isNested()) {
            log.trace("commit[{}] Nested transaction folded", this);
            return AbstractTransaction.FOLDED_ADDRESS;
        }

        // If the write set is empty, we're done and just return
        // NOWRITE_ADDRESS.
        if (Transactions.getContext().getWriteSet()
                .getWriteSet().getEntryMap().isEmpty()) {
            log.trace("commit[{}] Read-only commit (no write)", this);
            return NOWRITE_ADDRESS;
        }

        // Write to the transaction stream if transaction logging is enabled
        Set<UUID> affectedStreams = Transactions.getContext().getWriteSet()
                .getWriteSet()
                .getEntryMap().keySet();

        if (this.builder.runtime.getObjectsView().isTransactionLogging()) {
            affectedStreams = new HashSet<>(affectedStreams);
            affectedStreams.add(TRANSACTION_STREAM_ID);
        }

        // Now we obtain a conditional address from the sequencer and append to the log
        // If rejected we will get a transaction aborted exception.
        long address;

        final Map<UUID, Set<byte[]>> hashedConflictSet =
                Transactions.getContext().getConflictSet().getHashedConflictSet();
        try {
            address = this.builder.runtime.getStreamsView()
                    .append(
                            affectedStreams,
                            Transactions.getContext().getWriteSet().getWriteSet(),
                            new TxResolutionInfo(getTransactionID(),
                                    obtainSnapshotTimestamp(),
                                    hashedConflictSet,
                                    Transactions.getContext().getWriteSet()
                                            .getHashedConflictSet())
                    );
        } catch (TransactionAbortedException tae) {
            // If precise conflicts aren't required, re-throw the transaction.
            if (!builder.isPreciseConflicts()) {
                throw tae;
            }

            // Otherwise, do a precise conflict check.
            address = preciseCommit(tae, Transactions.getContext().getConflictSet().getConflicts()
                    , hashedConflictSet, affectedStreams);
        }

        log.trace("commit[{}] Acquire address {}", this, address);

        super.commit();

        tryCommitAllProxies(address);
        log.trace("commit[{}] Written to {}", this, address);
        return address;
    }

    /** Do a precise commit, which is guaranteed not to produce an abort
     * due to a false conflict. This method achieves this guarantee by scanning
     * scanning the log when the sequencer detects a conflict, and manually
     * inspecting each entry in the conflict window. If there are no true conflicts,
     * the transaction is retried with the sequencer, otherwise, if a true conflict
     * is detected, the transaction aborts, with a flag indicating the the abort
     * was precise (guaranteed to not be false).
     *
     * @param originalException     The original exception when we first tried to commit.
     * @param conflictSet           The set of objects this transaction conflicts with.
     * @param hashedConflictSet     The hashed version of the conflict set.
     * @param affectedStreams       The set of streams affected by this transaction.
     * @return                      The address the transaction was committed to.
     * @throws TransactionAbortedException  If the transaction must be aborted.
     */
    protected long preciseCommit(@Nonnull final TransactionAbortedException originalException,
                                 @NonNull final Map<ICorfuSMRProxyInternal, Set<Object>>
                                           conflictSet,
                                 @NonNull final Map<UUID, Set<byte[]>> hashedConflictSet,
                                 @NonNull final Set<UUID> affectedStreams) {

        log.debug("preciseCommit[{}]: Imprecise conflict detected, resolving...", this);
        TransactionAbortedException currentException = originalException;

        // This map contains the maximum address of the streams we have
        // verified to not have any true conflicts so far.
        final Map<UUID, Long> verifiedStreams = new HashMap<>();

        // We resolve conflicts until we have a -true- conflict.
        // This might involve having the sequencer reject our
        // request to commit multiple times.
        while (currentException.getAbortCause() == AbortCause.CONFLICT) {
            final UUID conflictStream = currentException.getConflictStream();
            final long currentAddress = currentException.getConflictAddress();
            final TransactionAbortedException thisException = currentException;

            // Get the proxy, which should be available either in the write set
            // or read set. We need the proxy to generate the conflict objects
            // from the SMR entry.
            ICorfuSMRProxyInternal proxy;
            Optional<ICorfuSMRProxyInternal> modifyProxy =
                    Transactions.getContext().getWriteSet()
                    .getConflicts().keySet()
                    .stream()
                    .filter(p -> p.getStreamID().equals(conflictStream))
                    .findFirst();
            if (modifyProxy.isPresent()) {
                proxy = modifyProxy.get();
            } else {
                modifyProxy = Transactions.getContext().getConflictSet().getProxy(conflictStream);
                if (!modifyProxy.isPresent()) {
                    modifyProxy = Transactions.getContext().getWriteSet().getProxy(conflictStream);
                    if (!modifyProxy.isPresent()) {
                        log.warn("preciseCommit[{}]: precise conflict resolution requested "
                                + "but proxy not found, aborting", this);
                    }
                    throw currentException;
                }
                proxy = modifyProxy.get();
            }

            // Otherwise starting from the snapshot address to the conflict
            // address (following backpointers if possible), check if there
            // is any conflict
            log.debug("preciseCommit[{}]: conflictStream {} searching {} to {}",
                    this,
                    Utils.toReadableId(conflictStream),
                    obtainSnapshotTimestamp() + 1,
                    currentAddress);
            // Generate a view over the stream that caused the conflict
            IStreamView stream =
                    builder.runtime.getStreamsView().get(conflictStream);
            try {
                ISMRStream smrStream = new StreamViewSMRAdapter(builder.runtime, stream);
                // Start the stream right after the snapshot.
                smrStream.seek(obtainSnapshotTimestamp() + 1);
                // Iterate over the stream, manually checking each conflict object.
                smrStream.streamUpTo(currentAddress)
                        .forEach(x -> {
                            Object[] conflicts =
                                    proxy.getConflictFromEntry(x.getSMRMethod(),
                                            x.getSMRArguments());
                            log.trace("preciseCommit[{}]: found conflicts {}", this, conflicts);
                            if (conflicts != null) {
                                Optional<Set<Object>> conflictObjects =
                                        conflictSet.entrySet().stream()
                                            .filter(e -> e.getKey().getStreamID()
                                                    .equals(conflictStream))
                                            .map(Map.Entry::getValue)
                                            .findAny();
                                if (!Collections.disjoint(Arrays.asList(conflicts),
                                        conflictObjects.get())) {
                                    log.debug("preciseCommit[{}]: True conflict, aborting",
                                            this);
                                    thisException.setPrecise(true);
                                    throw thisException;
                                }
                            } else {
                                // Conflicts was null, which means the entry conflicted with -any-
                                // update (for example, a clear).
                                log.debug("preciseCommit[{}]: True conflict due to conflict all,"
                                                + " aborting",
                                        this);
                                thisException.setPrecise(true);
                                throw thisException;
                            }
                        });
            } catch (TrimmedException te) {
                // During the scan, it could be possible for us to encounter
                // a trim exception. In this case, the trim counts as a conflict
                // and we must abort.
                log.warn("preciseCommit[{}]: Aborting due to trim during scan");
                throw new TransactionAbortedException(currentException.getTxResolutionInfo(),
                        currentException.getConflictKey(), conflictStream,
                        AbortCause.TRIM, te, this);
            }

            // If we got here, we now tell the sequencer we checked this
            // object manually and it was a false conflict, and try to
            // commit.
            log.warn("preciseCommit[{}]: False conflict, stream {} checked from {} to {}",
                    this, Utils.toReadableId(conflictStream), obtainSnapshotTimestamp() + 1,
                    currentAddress);
            verifiedStreams.put(conflictStream, currentAddress);
            try {
                return this.builder.runtime.getStreamsView()
                        .append(
                                affectedStreams,
                                Transactions.getContext().getWriteSet().getWriteSet(),
                                new TxResolutionInfo(getTransactionID(),
                                        obtainSnapshotTimestamp(),
                                        hashedConflictSet,
                                        Transactions.getContext().getWriteSet()
                                        .getHashedConflictSet(),
                                        verifiedStreams
                                )
                        );
            } catch (TransactionAbortedException taeRetry) {
                // This means that the sequencer still rejected our request
                // Which may be because another client updated this
                // conflict key already. We'll try again if the
                // abort reason was due to a conflict.
                log.warn("preciseCommit[{}]: Sequencer rejected, retrying", this, taeRetry);
                currentException = taeRetry;
            }
        }
        // If the abort had no conflict key information, we have
        // no choice but to re-throw.
        throw currentException;
    }

    /** Try to commit the optimistic updates to each proxy. */
    protected void tryCommitAllProxies(long address) {
        // First, get the committed entry
        // in order to get the backpointers
        // and the underlying SMREntries.
        ILogData committedEntry = this.builder.getRuntime()
                .getAddressSpaceView().read(address);

        updateAllProxies(x -> {
            log.trace("Commit[{}] Committing {}", this,  x);
            // Commit all the optimistic updates
            x.getUnderlyingObject().optimisticCommitUnsafe();
            // If some other client updated this object, sync
            // it forward to grab those updates
            x.getUnderlyingObject().syncObjectUnsafe(
                        address - 1);
            // Also, be nice and transfer the undo
            // log from the optimistic updates
            // for this to work the write sets better
            // be the same
            List<SMREntry> committedWrites =
                    Transactions.getContext().getWriteSet().getWriteSet()
                        .getSMRUpdates(x.getStreamID());
            List<SMREntry> entryWrites =
                    ((ISMRConsumable) committedEntry
                            .getPayload(this.getBuilder().runtime))
                    .getSMRUpdates(x.getStreamID());
            if (committedWrites.size()
                    == entryWrites.size()) {
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
            x.getUnderlyingObject().seek(address + 1);
            log.trace("Commit[{}] Committed {}", this,  x);
        });

    }

    @SuppressWarnings("unchecked")
    protected void updateAllProxies(Consumer<ICorfuSMRProxyInternal> function) {
        Transactions.getContext().getWriteSet().getConflicts().keySet().forEach(x -> {
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

    /**
     * Get the first timestamp for this transaction.
     *
     * @return The first timestamp to be used for this transaction.
     */
    public long obtainSnapshotTimestamp() {
        final long lastSnapshot = Transactions.getContext().getReadSnapshot();
        if (lastSnapshot != Address.NO_SNAPSHOT) {
            // Snapshot was previously set, so use it
            return lastSnapshot;
        } else {
            // Otherwise, fetch a read token from the sequencer the linearize
            // ourselves against.
            long currentTail = builder.runtime
                    .getSequencerView().nextToken(Collections.emptySet(),
                            0).getToken().getTokenValue();
            log.trace("SnapshotTimestamp[{}] {}", this, currentTail);
            Transactions.getContext().setReadSnapshot(currentTail);
            return currentTail;
        }
    }
}
