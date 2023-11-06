package org.corfudb.runtime.object.transactions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRAccess;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;
import org.corfudb.runtime.object.ICorfuSMRSnapshotProxy;
import org.corfudb.runtime.object.MVOCorfuCompileProxy;
import org.corfudb.runtime.object.VersionLockedObject;
import org.corfudb.runtime.object.transactions.TransactionalContext.PreCommitListener;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.Utils;

/**
 * Represents a transactional context. Transactional contexts
 * manage per-thread transaction state.
 *
 * <p>Recall from {@link CorfuCompileProxy} that an SMR object layer implements objects whose
 * history of updates
 * are backed by a stream. If a Corfu object's method is an Accessor, it invokes the proxy's
 * access() method. Likewise, if a Corfu object's method is a Mutator or Accessor-Mutator,
 * it invokes the proxy's logUpdate() method.
 *
 * <p>Within transactional context, these methods invoke the transactionalContext
 * accessor/mutator helper.
 *
 * <p>For example, OptimisticTransactionalContext.access() is responsible for
 * sync'ing the proxy state to the snapshot version, and then doing the access.
 *
 * <p>logUpdate() within transactional context is
 * responsible for updating the write-set.
 *
 * <p>Finally, if a Corfu object's method is an Accessor-Mutator, then although the mutation
 * is delayed, it needs to obtain the result by invoking getUpcallResult() on the optimistic
 * stream. This is similar to the second stage of access(), except working on the optimistic
 * stream instead of the underlying stream.
 *
 * <p>Created by mwei on 4/4/16.
 */
@Slf4j
public abstract class AbstractTransactionalContext implements
        Comparable<AbstractTransactionalContext> {

    /**
     * Constant for the address of an uncommitted log entry.
     */
    public static final long UNCOMMITTED_ADDRESS = -1L;

    /**
     * Constant for a transaction which has been folded into
     * another transaction.
     */
    public static final long FOLDED_ADDRESS = -2L;

    /**
     * Constant for a transaction which has been aborted.
     */
    public static final long ABORTED_ADDRESS = -3L;

    /**
     * The ID of the transaction. This is used for tracking only, it is
     * NOT recorded in the log.
     */
    @SuppressWarnings("checkstyle:abbreviationaswordinname")
    @Getter
    public UUID transactionID;

    /**
     * The builder used to create this transaction.
     */
    @Getter
    public final Transaction transaction;

    /**
     * The start time of the context.
     */
    @Getter
    public final long startTime;

    /**
     * The global-log position that the transaction snapshots in all reads.
     */
    @Getter(lazy = true)
    private final Token snapshotTimestamp = obtainSnapshotTimestamp();

    /**
     * The address that the transaction was committed at.
     */
    @Getter
    public long commitAddress = AbstractTransactionalContext.UNCOMMITTED_ADDRESS;

    @Getter
    private final List<PreCommitListener> preCommitListeners = new ArrayList<>();

    /**
     * The parent context of this transaction, if in a nested transaction.
     */
    @Getter
    private final AbstractTransactionalContext parentContext;

    /**
     * CorfuStore Transaction context to allow nesting.
     */
    @Getter
    @Setter
    private TxnContext txnContext;

    /**
     * Flag used to track if this transaction has performed any accesses
     * on monotonic objects. This is used to compute the commit address
     * of read-only transactions.
     */
    protected boolean hasAccessedMonotonicObject = false;

    @Getter
    private final WriteSetInfo writeSetInfo = new WriteSetInfo();

    @Getter
    private final ConflictSetInfo readSetInfo = new ConflictSetInfo();

    // TODO: Make into a class?
    protected final Map<ICorfuSMRProxyInternal<?>, ICorfuSMRSnapshotProxy<?>> snapshotProxyMap = new HashMap<>();

    /**
     * Cache of last known position of streams accessed in this transaction.
     */
    protected final Map<UUID, Long> knownStreamsPosition = new HashMap<>();

    public long dbNanoTime = 0;

    public long tailQuery = 0;
    public long streamQuery = 0;
    public long timeSpent = 0;
    public List<String> readStreams = new ArrayList<>();

    AbstractTransactionalContext(Transaction transaction) {
        transactionID = Utils.genPseudorandomUUID();
        this.transaction = transaction;
        this.startTime = System.currentTimeMillis();
        this.parentContext = TransactionalContext.getCurrentContext();
        AbstractTransactionalContext.log.trace("TXBegin[{}]", this);
    }

    protected <T extends ICorfuSMR<T>> ICorfuSMRSnapshotProxy<T> getAndCacheSnapshotProxy(ICorfuSMRProxyInternal<T> proxy, long ts) {
        // TODO: Refactor me to avoid casting on ICorfuSMRProxyInternal type.
        ICorfuSMRSnapshotProxy<T> snapshotProxy = (ICorfuSMRSnapshotProxy<T>) snapshotProxyMap.get(proxy);
        final MVOCorfuCompileProxy<T> persistentProxy = (MVOCorfuCompileProxy<T>) proxy;
        if (snapshotProxy == null) {
            snapshotProxy = persistentProxy.getUnderlyingMVO().getSnapshotProxy(ts);
            snapshotProxyMap.put(proxy, snapshotProxy);
        }

        return snapshotProxy;
    }

    protected void updateKnownStreamPosition(@NonNull ICorfuSMRProxyInternal<?> proxy, long position) {
        Long val = knownStreamsPosition.get(proxy.getStreamID());

        if (val != null) {
            if (proxy.isMonotonicStreamAccess()) {
                Preconditions.checkState(val <= position,
                        "new stream position %s has decreased from %s", position, val);
            } else {
                // This precondition is not valid for monotonic objects since multiple accesses
                // performed by a transaction may not always see the same stream position.
                // This can occur if another thread performs accesses at a later snapshot and
                // interleaves with this transaction.
                Preconditions.checkState(val == position,
                        "inconsistent stream positions %s and %s", val, position);
                return;
            }
        }

        hasAccessedMonotonicObject = hasAccessedMonotonicObject || proxy.isMonotonicObject();
        knownStreamsPosition.put(proxy.getStreamID(), position);
    }

    /**
     * Access the state of the object.
     *
     * @param proxy          The proxy to access the state for.
     * @param accessFunction The function to execute, which will be provided with the state
     *                       of the object.
     * @param conflictObject Fine-grained conflict information, if available.
     * @param <R>            The return type of the access function.
     * @param <T>            The type of the proxy's underlying object.
     * @return The return value of the access function.
     */
    public abstract <R, T extends ICorfuSMR<T>> R access(ICorfuSMRProxyInternal<T> proxy,
                                                         ICorfuSMRAccess<R, T> accessFunction,
                                                         Object[] conflictObject);

    /**
     * Get the result of an upcall.
     *
     * @param proxy          The proxy to retrieve the upcall for.
     * @param timestamp      The timestamp to return the upcall for.
     * @param conflictObject Fine-grained conflict information, if available.
     * @param <T>            The type of the proxy's underlying object.
     * @return The result of the upcall.
     */
    public abstract <T extends ICorfuSMR<T>> Object getUpcallResult(ICorfuSMRProxyInternal<T> proxy,
                                                                     long timestamp,
                                                                     Object[] conflictObject);

    public void syncWithRetryUnsafe(VersionLockedObject vlo,
                                    Token snapshotTimestamp,
                                    ICorfuSMRProxyInternal proxy,
                                    @Nullable Runnable optimisticStreamSetter) {
        for (int x = 0; x < this.transaction.getRuntime().getParameters().getTrimRetry(); x++) {
            try {
                if (optimisticStreamSetter != null) {
                    // Swap ourselves to be the active optimistic stream.
                    // Inside setAsOptimisticStream, if there are
                    // currently optimistic updates on the object, we
                    // roll them back.  Then, we set this context as  the
                    // object's new optimistic context.
                    optimisticStreamSetter.run();
                }
                vlo.syncObjectUnsafe(snapshotTimestamp.getSequence());
                break;
            } catch (TrimmedException te) {
                log.info("syncWithRetryUnsafe: Encountered trimmed address space " +
                                "for snapshot {} of stream {} with pointer={} on attempt {}",
                        snapshotTimestamp.getSequence(), vlo.getID(), vlo.getVersionUnsafe(), x);

                // If a trim is encountered, we must reset the object
                vlo.resetUnsafe();
                if (!te.isRetriable()
                        || x == this.transaction.getRuntime().getParameters().getTrimRetry() - 1) {
                    // abort the transaction
                    TransactionAbortedException tae =
                            new TransactionAbortedException(
                                    new TxResolutionInfo(getTransactionID(), snapshotTimestamp),
                                    TokenResponse.NO_CONFLICT_KEY, proxy.getStreamID(),
                                    Address.NON_ADDRESS, AbortCause.TRIM, te, this);
                    abortTransaction(tae);
                    throw tae;
                }
            }
        }
    }

    /**
     * Log an SMR update to the Corfu log.
     *
     * @param proxy          The proxy which generated the update.
     * @param updateEntry    The entry which we are writing to the log.
     * @param conflictObject Fine-grained conflict information, if available.
     * @param <T>            The type of the proxy's underlying object.
     * @return The address the update was written at.
     */
    public abstract <T extends ICorfuSMR<T>> long logUpdate(ICorfuSMRProxyInternal<T> proxy,
                                                             SMREntry updateEntry,
                                                             Object[] conflictObject);

    public abstract void logUpdate(UUID streamId, SMREntry updateEntry);

    public abstract void logUpdate(UUID streamId, SMREntry updateEntry, List<UUID> streamTags);

    /**
     * Log a list of SMR updates to the specified Corfu stream log
     *
     * @param streamId        The id of the stream which the SMR updates are applied to
     * @param updateEntries   The entries which we are writing to the log
     */
    public abstract void logUpdate(UUID streamId, List<SMREntry> updateEntries);

    /**
     * Add a given transaction to this transactional context, merging
     * the read and write sets.
     *
     * @param tc The transactional context to merge.
     */
    public abstract void addTransaction(AbstractTransactionalContext tc);

    /**
     * Add an object that needs extra processing right before commit happens
     *
     * @param preCommitListener The context of the object that needs extra processing
     *                         along with its lambda.
     */
    public abstract void addPreCommitListener(PreCommitListener preCommitListener);

    /**
     * Commit the transaction to the log.
     *
     * @throws TransactionAbortedException If the transaction is aborted.
     */
    public abstract long commitTransaction() throws TransactionAbortedException;

    /**
     * Forcefully abort the transaction.
     */
    public void abortTransaction(TransactionAbortedException ae) {
        AbstractTransactionalContext.log.debug("TXAbort[{}]", this);
        commitAddress = ABORTED_ADDRESS;
    }

    /**
     * Retrieves the max address that has been read in this transaction.
     * @return highest sequence number observed while reading
     */
    protected long getMaxAddressRead() {
        if (knownStreamsPosition.isEmpty()) {
            return Address.NON_ADDRESS;
        }
        return Collections.max(knownStreamsPosition.values());
    }

    /**
     * Returns the min address that has been read in this transaction,
     * or NON_ADDRESS if no such address exists.
     */
    protected long getMinAddressRead() {
        if (knownStreamsPosition.isEmpty()) {
            return Address.NON_ADDRESS;
        }

        return Collections.min(knownStreamsPosition.values());
    }

    /**
     * Retrieves the current tail from the sequencer, if this
     * is a nested transaction, then inherit the snapshot
     * time from the parent transaction.
     *
     * @return the current global tail
     */
    private Token obtainSnapshotTimestamp() {
        long startSnapshotTime = System.nanoTime();
        try {
            final AbstractTransactionalContext parentCtx = getParentContext();
            final Token txnBuilderTs = getTransaction().getSnapshot();
            if (parentCtx != null) {
                // If we're in a nested transaction, the first read timestamp
                // needs to come from the root.
                Token parentTimestamp = parentCtx.getSnapshotTimestamp();
                log.trace("obtainSnapshotTimestamp: inheriting parent snapshot" +
                        " SnapshotTimestamp[{}] {}", this, parentTimestamp);
                return parentTimestamp;
            } else if (!txnBuilderTs.equals(Token.UNINITIALIZED)) {
                log.trace("obtainSnapshotTimestamp: using user defined snapshot" +
                        " SnapshotTimestamp[{}] {}", this, txnBuilderTs);
                return txnBuilderTs;
            } else {
                // Otherwise, fetch a read token from the sequencer the linearize
                // ourselves against.
                Token timestamp = getTransaction()
                        .getRuntime()
                        .getSequencerView()
                        .query()
                        .getToken();
                log.trace("obtainSnapshotTimestamp: sequencer SnapshotTimestamp[{}] {}", this, timestamp);
                return timestamp;
            }
        } finally {
            dbNanoTime += (System.nanoTime() - startSnapshotTime);
        }
    }

    /**
     * Add the proxy and conflict-params information to our read set.
     *
     * @param proxy           The proxy to add
     * @param conflictObjects The fine-grained conflict information, if
     *                        available.
     */
    public <T extends ICorfuSMR<T>> void addToReadSet(ICorfuSMRProxyInternal<T> proxy, Object[] conflictObjects) {
        getReadSetInfo().add(proxy, conflictObjects);
    }

    /**
     * Merge another readSet into this one.
     *
     * @param other  Source readSet to merge in
     */
    void mergeReadSetInto(ConflictSetInfo other) {
        getReadSetInfo().mergeInto(other);
    }

    /**
     * Add an update to the transaction optimistic write-set.
     *
     * @param proxy           the SMR object for this update
     * @param updateEntry     the update
     * @param conflictObjects the conflict objects to add
     * @return a synthetic "address" in the write-set, to be used for
     *     checking upcall results
     */
    <T extends ICorfuSMR<T>> long addToWriteSet(ICorfuSMRProxyInternal<T> proxy,
                                                SMREntry updateEntry, Object[] conflictObjects) {
        return getWriteSetInfo().add(proxy, updateEntry, conflictObjects);
    }

    void addToWriteSet(UUID streamId, SMREntry updateEntry) {
        getWriteSetInfo().add(streamId, updateEntry);
    }

    void addToWriteSet(UUID streamId, SMREntry updateEntry, List<UUID> streamTags) {
        getWriteSetInfo().add(streamId, updateEntry, streamTags);
    }

    public void addToWriteSet(UUID streamId, List<SMREntry> updateEntries) {
        getWriteSetInfo().add(streamId, updateEntries);
    }

    void mergeWriteSetInto(WriteSetInfo other) {
        getWriteSetInfo().mergeInto(other);
    }

    /**
     * convert our write set into a new MultiObjectSMREntry.
     *
     * @return  the write set
     */
    MultiObjectSMREntry collectWriteSetEntries() {
        return getWriteSetInfo().getWriteSet();
    }

    /**
     * Helper function to get a write set for a particular stream.
     *
     * @param id The stream to get a append set for.
     * @return The append set for that stream, as an ordered list.
     */
    List<SMREntry> getWriteSetEntryList(UUID id) {
        return getWriteSetInfo().getWriteSet().getSMRUpdates(id);
    }

    int getWriteSetEntrySize(UUID id) {
        return getWriteSetInfo()
                .getWriteSet()
                .getSMRUpdates(id)
                .size();
    }

    /**
     * Transactions are ordered by their snapshot timestamp.
     */
    @Override
    public int compareTo(AbstractTransactionalContext o) {
        return this.getSnapshotTimestamp()
                .compareTo(o.getSnapshotTimestamp());
    }

    @Override
    public String toString() {
        return "TX[" + Utils.toReadableId(transactionID) + "]";
    }
}