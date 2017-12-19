package org.corfudb.runtime.object.transactions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import javax.annotation.Nullable;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMRAccess;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;
import org.corfudb.runtime.object.VersionLockedObject;
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
@ToString
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
     * Constant for committing a transaction which did not
     * modify the log at all.
     */
    public static final long NOWRITE_ADDRESS = -4L;

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
    public final TransactionBuilder builder;

    /**
     * The start time of the context.
     */
    @Getter
    public final long startTime;

    /**
     * The global-log position that the transaction snapshots in all reads.
     */
    @Getter(lazy = true)
    private final long snapshotTimestamp = obtainSnapshotTimestamp();

    /**
     * The address that the transaction was committed at.
     */
    @Getter
    public long commitAddress = AbstractTransactionalContext.UNCOMMITTED_ADDRESS;

    /**
     * The parent context of this transaction, if in a nested transaction.
     */
    @Getter
    private final AbstractTransactionalContext parentContext;

    @Getter
    private final WriteSetInfo writeSetInfo = new WriteSetInfo();

    @Getter
    private final ConflictSetInfo readSetInfo = new ConflictSetInfo();

    /**
     * A future which gets completed when this transaction commits.
     * It is completed exceptionally when the transaction aborts.
     */
    @Getter
    public CompletableFuture<Boolean> completionFuture =
            new CompletableFuture<>();

    /**
     * Cache of last known position of streams accessed in this transaction.
     */
    @Getter
    private final Map<UUID, Long> knownStreamPosition = new HashMap<>();

    AbstractTransactionalContext(TransactionBuilder builder) {
        transactionID = UUID.randomUUID();
        this.builder = builder;

        startTime = System.currentTimeMillis();

        parentContext = TransactionalContext.getCurrentContext();

        AbstractTransactionalContext.log.debug("TXBegin[{}]", this);
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
    public abstract <R, T> R access(ICorfuSMRProxyInternal<T> proxy,
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
    public abstract <T> Object getUpcallResult(ICorfuSMRProxyInternal<T> proxy,
                                               long timestamp,
                                               Object[] conflictObject);

    public void syncWithRetryUnsafe(VersionLockedObject vlo,
                                    long snapshotTimestamp,
                                    ICorfuSMRProxyInternal proxy,
                                    @Nullable Consumer<VersionLockedObject> optimisticStreamSetter) {
        for (int x = 0; x < this.builder.getRuntime().getParameters().getTrimRetry(); x++) {
            try {
                if (optimisticStreamSetter != null) {
                    // Swap ourselves to be the active optimistic stream.
                    // Inside setAsOptimisticStream, if there are
                    // currently optimistic updates on the object, we
                    // roll them back.  Then, we set this context as  the
                    // object's new optimistic context.
                    optimisticStreamSetter.accept(vlo);
                }
                vlo.syncObjectUnsafe(snapshotTimestamp);
                break;
            } catch (TrimmedException te) {
                // If a trim is encountered, we must reset the object
                vlo.resetUnsafe();
                if (!te.isRetriable()
                        || x == this.builder.getRuntime().getParameters().getTrimRetry() - 1) {
                    // abort the transaction
                    TransactionAbortedException tae =
                            new TransactionAbortedException(
                                    new TxResolutionInfo(getTransactionID(),
                                            snapshotTimestamp), null,
                                    proxy.getStreamID(),
                                    AbortCause.TRIM, te, this);
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
    public abstract <T> long logUpdate(ICorfuSMRProxyInternal<T> proxy, SMREntry updateEntry,
                                       Object[] conflictObject);

    /**
     * Add a given transaction to this transactional context, merging
     * the read and write sets.
     *
     * @param tc The transactional context to merge.
     */
    public abstract void addTransaction(AbstractTransactionalContext tc);

    /**
     * Commit the transaction to the log.
     *
     * @throws TransactionAbortedException If the transaction is aborted.
     */
    public long commitTransaction() throws TransactionAbortedException {
        completionFuture.complete(true);
        return NOWRITE_ADDRESS;
    }

    /**
     * Forcefully abort the transaction.
     */
    public void abortTransaction(TransactionAbortedException ae) {
        AbstractTransactionalContext.log.debug("TXAbort[{}]", this);
        commitAddress = ABORTED_ADDRESS;
        completionFuture
                .completeExceptionally(ae);
    }

    public abstract long obtainSnapshotTimestamp();

    /**
     * Add the proxy and conflict-params information to our read set.
     *
     * @param proxy           The proxy to add
     * @param conflictObjects The fine-grained conflict information, if
     *                        available.
     */
    public void addToReadSet(ICorfuSMRProxyInternal proxy, Object[] conflictObjects) {
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
    long addToWriteSet(ICorfuSMRProxyInternal proxy, SMREntry updateEntry, Object[]
            conflictObjects) {
        return getWriteSetInfo().add(proxy, updateEntry, conflictObjects);
    }

    /**
     * collect all the conflict-params from the write-set for this transaction
     * into a set.
     *
     * @return A set of longs representing all the conflict params
     */
    Map<UUID, Set<byte[]>> collectWriteConflictParams() {
        return getWriteSetInfo().getHashedConflictSet();
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
        List<SMREntry> entries = getWriteSetInfo().getWriteSet().getSMRUpdates(id);

        if (entries == null) {
            return 0;
        } else {
            return entries.size();
        }
    }

    /**
     * Transactions are ordered by their snapshot timestamp.
     */
    @Override
    public int compareTo(AbstractTransactionalContext o) {
        return Long.compare(this.getSnapshotTimestamp(), o
                .getSnapshotTimestamp());
    }

    @Override
    public String toString() {
        return "TX[" + Utils.toReadableId(transactionID) + "]";
    }
}