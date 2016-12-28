package org.corfudb.runtime.object.transactions;

import lombok.Getter;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Represents a transactional context. Transactional contexts
 * manage per-thread transaction state.
 *
 * Created by mwei on 4/4/16.
 */
public abstract class AbstractTransactionalContext {

    /** Constant for the address of an uncommitted log entry.
     *
     */
    public static final long UNCOMMITTED_ADDRESS = -1L;

    /** Constant for a transaction which has been folded into
     * another transaction.
     */
    public static final long FOLDED_ADDRESS = -2L;

    /** Constant for a transaction which has been aborted.
     *
     */
    public static final long ABORTED_ADDRESS = -3L;

    /** The ID of the transaction. This is used for tracking only, it is
     * NOT recorded in the log.
     */
    @Getter
    public UUID transactionID;

    /** The builder used to create this transaction.
     *
     */
    @Getter
    public final TransactionBuilder builder;

    /**
     * The start time of the context.
     */
    @Getter
    public final long startTime;

    /** The global-log position that the transaction snapshots in all reads.
     */
    @Getter(lazy = true)
    private final long snapshotTimestamp = obtainSnapshotTimestamp();


    /** The address that the transaction was committed at.
     */
    @Getter
    public long commitAddress = AbstractTransactionalContext.UNCOMMITTED_ADDRESS;

    /** The parent context of this transaction, if in a nested transaction.*/
    @Getter
    private final AbstractTransactionalContext parentContext;

    /**
     * A conflict set of the txn.
     * The txn can only commit if none of the conflict objects (or the stream, if no specific conflict keys are provided)
     * is superseded by a later update.
     */
    @Getter
    private final Map<UUID, Set<Long>> conflictSet = new HashMap<>();

    /**
     * For the transaction update itself, we collect SMREntry objects representing the updates made by this TX
     *
     * @return a map from streams to write entry representing an update made by this TX
     */
    @Getter
    protected final Map<UUID, List<WriteSetEntry>> writeSet =
            new ConcurrentHashMap<>();

    /**
     * A future which gets completed when this transaction commits.
     * It is completed exceptionally when the transaction aborts.
     */
    @Getter
    public CompletableFuture<Boolean> completionFuture = new CompletableFuture<>();

    AbstractTransactionalContext(TransactionBuilder builder) {
        transactionID = UUID.randomUUID();
        this.builder = builder;
        this.startTime = System.currentTimeMillis();
        this.parentContext = TransactionalContext.getCurrentContext();
    }

    /** Access the state of the object.
     *
     * @param proxy             The proxy to access the state for.
     * @param accessFunction    The function to execute, which will be provided with the state
     *                          of the object.
     * @param conflictObject    Fine-grained conflict information, if available.
     * @param <R>               The return type of the access function.
     * @param <T>               The type of the proxy's underlying object.
     * @return                  The return value of the access function.
     */
    abstract public <R,T> R access(ICorfuSMRProxyInternal<T> proxy,
                                   ICorfuSMRAccess<R,T> accessFunction,
                                   Object[] conflictObject);

    /** Get the result of an upcall.
     *
     * @param proxy             The proxy to retrieve the upcall for.
     * @param timestamp         The timestamp to return the upcall for.
     * @param conflictObject    Fine-grained conflict information, if available.
     * @param <T>               The type of the proxy's underlying object.
     * @return                  The result of the upcall.
     */
    abstract public <T> Object getUpcallResult(ICorfuSMRProxyInternal<T> proxy,
                                               long timestamp,
                                               Object[] conflictObject);

    /** Log an SMR update to the Corfu log.
     *
     * @param proxy             The proxy which generated the update.
     * @param updateEntry       The entry which we are writing to the log.
     * @param conflictObject    Fine-grained conflict information, if available.
     * @param <T>               The type of the proxy's underlying object.
     * @return                  The address the update was written at.
     */
    abstract public <T> long logUpdate(ICorfuSMRProxyInternal<T> proxy,
                              SMREntry updateEntry,
                              Object[] conflictObject);

    /** Add a given transaction to this transactional context, merging
     * the read and write sets.
     * @param tc    The transactional context to merge.
     */
    abstract public void addTransaction(AbstractTransactionalContext tc);

    /** Commit the transaction to the log.
     *
     * @throws TransactionAbortedException  If the transaction is aborted.
     */
    public long commitTransaction() throws TransactionAbortedException {
        completionFuture.complete(true);
        return 0L;
    }

    /** Forcefully abort the transaction.
     */
    public void abortTransaction() {
        commitAddress = ABORTED_ADDRESS;
        completionFuture
                .completeExceptionally(new TransactionAbortedException());
    }

    abstract public long obtainSnapshotTimestamp();

    /** Add the proxy and conflict information to our conflict set.
     * @param proxy             The proxy to add
     * @param conflictObject    The fine-grained conflict information, if
     *                          available.
     */
    public void addToConflictSet(ICorfuSMRProxyInternal proxy, Object[] conflictObject)
    {
        conflictSet.computeIfAbsent(proxy.getStreamID(), k -> new HashSet<>());
        if (conflictObject != null)
            Arrays.asList(conflictObject).stream()
                .forEach(V -> conflictSet.get(proxy.getStreamID()).add(Long.valueOf(V.hashCode())) ) ;
    }


    /**
     * merge another conflictSet into this one
     * @param otherCSet
     */
    public void mergeInto(Map<UUID, Set<Long>> otherCSet) {
        otherCSet.forEach((branchID, conflictParamSet) -> {
            this.conflictSet.putIfAbsent(branchID, new HashSet<>());
            this.conflictSet.get(branchID).addAll(conflictParamSet);
        });
    }


}
