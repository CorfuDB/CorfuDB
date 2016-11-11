package org.corfudb.runtime.object.transactions;

import lombok.Data;
import lombok.Getter;

import lombok.RequiredArgsConstructor;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.*;

import java.util.*;
import java.util.concurrent.CompletableFuture;

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
    public long startTime;

    /** The address that the transaction was committed at.
     */
    @Getter
    public long commitAddress = AbstractTransactionalContext.UNCOMMITTED_ADDRESS;

    /** The parent context of this transaction, if in a nested transaction.*/
    @Getter
    AbstractTransactionalContext parentContext;

    /** A wrapper which combines SMREntries with
     * their upcall result.
     */
    @Data
    @RequiredArgsConstructor
    public static class UpcallWrapper {
        final SMREntry entry;
        Object upcallResult;
        boolean haveUpcallResult;
    }

    abstract public Set<ICorfuSMRProxyInternal> getModifiedProxies();

    /** Return the write set for this transaction
     *
     * @return  The write set, which contains all modifications this
     *          transaction will make.
     */
    abstract public Map<UUID, List<UpcallWrapper>> getWriteSet();

    /** Return the read set for this transaction
     *
     * @return  The read set, which contains all objects read by this
     *          transaction.
     */
    abstract public Set<UUID> getReadSet();

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
     * @param <R>               The return type of the access function.
     * @param <T>               The type of the proxy's underlying object.
     * @return                  The return value of the access function.
     */
    abstract public <R,T> R access(ICorfuSMRProxyInternal<T> proxy, ICorfuSMRAccess<R,T> accessFunction);

    /** Get the result of an upcall.
     *
     * @param proxy             The proxy to retrieve the upcall for.
     * @param timestamp         The timestamp to return the upcall for.
     * @param <T>               The type of the proxy's underlying object.
     * @return                  The result of the upcall.
     */
    abstract public <T> Object getUpcallResult(ICorfuSMRProxyInternal<T> proxy, long timestamp);

    /** Log an SMR update to the Corfu log.
     *
     * @param proxy             The proxy which generated the update.
     * @param updateEntry       The entry which we are writing to the log.
     * @param <T>               The type of the proxy's underlying object.
     * @return                  The address the update was written at.
     */
    abstract public <T> long logUpdate(ICorfuSMRProxyInternal<T> proxy,
                              SMREntry updateEntry);

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
}
