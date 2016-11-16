package org.corfudb.runtime.object.transactions;

import lombok.Getter;

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

    /** The ID of the transaction. This is used for tracking only, it is
     * NOT recorded in the log.
     */
    @Getter
    public UUID transactionID;

    /**
     * The runtime used to create this transaction.
     */
    @Getter
    public CorfuRuntime runtime;

    /**
     * The start time of the context.
     */
    @Getter
    public long startTime;

    /** The address that the transaction was committed at.
     */
    @Getter
    public long commitAddress = AbstractTransactionalContext.UNCOMMITTED_ADDRESS;

    /**
     * A future which gets completed when this transaction commits.
     * It is completed exceptionally when the transaction aborts.
     */
    @Getter
    public CompletableFuture<Boolean> completionFuture = new CompletableFuture<>();

    AbstractTransactionalContext(CorfuRuntime runtime) {
        transactionID = UUID.randomUUID();
        this.runtime = runtime;
        this.startTime = System.currentTimeMillis();
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

    /** Roll back this transaction in the proxy's underlying object.
     *
     * This method is unsafe, so it should only be called when the caller
     * has acquired a write lock for the proxy.
     *
     * @param proxy             The proxy which we are rolling back.
     * @param <T>               The type of the proxy's underlying object.
     */
    abstract public <T> void rollbackUnsafe(ICorfuSMRProxyInternal<T> proxy);

    /** Sync (play forward) this transaction in the proxy's underlying object.
     *
     * This method is unsafe, so it should only be called when the caller
     * has acquired a write lock for the proxy.
     * @param proxy             The proxy which we are playing forward.
     * @param <T>               The type of the proxy's underlying object.
     */
    abstract public <T> void syncUnsafe(ICorfuSMRProxyInternal<T> proxy);


    /** Return if the transaction can be undone, or if the object
     * must be reconstructed from the last checkpoint.
     * @param stream    The stream ID of the object to check.
     * @return          True if the transaction can be undone,
     *                  false otherwise.
     */
    abstract public boolean canUndoTransaction(UUID stream);

    abstract public void addTransaction(AbstractTransactionalContext tc);

    /** Commit the transaction to the log.
     *
     * @throws TransactionAbortedException  If the transaction is aborted.
     */
    public long commitTransaction() throws TransactionAbortedException {
        completionFuture.complete(true);
        return 0L;
    }

    /** Forcefully abort the transaction, restoring the state of
     * the underlying objects.
     */
    public void abortTransaction() {
        completionFuture
                .completeExceptionally(new TransactionAbortedException());
    }
}
