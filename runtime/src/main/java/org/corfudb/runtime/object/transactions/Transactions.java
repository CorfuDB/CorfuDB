package org.corfudb.runtime.object.transactions;

import java.util.Deque;
import java.util.LinkedList;
import java.util.UUID;

import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.Address;

import lombok.extern.slf4j.Slf4j;

/** This class provides support for transactions.
 *
 */
@Slf4j
public class Transactions {

    /** A thread local stack containing all transactions for a given thread.
     */
    private static final ThreadLocal<Deque<AbstractTransaction>>
            threadTransactionStack = ThreadLocal.withInitial(LinkedList<AbstractTransaction>::new);

    /** A thread local containing the transaction context for a given thread.
     * This context is reset whenever a transaction stack is committed or aborted.
     * */
    private static final ThreadLocal<TransactionContext>
            transactionContext = ThreadLocal.withInitial(TransactionContext::new);

    /** Begin a new transaction.
     *
     * @param transaction   The transaction which will begin execution.
     */
    public static void begin(AbstractTransaction transaction) {
        // Optimistic transactions may only be nested on
        // other optimistic transactions.
        if (active()
                && transaction instanceof AbstractOptimisticTransaction
                && !(threadTransactionStack.get().getFirst()
                instanceof AbstractOptimisticTransaction)) {
            throw new TransactionAbortedException(
                    new TxResolutionInfo(new UUID(0, 0), 0),
                    null, AbortCause.UNSUPPORTED, null);
        }

        threadTransactionStack.get().addFirst(transaction);
    }

    /** Attempt to commit the current transaction. */
    public static long commit() throws TransactionAbortedException {
        try {
            if (threadTransactionStack.get().isEmpty()) {
                log.warn("commit: Attempt to commit but no transaction present.");
                return Address.NEVER_READ;
            }
            long pos = threadTransactionStack.get().peekFirst().commit();
            threadTransactionStack.get().removeFirst();
            return pos;
        } catch (TransactionAbortedException tae) {
            // Transaction aborted during the commit attempt, so empty the
            // stack and re-throw the exception.
            threadTransactionStack.get().clear();
            throw tae;
        } finally {
            // If there are no transactions left on the stack,
            // clear the current context.
            if (threadTransactionStack.get().isEmpty()) {
                resetTransactionContext();
            }
        }
    }

    /** Abort the transaction stack. */
    public static void abort() {
        threadTransactionStack.get().clear();
        resetTransactionContext();
        throw new TransactionAbortedException(new
                TxResolutionInfo(new UUID(0,0), 0),
                null, AbortCause.USER, null);
    }

    /** Check whether there is an active transaction on this thread or not.
     *
     * @return  True, if there is an active transaction on this thread, false otherwise.
     */
    public static boolean active() {
        return !threadTransactionStack.get().isEmpty();
    }

    /** Check whether the current thread stack contains more than one active transaction.
     *
     * @return  True, if there is more than one active transaction, false otherwise.
     */
    public static boolean isNested() {
        return threadTransactionStack.get().size() > 1;
    }

    /** Get the snapshot "version" reads are being served from, if present, otherwise
     * returns Address.NEVER_READ.
     *
     * @return The address reads are being served from, or Address.NO_SNAPSHOT otherwise.
     */
    public static long getReadSnapshot() {
        return transactionContext.get().getReadSnapshot();
    }

    /** Get the current active transaction from the thread stack.
     *
     * @return  The current active transaction, or null, if no transaction is present.
     */
    public static AbstractTransaction current() {
        return threadTransactionStack.get().peekFirst();
    }

    /** Reset the transaction context, clearing the read and write sets. */
    private static void resetTransactionContext() {
        transactionContext.remove();
        transactionContext.set(new TransactionContext());
    }

    /** Get the current transaction context. */
    static TransactionContext getContext() {
        return transactionContext.get();
    }
}
