package org.corfudb.runtime.object.transactions;

import java.util.UUID;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.IObjectManager;
import org.corfudb.runtime.object.IStateMachineStream;
import org.corfudb.runtime.view.Address;

/** This class provides support for transactions.
 *
 */
@Slf4j
public class Transactions {
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
                && !(current() instanceof AbstractOptimisticTransaction)) {
            throw new TransactionAbortedException(
                    new TxResolutionInfo(new UUID(0, 0), 0),
                    null, AbortCause.UNSUPPORTED, null);
        }

        transactionContext.get().setCurrent(transaction);
    }

    /** Attempt to commit the current transaction. */
    public static long commit() throws TransactionAbortedException {
        final TransactionContext context = transactionContext.get();
        try {
            if (context.getCurrent() == null) {
                log.warn("commit: Attempt to commit but no transaction present.");
                return Address.NEVER_READ;
            }
            final AbstractTransaction currentTransaction = context.getCurrent();
            long pos = context.getCurrent().commit();
            context.setCurrent(context.getCurrent().getParent());
            // If we are not nested, this is the final TX in the chain and the context
            // has been committed to the log.
            if (currentTransaction.getParent() == null) {
                context.setCommitAddress(pos);
            }
            return pos;
        } catch (TransactionAbortedException tae) {
            // Transaction aborted during the commit attempt,
            // so stop all transactions and re-throw the exception.
            context.setCurrent(null);
            throw tae;
        } finally {
            // If there are no transactions left on the stack,
            // clear the current context.
            if (context.getCurrent() == null) {
                resetTransactionContext();
            }
        }
    }

    /** Abort the transaction stack. */
    public static void abort() {
        abort(new TransactionAbortedException(new
                TxResolutionInfo(new UUID(0,0), 0),
                null, AbortCause.USER, null));
    }

    /** Abort the transaction stack with the specified exception.
     * @param tae   The exception to abort with.
     */
    public static void abort(TransactionAbortedException tae) {
        resetTransactionContext();
        throw tae;
    }

    /** Check whether there is an active transaction on this thread or not.
     *
     * @return  True, if there is an active transaction on this thread, false otherwise.
     */
    public static boolean active() {
        return transactionContext.get().getCurrent() !=  null;
    }

    /** Check whether the current thread stack contains more than one active transaction.
     *
     * @return  True, if there is more than one active transaction, false otherwise.
     */
    public static boolean isNested() {
        return transactionContext.get().getCurrent() != null
                && transactionContext.get().getCurrent().getParent() != null;
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
        return getContext().getCurrent();
    }

    /** Get a state machine stream for the active transaction.
     *
     * @param manager   The manager to get a state machine stream for
     * @param current   The current state machine stream
     * @return          A state machine stream for the active transaction.
     */
    public static IStateMachineStream getStateMachineStream(@Nonnull IObjectManager manager,
                                                @Nonnull IStateMachineStream current) {
        return getContext().getStateMachineStream(manager, current);
    }

    /** Reset the transaction context, clearing the read and write sets. */
    private static void resetTransactionContext() {
        transactionContext.remove();
    }

    /** Get the current transaction context. */
    static TransactionContext getContext() {
        return transactionContext.get();
    }
}
