package org.corfudb.runtime.object.transactions;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Deque;
import java.util.LinkedList;

/** A class which allows access to transactional contexts, which manage
 * transactions. The static methods of this class provide access to the
 * thread's transaction stack, which is a stack of transaction contexts
 * active for a particular thread.
 *
 * Created by mwei on 1/11/16.
 */
@Slf4j
public class TransactionalContext {
    /** MicroTransaction support:
     * allow short-lived transactions to run without contention for this
     * duration
     */
    public static final Duration mTxDuration = Duration.ofMillis(1);

    /** A thread local stack containing all transaction contexts
     * for a given thread.
     */
    private static final ThreadLocal<Deque<AbstractTransactionalContext>> threadStack = ThreadLocal.withInitial(
            LinkedList<AbstractTransactionalContext>::new);

    /** Whether or not the current thread is in a nested transaction.
     *
     * @return  True, if the current thread is in a nested transaction.
     */
    public static boolean isInNestedTransaction() {return threadStack.get().size() > 1;}

    /**
     * Returns the transaction stack for the calling thread.
     *
     * @return The transaction stack for the calling thread.
     */
    public static Deque<AbstractTransactionalContext> getTransactionStack() {
        return threadStack.get();
    }

    /**
     * Returns the current transactional context for the calling thread.
     *
     * @return The current transactional context for the calling thread.
     */
    public static AbstractTransactionalContext getCurrentContext() {
        return getTransactionStack().peekFirst();
    }

    /**
     * Returns the last transactional context (parent/root) for the calling thread.
     *
     * @return The last transactional context for the calling thread.
     */
    public static AbstractTransactionalContext getRootContext() {
        return getTransactionStack().peekLast();
    }

    /**
     * Returns whether or not the calling thread is in a transaction.
     *
     * @return True, if the calling thread is in a transaction.
     * False otherwise.
     */
    public static boolean isInTransaction() {
        return getTransactionStack().peekFirst() != null;
    }

    /** Add a new transactional context to the thread's transaction stack.
     *
     * @param context   The context to add to the transaction stack.
     * @return          The context which was added to the transaction stack.
     */
    public static AbstractTransactionalContext newContext(AbstractTransactionalContext context) {
        getTransactionStack().addFirst(context);
        return context;
    }

    /** Remove the most recent transaction context from the transaction stack.
     *
     * @return          The context which was removed from the transaction stack.
     */
    public static AbstractTransactionalContext removeContext() {
        AbstractTransactionalContext r = getTransactionStack().pollFirst();
        if (getTransactionStack().isEmpty()) {
            synchronized (getTransactionStack())
            {
                getTransactionStack().notifyAll();
            }
        }
        return r;
    }
}
