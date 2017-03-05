package org.corfudb.runtime.object.transactions;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Deque;
import java.util.LinkedList;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListSet;

/** A class which allows access to transactional contexts, which manage
 * transactions. The static methods of this class provide access to the
 * thread's transaction stack, which is a stack of transaction contexts
 * active for a particular thread.
 *
 * Created by mwei on 1/11/16.
 */
@Slf4j
public class TransactionalContext {

    /** A thread local stack containing all transaction contexts
     * for a given thread.
     */
    private static final ThreadLocal<Deque<AbstractTransactionalContext>>
            threadTransactionStack = ThreadLocal.withInitial(
            LinkedList<AbstractTransactionalContext>::new);

    /** A navigable set (priority queue) of contexts. The minimum context
     * indicates how far we should try to keep the undo log up to.
     */
    private static final NavigableSet<AbstractTransactionalContext> contextSet =
            new ConcurrentSkipListSet<>();

    /** Return the oldest snapshot active in the system, or -1L if
     * there are no active snapshots. */
    public static long getOldestSnapshot() {
        if (contextSet.isEmpty()) {
            return -1L;
        }
        try {
            return contextSet.first().getSnapshotTimestamp();
        } catch (NoSuchElementException nse) {
            return -1L;
        }
    }

    /** Whether or not the current thread is in a nested transaction.
     *
     * @return  True, if the current thread is in a nested transaction.
     */
    public static boolean isInNestedTransaction() {return threadTransactionStack.get().size() > 1;}

    /**
     * Returns the transaction stack for the calling thread.
     *
     * @return The transaction stack for the calling thread.
     */
    public static Deque<AbstractTransactionalContext> getTransactionStack() {
        return threadTransactionStack.get();
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
        contextSet.add(context);
        return context;
    }

    /** Remove the most recent transaction context from the transaction stack.
     *
     * @return          The context which was removed from the transaction stack.
     */
    public static AbstractTransactionalContext removeContext() {
        AbstractTransactionalContext r = getTransactionStack().pollFirst();
        contextSet.remove(r);
        if (getTransactionStack().isEmpty()) {
            synchronized (getTransactionStack())
            {
                getTransactionStack().notifyAll();
            }
        }
        return r;
    }
}
