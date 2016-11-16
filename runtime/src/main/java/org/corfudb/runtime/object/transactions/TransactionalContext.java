package org.corfudb.runtime.object.transactions;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;

import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.LinkedList;

/**
 * Created by mwei on 1/11/16.
 */
@Slf4j
public class TransactionalContext {


    private static final ThreadLocal<Deque<AbstractTransactionalContext>> threadStack = ThreadLocal.withInitial(
            LinkedList<AbstractTransactionalContext>::new);
    @Getter
    private static final LinkedHashSet<TXCompletionMethod> completionMethods = new LinkedHashSet<>();

    public static void addCompletionMethod(TXCompletionMethod completionMethod) {
        completionMethods.add(completionMethod);
    }

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
     * Returns whether or not the calling thread is in a transaction.
     *
     * @return True, if the calling thread is in a transaction.
     * False otherwise.
     */
    public static boolean isInTransaction() {
        return getTransactionStack().peekFirst() != null;
    }

    public static AbstractTransactionalContext newContext(CorfuRuntime runtime) {
        AbstractTransactionalContext context = new OptimisticTransactionalContext(runtime);
        getTransactionStack().addFirst(context);
        return context;
    }

    public static AbstractTransactionalContext newContext(AbstractTransactionalContext context) {
        getTransactionStack().addFirst(context);
        return context;
    }

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

    public static boolean isInOptimisticTransaction() {
        return getCurrentContext() instanceof OptimisticTransactionalContext;
    }

    @FunctionalInterface
    public interface TXCompletionMethod {
        void handle(AbstractTransactionalContext context);
    }
}
