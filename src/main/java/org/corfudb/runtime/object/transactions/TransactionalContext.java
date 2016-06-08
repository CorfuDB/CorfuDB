package org.corfudb.runtime.object.transactions;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.logprotocol.TXEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.object.CorfuSMRObjectProxy;
import org.corfudb.runtime.view.TransactionStrategy;
import org.corfudb.util.serializer.Serializers;

import java.util.*;

/**
 * Created by mwei on 1/11/16.
 */
@Slf4j
public class TransactionalContext {

    private static final ThreadLocal<Deque<AbstractTransactionalContext>> threadStack = ThreadLocal.withInitial(
            LinkedList<AbstractTransactionalContext>::new);

    @FunctionalInterface
    public interface TXCompletionMethod {
        void handle(AbstractTransactionalContext context);
    }

    @Getter
    private static final LinkedHashSet<TXCompletionMethod> completionMethods = new LinkedHashSet<>();

    public static void addCompletionMethod(TXCompletionMethod completionMethod) {
        completionMethods.add(completionMethod);
    }

    /** Returns the transaction stack for the calling thread.
     *
     * @return      The transaction stack for the calling thread.
     */
    public static Deque<AbstractTransactionalContext> getTransactionStack()
    {
        return threadStack.get();
    }

    /** Returns the current transactional context for the calling thread.
     *
     * @return      The current transactional context for the calling thread.
     */
    public static AbstractTransactionalContext getCurrentContext() {
        return getTransactionStack().peekFirst();
    }

    /** Returns whether or not the calling thread is in a transaction.
     *
     * @return      True, if the calling thread is in a transaction.
     *              False otherwise.
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
        if (getCurrentContext() != null) {
            getCurrentContext().close();
        }
        return getTransactionStack().pollFirst();
    }

    public static boolean isInOptimisticTransaction() {
        return getCurrentContext() instanceof OptimisticTransactionalContext;}
}
