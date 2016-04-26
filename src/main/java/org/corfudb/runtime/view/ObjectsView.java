package org.corfudb.runtime.view;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.TXEntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.*;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.OptimisticTransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.util.LambdaUtils;
import org.corfudb.util.serializer.Serializers;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A view of the objects inside a Corfu instance.
 * Created by mwei on 1/7/16.
 */
@Slf4j
public class ObjectsView extends AbstractView {

    @Data
    public static class ObjectID<T,R> {
        final UUID streamID;
        final Class<T> type;
        final Class<R> overlay;
    }

    @Data
    class CallSiteData {

        TransactionStrategy getNextStrategy() {
            return TransactionStrategy.OPTIMISTIC;
        }
    }

    @Getter
    Map<Long, CompletableFuture> txFuturesMap = new ConcurrentHashMap<>();

    @Getter
    Map<ObjectID, Object> objectCache = new ConcurrentHashMap<>();

    LoadingCache<StackTraceElement, CallSiteData> callSiteDataCache = Caffeine.newBuilder()
                                                                        .maximumSize(10000)
                                                                        .build(stackTraceElement -> new CallSiteData());

    public ObjectsView(CorfuRuntime runtime) {
        super(runtime);
    }

    /** Gets a view of an object in the Corfu instance.
     *
     * @param streamID      The stream that the object should be read from.
     * @param type          The type of the object that should be opened.
     *                      If the type implements ICorfuSMRObject or implements an interface which implements
     *                      ISMRInterface, Accessors, Mutator and MutatorAccessor annotations will be respected.
     *                      Otherwise, the entire object will be wrapped around SMR and it will be assumed that
     *                      all methods are MutatorAccessors.
     * @param <T>           The type of object to return.
     * @return              Returns a view of the object in a Corfu instance.
     */
    @Deprecated
    public <T> T open(@NonNull UUID streamID, @NonNull Class<T> type, Object... args) {
        return new ObjectBuilder<T>(runtime)
                .setType(type)
                .setStreamID(streamID)
                .setArguments(args)
                .open();
    }

    /** Gets a view of an object in the Corfu instance.
     *
     * @param streamName    The stream that the object should be read from.
     * @param type          The type of the object that should be opened.
     *                      If the type implements ICorfuSMRObject or implements an interface which implements
     *                      ISMRInterface, Accessors, Mutator and MutatorAccessor annotations will be respected.
     *                      Otherwise, the entire object will be wrapped around SMR and it will be assumed that
     *                      all methods are MutatorAccessors.
     * @param <T>           The type of object to return.
     * @return              Returns a view of the object in a Corfu instance.
     */
    @Deprecated
    public <T> T open(@NonNull String streamName, @NonNull Class<T> type, Object... args) {
        return new ObjectBuilder<T>(runtime)
                .setType(type)
                .setStreamName(streamName)
                .setArguments(args)
                .open();
    }

    /** Return an object builder which builds a new object.
     *
     * @return  An object builder to open an object with.
     */
    public ObjectBuilder<?> build() {
        return new ObjectBuilder(runtime);
    }

    /** Creates a copy-on-write copy of an object.
     *
     * @param obj           The object that should be copied.
     * @param destination   The destination ID of the object to be copied.
     * @param <T>           The type of the object being copied.
     * @return              A copy-on-write copy of the object.
     */
    @SuppressWarnings("unchecked")
    public <T> T copy(@NonNull T obj, @NonNull UUID destination) {
        CorfuSMRObjectProxy<T> proxy = (CorfuSMRObjectProxy<T>) ((ICorfuSMRObject)obj).getProxy();
            ObjectID oid = new ObjectID(destination, proxy.getOriginalClass(), null);
            return (T) objectCache.computeIfAbsent(oid, x -> {
                StreamView sv = runtime.getStreamsView().copy(proxy.getSv().getStreamID(),
                        destination, proxy.getTimestamp());
                return CorfuProxyBuilder.getProxy(proxy.getOriginalClass(), null, sv, runtime,
                        proxy.getSerializer(), Collections.emptySet());
            });
    }

    /** Creates a copy-on-write copy of an object.
     *
     * @param obj           The object that should be copied.
     * @param destination   The destination stream name of the object to be copied.
     * @param <T>           The type of the object being copied.
     * @return              A copy-on-write copy of the object.
     */
    @SuppressWarnings("unchecked")
    public <T> T copy(@NonNull T obj, @NonNull String destination) {
        return copy(obj, CorfuRuntime.getStreamID(destination));
    }

    /** Begins a transaction on the current thread.
     *  Automatically selects the correct transaction strategy.
     *  Modifications to objects will not be visible
     *  to other threads or clients until TXEnd is called.
     */
    public void TXBegin() {
        TXBegin(TransactionStrategy.AUTOMATIC);
    }

    /** Determine the call site for the transaction.
     *
     * @return  The call site where TXBegin(); was called.
     */
    public StackTraceElement getCallSite()
    {
        StackTraceElement[] st = new Exception().getStackTrace();
        for (int i = 1; i < st.length ; i++)
        {
            if (!st[i].getClassName().equals(this.getClass().getName()))
            {
                return st[i];
            }
        }
        throw new RuntimeException("Couldn't get call site!");
    }

    /** Begins a transaction on the current thread.
     *  Modifications to objects will not be visible
     *  to other threads or clients until TXEnd is called.
     */
    public void TXBegin(TransactionStrategy strategy) {

        if (strategy == TransactionStrategy.AUTOMATIC)
        {
            StackTraceElement st = getCallSite();
            log.trace("Determined call site to be {}", st);
            CallSiteData csd = callSiteDataCache.get(st);
            strategy = csd.getNextStrategy();
            log.trace("Selecting transaction strategy {} based on call site.", strategy);
        }


        AbstractTransactionalContext context = TransactionalContext.newContext(runtime);
        context.setStrategy(strategy);
        context.setStartTime(System.currentTimeMillis());

        log.trace("Entering transactional context {}.", context.getTransactionID());
    }


    /** Aborts a transaction on the current thread.
     * Modifications to objects in the current transactional
     * context will be discarded.
     */
    public void TXAbort() {
        AbstractTransactionalContext context = TransactionalContext.removeContext();
        if (context == null)
        {
            log.warn("Attempted to abort a transaction, but no transaction active!");
        }
        else {
            log.trace("Aborting transactional context {}.", context.getTransactionID());
        }
    }

    /** Query whether a transaction is currently running.
     *
     * @return  True, if called within a transactional context,
     *          False, otherwise.
     */
    public boolean TXActive() {
        return TransactionalContext.isInTransaction();
    }

    /** End a transaction on the current thread.
     *  @throws TransactionAbortedException      If the transaction could not be executed successfully.
     */
    public void TXEnd()
        throws TransactionAbortedException
    {
        AbstractTransactionalContext context = TransactionalContext.getCurrentContext();
        if (context == null)
        {
            log.warn("Attempted to end a transaction, but no transaction active!");
        }
        else {
            long totalTime = System.currentTimeMillis() - context.getStartTime();
            log.trace("Exiting (committing) transactional context {} (time={} ms).",
                    context.getTransactionID(), totalTime);
            if (context.hasNoWriteSet())
            {
                log.trace("Transactional context {} was read-only, exiting context without commit.",
                        context.getTransactionID());
                TransactionalContext.removeContext();
                return;
            }

            if (TransactionalContext.getTransactionStack().size() > 1) {
                TransactionalContext.removeContext();
                log.trace("Transaction {} within context {}, writing to context.", context.getTransactionID(),
                        TransactionalContext.getCurrentContext().getTransactionID());
                TransactionalContext.getCurrentContext().addTransaction(context);
            } else {
                TXEntry entry = ((OptimisticTransactionalContext)context).getEntry();
                long address = runtime.getStreamsView().write(entry.getAffectedStreams(), entry);
                TransactionalContext.removeContext();
                log.trace("TX entry {} written at address {}", entry, address);
                //now check if the TX will be an abort...
                if (entry.isAborted()) {
                    throw new TransactionAbortedException();
                }
                //otherwise fire the handlers.
                TransactionalContext.getCompletionMethods().parallelStream()
                        .forEach(x -> x.handle(context));
            }
        }
    }

    /** Executes the supplied function transactionally.
     *
     * @param txFunction                        The function to execute transactionally.
     * @param <T>                               The return type of the function to execute.
     * @return                                  The return value of the function.
     * @throws TransactionAbortedException      If the transaction could not be executed successfully.
     */
    @SuppressWarnings("unchecked")
    public <T> T executeTX(Supplier<T> txFunction)
        throws TransactionAbortedException
    {
        return (T) executeTX(txFunction, TransactionStrategy.AUTOMATIC, null);
    }

    /** Executes the supplied function transactionally.
     *
     * @param txFunction                        The function to execute transactionally.
     * @param <T>                               The return type of the function to execute.
     * @return                                  The return value of the function.
     * @throws TransactionAbortedException      If the transaction could not be executed successfully.
     */
    @SuppressWarnings("unchecked")
    public <T, R> R executeTX(Function<T, R> txFunction, T arg0)
            throws TransactionAbortedException
    {
        return (R) executeTX(txFunction, TransactionStrategy.AUTOMATIC, arg0);
    }

    /** Executes the supplied function transactionally.
     *
     * @param txFunction                        The function to execute transactionally.
     * @return                                  The return value of the function.
     * @throws TransactionAbortedException      If the transaction could not be executed successfully.
     */
    public Object executeTX(Object txFunction, TransactionStrategy strategy, Object... arguments)
            throws TransactionAbortedException
    {
        if (strategy == TransactionStrategy.AUTOMATIC)
        {
            strategy = findTransactionStrategyForLambda(txFunction);
            log.trace("Automatically determined transaction strategy={}", strategy);
        }

        switch (strategy) {
            case OPTIMISTIC:
                TXBegin();
                Object result = LambdaUtils.executeUnknownLambda(txFunction, arguments);
                TXEnd();
                return result;
        }

        throw new UnsupportedOperationException("Unsupported transaction strategy " + strategy.toString());
    }


    public <T> TransactionStrategy findTransactionStrategyForLambda(Object txFunction) {
        //log.info("TXtype={}, TXLength={}", txFunction.getClass(), Utils.getByteCodeOf(txFunction.getClass()).length);
        //System.out.print(Utils.printByteCode(Utils.getByteCodeOf(txFunction.getClass())));
        return TransactionStrategy.OPTIMISTIC;
    }
}
