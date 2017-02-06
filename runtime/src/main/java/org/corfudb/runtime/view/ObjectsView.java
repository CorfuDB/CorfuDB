package org.corfudb.runtime.view;

import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.*;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionBuilder;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.Serializers;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A view of the objects inside a Corfu instance.
 * Created by mwei on 1/7/16.
 */
@Slf4j
public class ObjectsView extends AbstractView {

    /**
     * The Transaction stream is used to log/write successful transactions from different clients.
     * Transaction data and meta data can be obtained by reading this stream.
     */
    static public UUID TRANSACTION_STREAM_ID = CorfuRuntime.getStreamID("Transaction_Stream");

    @Getter
    @Setter
    boolean transactionLogging = false;

    @Getter
    Map<Long, CompletableFuture> txFuturesMap = new ConcurrentHashMap<>();

    @Getter
    Map<ObjectID, Object> objectCache = new ConcurrentHashMap<>();

    public ObjectsView(CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     * Gets a view of an object in the Corfu instance.
     *
     * @param streamID The stream that the object should be read from.
     * @param type     The type of the object that should be opened.
     *                 If the type implements ICorfuSMRObject or implements an interface which implements
     *                 ISMRInterface, Accessors, Mutator and MutatorAccessor annotations will be respected.
     *                 Otherwise, the entire object will be wrapped around SMR and it will be assumed that
     *                 all methods are MutatorAccessors.
     * @param <T>      The type of object to return.
     * @return Returns a view of the object in a Corfu instance.
     */
    @Deprecated
    public <T> T open(@NonNull UUID streamID, @NonNull Class<T> type, Object... args) {
        return build()
                .setType(type)
                .setStreamID(streamID)
                .setArgumentsArray(args)
                .open();
    }

    /**
     * Gets a view of an object in the Corfu instance.
     *
     * @param streamName The stream that the object should be read from.
     * @param type       The type of the object that should be opened.
     *                   If the type implements ICorfuSMRObject or implements an interface which implements
     *                   ISMRInterface, Accessors, Mutator and MutatorAccessor annotations will be respected.
     *                   Otherwise, the entire object will be wrapped around SMR and it will be assumed that
     *                   all methods are MutatorAccessors.
     * @param <T>        The type of object to return.
     * @return Returns a view of the object in a Corfu instance.
     */
    @Deprecated
    public <T> T open(@NonNull String streamName, @NonNull Class<T> type, Object... args) {
        return new ObjectBuilder<T>(runtime)
                .setType(type)
                .setStreamName(streamName)
                .setArgumentsArray(args)
                .open();
    }

    /**
     * Return an object builder which builds a new object.
     *
     * @return An object builder to open an object with.
     */
    public ObjectBuilder<?> build() {
        return new ObjectBuilder(runtime);
    }

    /**
     * Creates a copy-on-append copy of an object.
     *
     * @param obj         The object that should be copied.
     * @param destination The destination ID of the object to be copied.
     * @param <T>         The type of the object being copied.
     * @return A copy-on-append copy of the object.
     */
    @SuppressWarnings("unchecked")
    public <T> T copy(@NonNull T obj, @NonNull UUID destination) {
        try {
            // to be deprecated
            CorfuSMRObjectProxy<T> proxy = (CorfuSMRObjectProxy<T>) ((ICorfuSMRObject) obj).getProxy();
            ObjectID oid = new ObjectID(destination, proxy.getOriginalClass(), null);
            return (T) objectCache.computeIfAbsent(oid, x -> {
                IStreamView sv = runtime.getStreamsView().copy(proxy.getSv().getID(),
                        destination, proxy.getTimestamp());
                return CorfuProxyBuilder.getProxy(proxy.getOriginalClass(), null, sv, runtime,
                        proxy.getSerializer(), Collections.emptySet());
            });
        } catch (Exception e) {
            // new code path
            ICorfuSMR<T> proxy = (ICorfuSMR<T>)obj;
            ObjectID oid = new ObjectID(destination, proxy.getCorfuSMRProxy().getObjectType(), null);
            return (T) objectCache.computeIfAbsent(oid, x -> {
                IStreamView sv = runtime.getStreamsView().copy(proxy.getCorfuStreamID(),
                        destination, proxy.getCorfuSMRProxy().getVersion());
                try {
                    return
                            CorfuCompileWrapperBuilder.getWrapper(proxy.getCorfuSMRProxy().getObjectType(),
                                    runtime, sv.getID(), null, Serializers.JSON);
                }
                catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
        }
    }

    /**
     * Creates a copy-on-append copy of an object.
     *
     * @param obj         The object that should be copied.
     * @param destination The destination stream name of the object to be copied.
     * @param <T>         The type of the object being copied.
     * @return A copy-on-append copy of the object.
     */
    @SuppressWarnings("unchecked")
    public <T> T copy(@NonNull T obj, @NonNull String destination) {
        return copy(obj, CorfuRuntime.getStreamID(destination));
    }

    /**
     * Begins a transaction on the current thread.
     * Automatically selects the correct transaction strategy.
     * Modifications to objects will not be visible
     * to other threads or clients until TXEnd is called.
     */
    public void TXBegin() {
        TXBuild()
                .setType(TransactionType.OPTIMISTIC) // TODO:default needs to be configurable
                .begin();
    }

    /** Builds a new transaction using the transaction
     * builder.
     * @return  A transaction builder to build a transaction with.
     */
    public TransactionBuilder TXBuild() {
        return new TransactionBuilder(runtime);
    }

    /**
     * Aborts a transaction on the current thread.
     * Modifications to objects in the current transactional
     * context will be discarded.
     */
    public void TXAbort() {
        AbstractTransactionalContext context = TransactionalContext.getCurrentContext();
        if (context == null) {
            log.warn("Attempted to abort a transaction, but no transaction active!");
        } else {
            log.trace("Aborting transactional context {}.", context.getTransactionID());
            context.abortTransaction();
            TransactionalContext.removeContext();
        }
    }

    /**
     * Query whether a transaction is currently running.
     *
     * @return True, if called within a transactional context,
     * False, otherwise.
     */
    public boolean TXActive() {
        return TransactionalContext.isInTransaction();
    }

    /**
     * End a transaction on the current thread.
     *
     * @throws TransactionAbortedException If the transaction could not be executed successfully.
     *
     * @return The address of the transaction, if it commits.
     */
    public long TXEnd()
            throws TransactionAbortedException {
        AbstractTransactionalContext context = TransactionalContext.getCurrentContext();
        if (context == null) {
            log.warn("Attempted to end a transaction, but no transaction active!");
            return AbstractTransactionalContext.UNCOMMITTED_ADDRESS;
        } else {
            long totalTime = System.currentTimeMillis() - context.getStartTime();
            log.trace("Exiting (committing) transactional context {} (time={} ms).",
                    context.getTransactionID(), totalTime);

                try {
                    return TransactionalContext.getCurrentContext().commitTransaction();
                } finally {
                    TransactionalContext.removeContext();
                }
        }
    }

    @Data
    public static class ObjectID<T, R> {
        final UUID streamID;
        final Class<T> type;
        final Class<R> overlay;
    }
}
