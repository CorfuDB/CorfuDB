package org.corfudb.runtime.view;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;

import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.CorfuWrapperBuilder;
import org.corfudb.runtime.object.ICorfuWrapper;
import org.corfudb.runtime.object.IObjectBuilder;
import org.corfudb.runtime.object.transactions.TransactionBuilder;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.Transactions;
import org.corfudb.runtime.view.stream.IStreamView;

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
    public static UUID TRANSACTION_STREAM_ID = CorfuRuntime.getStreamID("Transaction_Stream");

    @Getter
    @Setter
    boolean transactionLogging = false;


    @Getter
    Map<ObjectID, Object> objectCache = new ConcurrentHashMap<>();

    public ObjectsView(@Nonnull final CorfuRuntime runtime) {
        super(runtime);
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
        ICorfuWrapper<T> wrapper = (ICorfuWrapper<T>)obj;
        ObjectID oid = new ObjectID(destination, wrapper.getCorfuBuilder().getType());
        return (T) objectCache.computeIfAbsent(oid, x -> {
            IStreamView sv = runtime.getStreamsView().copy(wrapper.getId$CORFU(),
                    destination, wrapper.getObjectManager$CORFU().getVersion());
            try {
                final ObjectBuilder<T> originalBuilder = (ObjectBuilder<T>)
                        wrapper.getCorfuBuilder();
                IObjectBuilder<T> builder = new ObjectBuilder(originalBuilder.getRuntime())
                        .setArguments(originalBuilder.getArguments())
                        .setType(originalBuilder.getType())
                        .setSerializer(originalBuilder.getSerializer())
                        .setStreamID(sv.getId());

                return
                        CorfuWrapperBuilder.getWrapper((ObjectBuilder)builder);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });
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
    @SuppressWarnings({"checkstyle:methodname", "checkstyle:abbreviation"})
    public void TXBegin() {
        TransactionType type = TransactionType.OPTIMISTIC;

        TXBuild()
                .setType(type)
                .begin();
    }

    /** Builds a new transaction using the transaction
     * builder.
     * @return  A transaction builder to build a transaction with.
     */
    @SuppressWarnings({"checkstyle:methodname", "checkstyle:abbreviation"})
    public TransactionBuilder TXBuild() {
        return new TransactionBuilder(runtime);
    }

    /**
     * Aborts a transaction on the current thread.
     * Modifications to objects in the current transactional
     * context will be discarded.
     */
    @SuppressWarnings({"checkstyle:methodname", "checkstyle:abbreviation"})
    public void TXAbort() {
        Transactions.abort();
    }

    /**
     * Query whether a transaction is currently running.
     *
     * @return True, if called within a transactional context,
     *         False, otherwise.
     */
    @SuppressWarnings({"checkstyle:methodname", "checkstyle:abbreviation"})
    public boolean TXActive() {
        return Transactions.active();
    }

    /**
     * End a transaction on the current thread.
     *
     * @return The address of the transaction, if it commits.
     *
     * @throws TransactionAbortedException If the transaction could not be executed successfully.
     */
    @SuppressWarnings({"checkstyle:methodname", "checkstyle:abbreviation"})
    public long TXEnd()
            throws TransactionAbortedException {
        return Transactions.commit();
    }

    /** Given a Corfu object, syncs the object to the most up to date version.
     * If the object is not a Corfu object, this function won't do anything.
     * @param object    The Corfu object to sync.
     */
    public void syncObject(Object object) {
        if (object instanceof ICorfuWrapper<?>) {
            ICorfuWrapper<?> corfuObject = (ICorfuWrapper<?>) object;
            corfuObject.getObjectManager$CORFU().sync();
        }
    }

    /** Given a list of Corfu objects, syncs the objects to the most up to date
     * version, possibly in parallel.
     * @param objects   A list of Corfu objects to sync.
     */
    public void syncObject(Object... objects) {
        Arrays.stream(objects)
                .parallel()
                .filter(x -> x instanceof ICorfuWrapper<?>)
                .map(x -> (ICorfuWrapper<?>) x)
                .forEach(x -> x.getObjectManager$CORFU().sync());
    }

    @Data
    @SuppressWarnings({"checkstyle:abbreviation"})
    public static class ObjectID<T> {
        final UUID streamID;
        final Class<T> type;

        public String toString() {
            return "[" + streamID + ", " + type.getSimpleName() + "]";
        }
    }
}
