package org.corfudb.runtime.view;

import com.sun.xml.internal.bind.v2.TODO;

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

import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.CorfuCompileWrapperBuilder;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionBuilder;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.Serializers;

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
     * Begins a transaction on the current thread.
     * Automatically selects the correct transaction strategy.
     * Modifications to objects will not be visible
     * to other threads or clients until TXEnd is called.
     */
    @SuppressWarnings({"checkstyle:methodname", "checkstyle:abbreviation"})
    public void TXBegin() {
        TransactionType type = TransactionType.OPTIMISTIC;

        /* If it is a nested transaction, inherit type of parent */
        if (TransactionalContext.isInTransaction()) {
            type = TransactionalContext.getCurrentContext().getBuilder().getType();
            log.trace("Inheriting parent's transaction type {}", type);
        }

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
        AbstractTransactionalContext context = TransactionalContext.getCurrentContext();
        if (context == null) {
            log.warn("Attempted to abort a transaction, but no transaction active!");
        } else {
            TxResolutionInfo txInfo = new TxResolutionInfo(
                    context.getTransactionID(), context.getSnapshotTimestamp());
            context.abortTransaction(new TransactionAbortedException(
                    txInfo, null, AbortCause.USER, context));
            TransactionalContext.removeContext();
        }
    }

    /**
     * Query whether a transaction is currently running.
     *
     * @return True, if called within a transactional context,
     *         False, otherwise.
     */
    @SuppressWarnings({"checkstyle:methodname", "checkstyle:abbreviation"})
    public boolean TXActive() {
        return TransactionalContext.isInTransaction();
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
        AbstractTransactionalContext context = TransactionalContext.getCurrentContext();
        if (context == null) {
            log.warn("Attempted to end a transaction, but no transaction active!");
            return AbstractTransactionalContext.UNCOMMITTED_ADDRESS;
        } else {
            long totalTime = System.currentTimeMillis() - context.getStartTime();
            log.trace("TXEnd[{}] time={} ms", context, totalTime);
            try {
                return TransactionalContext.getCurrentContext().commitTransaction();
            } catch (TransactionAbortedException e) {
                log.warn("TXEnd[{}] Aborted Exception {}", context, e);
                TransactionalContext.getCurrentContext().abortTransaction(e);
                throw e;
            } catch (NetworkException e) {
                log.warn("TXEnd[{}] Network Exception {}", context, e);
                long snapshotTimestamp;
                try {
                    snapshotTimestamp = context.getSnapshotTimestamp();
                } catch (NetworkException ne) {
                    snapshotTimestamp = -1L;
                }
                TxResolutionInfo txInfo = new TxResolutionInfo(context.getTransactionID(),
                    snapshotTimestamp);
                TransactionAbortedException tae = new TransactionAbortedException(txInfo,
                    null, null, AbortCause.NETWORK, e, context);
                context.abortTransaction(tae);
                throw tae;

            } catch (Exception e) {
               log.error("TXEnd[{}]: Unexpected exception", context, e);
                TxResolutionInfo txInfo = new TxResolutionInfo(context.getTransactionID(),
                    -1L);
                TransactionAbortedException tae = new TransactionAbortedException(txInfo,
                    null, null, AbortCause.UNDEFINED, e, context);
                context.abortTransaction(tae);
                throw new UnrecoverableCorfuError("Unexpected exception during commit", e);
            } finally {
                TransactionalContext.removeContext();
            }
        }
    }

    /** Given a Corfu object, syncs the object to the most up to date version.
     * If the object is not a Corfu object, this function won't do anything.
     * @param object    The Corfu object to sync.
     */
    public void syncObject(Object object) {
        if (object instanceof ICorfuSMR<?>) {
            ICorfuSMR<?> corfuObject = (ICorfuSMR<?>) object;
            corfuObject.getCorfuSMRProxy().sync();
        }
    }

    /** Given a list of Corfu objects, syncs the objects to the most up to date
     * version, possibly in parallel.
     * @param objects   A list of Corfu objects to sync.
     */
    public void syncObject(Object... objects) {
        Arrays.stream(objects)
                .parallel()
                .filter(x -> x instanceof ICorfuSMR<?>)
                .map(x -> (ICorfuSMR<?>) x)
                .forEach(x -> x.getCorfuSMRProxy().sync());
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
