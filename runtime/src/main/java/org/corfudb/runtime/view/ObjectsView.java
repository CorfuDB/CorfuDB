package org.corfudb.runtime.view;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.QuotaExceededException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.WriteSizeException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.MVOCache;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.Transaction;
import org.corfudb.runtime.object.transactions.Transaction.TransactionBuilder;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.corfudb.runtime.object.transactions.TransactionalContext;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A view of the objects inside a Corfu instance.
 * Created by mwei on 1/7/16.
 */
@Slf4j
public class ObjectsView extends AbstractView {

    private static final String LOG_REPLICATOR_STREAM_NAME =
        "LR_Transaction_Stream";

    public static final StreamTagInfo LOG_REPLICATOR_STREAM_INFO =
            new StreamTagInfo(LOG_REPLICATOR_STREAM_NAME,
                CorfuRuntime.getStreamID(LOG_REPLICATOR_STREAM_NAME));

    /**
     * @return the ID of the log replicator stream.
     */
    public static UUID getLogReplicatorStreamId() {
        return LOG_REPLICATOR_STREAM_INFO.getStreamId();
    }

    @Getter
    Map<ObjectID, Object> objectCache = new ConcurrentHashMap<>();

    @Getter
    @Setter
    MVOCache mvoCache = new MVOCache(runtime);

    public ObjectsView(@Nonnull final CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     * Return an object builder which builds a new object.
     *
     * @return An object builder to open an object with.
     */
    public <T extends ICorfuSMR<?>> SMRObject.Builder<T> build() {
        return new SMRObject.Builder<T>().setCorfuRuntime(runtime);
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
            type = TransactionalContext.getCurrentContext().getTransaction().getType();
            log.trace("Inheriting parent's transaction type {}", type);
        }

        TXBuild()
                .type(type)
                .build()
                .begin();
    }

    /**
     * Builds a new transaction using the transaction
     * builder.
     *
     * @return A transaction builder to build a transaction with.
     */
    @SuppressWarnings({"checkstyle:methodname", "checkstyle:abbreviation"})
    public TransactionBuilder TXBuild() {
        TransactionBuilder builder = Transaction.builder();
        builder.runtime(runtime);
        return builder;
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
                    txInfo, TokenResponse.NO_CONFLICT_KEY, TokenResponse.NO_CONFLICT_STREAM,
                    Address.NON_ADDRESS, AbortCause.USER, context));
            TransactionalContext.removeContext();
        }
    }

    /**
     * Query whether a transaction is currently running.
     *
     * @return True, if called within a transactional context,
     * False, otherwise.
     */
    @SuppressWarnings({"checkstyle:methodname", "checkstyle:abbreviation"})
    public boolean TXActive() {
        return TransactionalContext.isInTransaction();
    }

    /**
     * End a transaction on the current thread.
     *
     * @return The address of the transaction, if it commits.
     * @throws TransactionAbortedException If the transaction could not be executed successfully.
     */
    @SuppressWarnings({"checkstyle:methodname", "checkstyle:abbreviation"})
    public long TXEnd() throws TransactionAbortedException {
        AbstractTransactionalContext context = TransactionalContext.getCurrentContext();
        if (context == null) {
            log.warn("Attempted to end a transaction, but no transaction active!");
            return AbstractTransactionalContext.UNCOMMITTED_ADDRESS;
        }

        // Continue with ending the transaction if the context is not null
        long totalTime = System.currentTimeMillis() - context.getStartTime();
        log.trace("TXEnd[{}] time={} ms", context, totalTime);

        long txEndStartTime = System.nanoTime();
        try {
            long commitAddress = TransactionalContext.getCurrentContext().commitTransaction();
            MicroMeterUtils.time(Duration.ofMillis(System.currentTimeMillis() - context.getStartTime()),
                    "transaction.duration");
            if (commitAddress == Address.NON_ADDRESS) {
                // If no streams were touched or only empty streams were touched, the returned address would be -1
                // But -1 can be detrimental to stream subscription which is just looking for a safe spot
                // to resume subscription from, so instead return the address from the snapshot token taken.
                long snapshotAddress = context.getSnapshotTimestamp().getSequence();
                if (snapshotAddress >= 0) {
                    // Perform a read at this address to force materialization.
                    runtime.getAddressSpaceView().read(snapshotAddress);
                }
                return snapshotAddress;
            }
            return commitAddress;
        } catch (TransactionAbortedException e) {
            log.warn("TXEnd[{}] Aborted Exception ", context, e);
            TransactionalContext.getCurrentContext().abortTransaction(e);
            throw e;
        } catch (NetworkException | WriteSizeException | QuotaExceededException e) {

            Token snapshotTimestamp;
            try {
                snapshotTimestamp = context.getSnapshotTimestamp();
            } catch (NetworkException ne) {
                snapshotTimestamp = Token.UNINITIALIZED;
            }
            TxResolutionInfo txInfo = new TxResolutionInfo(context.getTransactionID(),
                    snapshotTimestamp);

            AbortCause cause = AbortCause.UNDEFINED;
            if (e instanceof NetworkException) {
                log.warn("TXEnd[{}] Network Exception {}", context, e);
                cause = AbortCause.NETWORK;
            } else if (e instanceof WriteSizeException) {
                log.error("TXEnd[{}] transaction size limit exceeded {}", context, e);
                cause = AbortCause.SIZE_EXCEEDED;
            } else if (e instanceof QuotaExceededException){
                log.error("TXEnd[{}] server quota exceeded {}", context, e);
                cause = AbortCause.QUOTA_EXCEEDED;
            }

            TransactionAbortedException tae = new TransactionAbortedException(
                    txInfo, cause, e, context);
            context.abortTransaction(tae);
            throw tae;

        } catch (Exception e) {
            log.error("TXEnd[{}]: Unexpected exception", context, e);
            TxResolutionInfo txInfo = new TxResolutionInfo(context.getTransactionID(),
                    Token.UNINITIALIZED);
            TransactionAbortedException tae = new TransactionAbortedException(
                    txInfo, AbortCause.UNDEFINED, e, context);
            context.abortTransaction(tae);
            throw new UnrecoverableCorfuError("Unexpected exception during commit", e);
        } finally {
            long txEndEndTime = System.nanoTime();
            MicroMeterUtils.time(Duration.ofNanos(context.dbNanoTime + (txEndEndTime - txEndStartTime)),
                    "transaction.db.duration");
            TransactionalContext.removeContext();
        }
    }

    /**
     * Run garbage collection on all opened objects. Note that objects
     * open with the NO_CACHE options will not be gc'd
     */
    public void gc(long trimMark) {
        for (Object obj : getObjectCache().values()) {
            ((ICorfuSMR) obj).getCorfuSMRProxy().getUnderlyingMVO().gc(trimMark);
        }
    }

    @Builder
    @AllArgsConstructor
    @EqualsAndHashCode
    @SuppressWarnings({"checkstyle:abbreviation"})
    public static class ObjectID {
        final UUID streamID;
        final Class<?> type;

        public String toString() {
            return "[" + streamID + ", " + type.getSimpleName() + "]";
        }
    }

    @AllArgsConstructor
    @Getter
    @EqualsAndHashCode
    public static final class StreamTagInfo {
        @NonNull
        private final String tagName;
        @NonNull
        private final UUID streamId;

        @Override
        public String toString() {
            return "[" + tagName + ", " + streamId.toString() + "]";
        }
    }
}
