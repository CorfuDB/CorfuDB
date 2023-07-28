package org.corfudb.runtime.object;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.util.ReflectionUtils;
import org.corfudb.util.Utils;
import org.corfudb.util.serializer.ISerializer;

import java.lang.reflect.InvocationTargetException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
public class MVOCorfuCompileProxy<T extends ICorfuSMR<T>> implements ICorfuSMRProxyInternal<T> {

    @Getter
    MultiVersionObject<T> underlyingMVO;

    final CorfuRuntime rt;

    final UUID streamID;

    final Class<T> type;

    @Getter
    ISerializer serializer;

    @Getter
    Set<UUID> streamTags;

    private final Object[] args;

    private final ObjectOpenOption objectOpenOption;

    public MVOCorfuCompileProxy(CorfuRuntime rt, UUID streamID, Class<T> type, Object[] args,
                                ISerializer serializer, Set<UUID> streamTags, ICorfuSMR<T> wrapperObject,
                                ObjectOpenOption objectOpenOption) {
        this.rt = rt;
        this.streamID = streamID;
        this.type = type;
        this.args = args;
        this.serializer = serializer;
        this.streamTags = streamTags;
        this.objectOpenOption = objectOpenOption;

        this.underlyingMVO = new MultiVersionObject<>(
                rt,
                this::getNewInstance,
                new StreamViewSMRAdapter(rt, rt.getStreamsView().getUnsafe(streamID)),
                wrapperObject,
                objectOpenOption);
    }

    @Override
    public <R> R passThrough(Function<T, R> method) {
        return null;
    }

    public static final LongAdder gets = new LongAdder();
    public static final LongAdder puts = new LongAdder();

    @Override
    public <R> R access(ICorfuSMRAccess<R, T> accessMethod, Object[] conflictObject) {
        gets.increment();
        return MicroMeterUtils.time(() -> accessInner(accessMethod, conflictObject),
                "mvo.read.timer", "streamId", streamID.toString());
    }

    private <R> R accessInner(ICorfuSMRAccess<R, T> accessMethod,
                              Object[] conflictObject) {
        if (TransactionalContext.isInTransaction()) {
            try {
                return TransactionalContext.getCurrentContext()
                        .access(this, accessMethod, conflictObject);
            } catch (Exception e) {
                log.error("Access[{}]", this, e);
                this.abortTransaction(e);
            }
        }

        // Linearize this read against a timestamp
        long timestamp = rt.getSequencerView().query(getStreamID());
        log.trace("Access[{}] conflictObj={} version={}", this, conflictObject, timestamp);

        // Perform underlying access
        ICorfuSMRSnapshotProxy<T> snapshotProxy = underlyingMVO.getSnapshotProxy(timestamp);
        return snapshotProxy.access(accessMethod, v -> {});
    }

    @Override
    public long logUpdate(String smrUpdateFunction, boolean keepUpcallResult, Object[] conflictObject, Object... args) {
        puts.increment();
        return MicroMeterUtils.time(
                () -> logUpdateInner(smrUpdateFunction, conflictObject, args),
                "mvo.write.timer", "streamId", streamID.toString());
    }

    private long logUpdateInner(String smrUpdateFunction,
                                Object[] conflictObject, Object... args) {

        // If we aren't coming from a transactional context,
        // redirect us to a transactional context first.
        if (TransactionalContext.isInTransaction()) {
            try {
                // We generate an entry to avoid exposing the serializer to the tx context.
                SMREntry entry = new SMREntry(smrUpdateFunction, args, serializer);
                return TransactionalContext.getCurrentContext()
                        .logUpdate(this, entry, conflictObject);
            } catch (Exception e) {
                log.warn("Update[{}]", this, e);
                this.abortTransaction(e);
            }
        }

        // If we aren't in a transaction, we can just write the modification.
        // We need to add the acquired token into the pending upcall list.
        SMREntry smrEntry = new SMREntry(smrUpdateFunction, args, serializer);
        long address = underlyingMVO.logUpdate(smrEntry);
        log.trace("Update[{}] {}@{} ({}) conflictObj={}",
                this, smrUpdateFunction, address, args, conflictObject);
        return address;
    }

    @Override
    public <R> R getUpcallResult(long timestamp, Object[] conflictObject) {
        return null;
    }

    @Override
    public UUID getStreamID() {
        return streamID;
    }

    @Override
    public <R> R TXExecute(Supplier<R> txFunction) {
        return null;
    }

    @Override
    public Class<T> getObjectType() {
        return type;
    }

    @Override
    public long getVersion() {
        return 0;
    }

    @Override
    public VersionLockedObject<T> getUnderlyingObject() {
        return null;
    }

    private T getNewInstance() {
        try {
            T ret = (T) ReflectionUtils
                    .findMatchingConstructor(type.getDeclaredConstructors(), args);
            return ret;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return type.getSimpleName() + "[" + Utils.toReadableId(streamID) + "]";
    }

    private void abortTransaction(Exception e) {
        final AbstractTransactionalContext context = TransactionalContext.getCurrentContext();
        TransactionalContext.removeContext();

        // Base case: No need to translate, just throw the exception as-is.
        if (e instanceof TransactionAbortedException) {
            throw (TransactionAbortedException) e;
        }

        Token snapshotTimestamp = Token.UNINITIALIZED;
        AbortCause abortCause = AbortCause.UNDEFINED;

        if (e instanceof NetworkException) {
            // If a 'NetworkException' was received within a transactional context, an attempt to
            // 'getSnapshotTimestamp' will also fail (as it requests it to the Sequencer).
            // A new NetworkException would prevent the earliest to be propagated and encapsulated
            // as a TransactionAbortedException.
            abortCause = AbortCause.NETWORK;
        } else if (e instanceof UnsupportedOperationException) {
            snapshotTimestamp = context.getSnapshotTimestamp();
            abortCause = AbortCause.UNSUPPORTED;
        } else if (e instanceof TrimmedException) {
            // Previously, this was handled in AbstractTransactionalContext, but with the addition of the MVO,
            // the retries now occur in the MVO sync. If we reach this point, then the retries hae already occured.
            // Hence, we abort the transaction and mark the cause as TRIM.
            snapshotTimestamp = context.getSnapshotTimestamp();
            abortCause = AbortCause.TRIM;
        } else {
            log.error("abortTransaction[{}] Abort Transaction with Exception {}", this, e);
            snapshotTimestamp = context.getSnapshotTimestamp();
        }

        final TxResolutionInfo txInfo = new TxResolutionInfo(
                context.getTransactionID(), snapshotTimestamp);
        final TransactionAbortedException tae = new TransactionAbortedException(txInfo,
                TokenResponse.NO_CONFLICT_KEY, getStreamID(), Address.NON_ADDRESS,
                abortCause, e, context);
        context.abortTransaction(tae);

        throw tae;
    }

    @Override
    public boolean isMonotonicObject() {
        return false;
    }

    @Override
    public boolean isMonotonicStreamAccess() {
        return true;
    }

    @Override
    public boolean isObjectCached() {
        return objectOpenOption.equals(ObjectOpenOption.CACHE);
    }
}
