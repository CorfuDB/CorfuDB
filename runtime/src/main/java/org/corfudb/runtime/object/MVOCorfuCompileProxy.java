package org.corfudb.runtime.object;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.common.util.ClassUtils;
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
import org.corfudb.runtime.view.ObjectsView;
import org.corfudb.runtime.view.SMRObject.SmrObjectConfig;
import org.corfudb.util.ReflectionUtils;
import org.corfudb.util.Utils;
import org.corfudb.util.serializer.ISerializer;

import java.lang.reflect.InvocationTargetException;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

@Slf4j
public class MVOCorfuCompileProxy<
        S extends SnapshotGenerator<S> & ConsistencyView>
        implements ICorfuSMRProxy<S> {

    @Getter
    private final MultiVersionObject<S> underlyingMVO;

    private final CorfuRuntime rt;

    private final SmrObjectConfig<? extends ICorfuSMR<?>> config;

    public MVOCorfuCompileProxy(
            CorfuRuntime rt, SmrObjectConfig<? extends ICorfuSMR<?>> cfg,
            ICorfuSMR<?> smrTableInstance, MVOCache<S> mvoCache
    ) {
        this.rt = rt;
        this.config = cfg;
        Supplier<S> newInstanceAction = () -> ClassUtils.cast(getNewInstance());

        this.underlyingMVO = new MultiVersionObject<>(
                rt,
                newInstanceAction,
                new StreamViewSMRAdapter(rt, rt.getStreamsView().getUnsafe(getStreamID())),
                smrTableInstance,
                mvoCache,
                config.getOpenOption()
        );
    }

    @Override
    public <R> R access(ICorfuSMRAccess<R, S> accessMethod, Object[] conflictObject) {
        return MicroMeterUtils.time(
                () -> accessInner(accessMethod, conflictObject),
                "mvo.read.timer",
                "streamId",
                getStreamID().toString()
        );
    }

    private <R> R accessInner(ICorfuSMRAccess<R, S> accessMethod,
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
        ICorfuSMRSnapshotProxy<S> snapshotProxy = underlyingMVO.getSnapshotProxy(timestamp);
        final R result = snapshotProxy.access(accessMethod, v -> {});
        snapshotProxy.releaseView();
        return result;
    }

    @Override
    public long logUpdate(String smrUpdateFunction, Object[] conflictObject, Object... args) {
        return MicroMeterUtils.time(
                () -> logUpdateInner(smrUpdateFunction, conflictObject, args),
                "mvo.write.timer", "streamId", getStreamID().toString());
    }

    private long logUpdateInner(String smrUpdateFunction,
                                Object[] conflictObject, Object... args) {

        // If we aren't coming from a transactional context,
        // redirect us to a transactional context first.
        if (TransactionalContext.isInTransaction()) {
            try {
                // We generate an entry to avoid exposing the serializer to the tx context.
                SMREntry entry = new SMREntry(smrUpdateFunction, args, getSerializer());
                return TransactionalContext.getCurrentContext()
                        .logUpdate(this, entry, conflictObject);
            } catch (Exception e) {
                log.warn("Update[{}]", this, e);
                this.abortTransaction(e);
            }
        }

        // If we aren't in a transaction, we can just write the modification.
        // We need to add the acquired token into the pending upcall list.
        SMREntry smrEntry = new SMREntry(smrUpdateFunction, args, getSerializer());
        long address = underlyingMVO.logUpdate(smrEntry);
        log.trace("Update[{}] {}@{} ({}) conflictObj={}",
                this, smrUpdateFunction, address, args, conflictObject);
        return address;
    }

    @Override
    public UUID getStreamID() {
        return config.getStreamName().getId().getId();
    }

    @Override
    public Set<UUID> getStreamTags() {
        return config.getStreamTags();
    }

    private S getNewInstance() {
        try {
            var constructors = config.tableImplementationType().getDeclaredConstructors();
            Object ret = ReflectionUtils.findMatchingConstructor(constructors, config.getArguments());
            return ClassUtils.cast(ret);
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return config.tableImplementationType().getSimpleName() + "[" + Utils.toReadableId(getStreamID()) + "]";
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
            log.error("abortTransaction[{}] Abort Transaction with Exception", this, e);
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
    public boolean isObjectCached() {
        return config.getOpenOption() == ObjectOpenOption.CACHE;
    }

    @Override
    public ISerializer getSerializer() {
        return config.getSerializer();
    }

    @Override
    public void close() {
        // Remove this object from the object cache.
        // This prevents a cached version from being returned in the future.
        rt.getObjectsView().getObjectCache().remove(new ObjectsView.ObjectID(getStreamID(), config.getType()));
        getUnderlyingMVO().close();
    }
}

