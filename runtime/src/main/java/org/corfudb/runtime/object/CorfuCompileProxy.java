package org.corfudb.runtime.object;

import static java.lang.Long.min;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.TrimmedUpcallException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.Sleep;
import org.corfudb.util.Utils;
import org.corfudb.util.serializer.ISerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In the Corfu runtime, on top of a stream,
 * an SMR object layer implements objects whose history of updates
 * are backed by a stream.
 *
 * <p>This class implements the methods that an in-memory corfu-object proxy carries
 * in order to by in sync with a stream.
 *
 * <p>We refer to the program's object as the -corfu object-,
 * and to the internal object implementation as the -proxy-.
 *
 * <p>If a Corfu object's method is an Accessor, it invokes the proxy's
 * access() method.
 *
 * <p>If a Corfu object's method is a Mutator or Accessor-Mutator, it invokes the
 * proxy's logUpdate() method.
 *
 * <p>Finally, if a Corfu object's method is an Accessor-Mutator,
 * it obtains a result by invoking getUpcallResult().
 *
 * <p>Created by mwei on 11/11/16.
 */
@Slf4j
public class CorfuCompileProxy<T> implements ICorfuSMRProxyInternal<T> {

    /**
     * The underlying object. This object stores the actual
     * state as well as the version of the object. It also
     * provides locks to access the object safely from a
     * multi-threaded context.
     */
    @Getter
    VersionLockedObject<T> underlyingObject;

    /**
     * The CorfuRuntime. This allows us to interact with the
     * Corfu log.
     */
    CorfuRuntime rt;

    /**
     * The ID of the stream of the log.
     */
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
    UUID streamID;

    /**
     * The type of the underlying object. We use this to instantiate
     * new instances of the underlying object.
     */
    Class<T> type;

    /**
     * The serializer SMR entries will use to serialize their
     * arguments.
     */
    @Getter
    ISerializer serializer;

    /**
     * The arguments this proxy was created with.
     */
    final Object[] args;

    private final MetricRegistry metrics;
    /**
     * Metrics: meter (counter), histogram.
     */
    private final String mpObj;
    private final Timer timerAccess;
    private final Timer timerLogWrite;
    private final Timer timerTxn;
    private final Timer timerUpcall;
    private final Counter counterAccessOptimistic;
    private final Counter counterAccessLocked;
    private final Counter counterTxnRetry1;
    private final Counter counterTxnRetryN;

    /**
     * Correctness Logging
     */
    private final Logger correctnessLogger = LoggerFactory.getLogger("correctness");

    /**
     * Creates a CorfuCompileProxy object on a particular stream.
     *
     * @param rt                  Connected CorfuRuntime instance.
     * @param streamID            StreamID of the log.
     * @param type                Type of underlying object to instantiate a new instance.
     * @param args                Arguments to create this proxy.
     * @param serializer          Serializer used by the SMR entries to serialize the arguments.
     * @param upcallTargetMap     upCallTargetMap
     * @param undoTargetMap       undoTargetMap
     * @param undoRecordTargetMap undoRecordTargetMap
     * @param resetSet            resetSet
     */
    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
    public CorfuCompileProxy(CorfuRuntime rt, UUID streamID, Class<T> type, Object[] args,
                             ISerializer serializer,
                             Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap,
                             Map<String, IUndoFunction<T>> undoTargetMap,
                             Map<String, IUndoRecordFunction<T>> undoRecordTargetMap,
                             Set<String> resetSet
    ) {
        this.rt = rt;
        this.streamID = streamID;
        this.type = type;
        this.args = args;
        this.serializer = serializer;

        underlyingObject = new VersionLockedObject<T>(this::getNewInstance,
                new StreamViewSMRAdapter(rt, rt.getStreamsView().get(streamID)),
                upcallTargetMap, undoRecordTargetMap,
                undoTargetMap, resetSet);

        metrics = rt.getMetrics() != null ? rt.getMetrics() : CorfuRuntime.getDefaultMetrics();
        mpObj = CorfuRuntime.getMpObj();
        timerAccess = metrics.timer(mpObj + "access");
        timerLogWrite = metrics.timer(mpObj + "log-write");
        timerTxn = metrics.timer(mpObj + "txn");
        timerUpcall = metrics.timer(mpObj + "upcall");
        counterAccessOptimistic = metrics.counter(mpObj + "access-optimistic");
        counterAccessLocked = metrics.counter(mpObj + "access-locked");
        counterTxnRetry1 = metrics.counter(mpObj + "txn-first-retry");
        counterTxnRetryN = metrics.counter(mpObj + "txn-extra-retries");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R> R access(ICorfuSMRAccess<R, T> accessMethod,
                        Object[] conflictObject) {
        boolean isEnabled = MetricsUtils.isMetricsCollectionEnabled();
        try (Timer.Context context = MetricsUtils.getConditionalContext(isEnabled, timerAccess)) {
            return accessInner(accessMethod, conflictObject, isEnabled);
        }
    }

    private <R> R accessInner(ICorfuSMRAccess<R, T> accessMethod,
                              Object[] conflictObject, boolean isMetricsEnabled) {
        if (TransactionalContext.isInTransaction()) {
            try {
                return TransactionalContext.getCurrentContext()
                        .access(this, accessMethod, conflictObject);
            } catch (Exception e) {
                log.error("Access[{}] Exception: {}", this, e);
                this.abortTransaction(e);
            }
        }

        // Linearize this read against a timestamp
        final long timestamp =
                rt.getSequencerView()
                        .nextToken(Collections.singleton(streamID), 0).getToken()
                        .getTokenValue();
        log.debug("Access[{}] conflictObj={} version={}", this, conflictObject, timestamp);
        correctnessLogger.trace("Version, {}", timestamp);

        // Perform underlying access
        try {
            return underlyingObject.access(o -> o.getVersionUnsafe() >= timestamp
                            && !o.isOptimisticallyModifiedUnsafe(),
                    o -> o.syncObjectUnsafe(timestamp),
                    o -> accessMethod.access(o));
        } catch (TrimmedException te) {
            log.warn("Access[{}] Encountered Trim, reset and retry", this);
            // We encountered a TRIM during sync, reset the object
            underlyingObject.update(o -> {
                o.resetUnsafe();
                return null;
            });
            // And attempt an access again.
            return accessInner(accessMethod, conflictObject, isMetricsEnabled);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long logUpdate(String smrUpdateFunction, final boolean keepUpcallResult,
                          Object[] conflictObject, Object... args) {
        try (Timer.Context context = MetricsUtils.getConditionalContext(timerLogWrite)) {
            return logUpdateInner(smrUpdateFunction, keepUpcallResult, conflictObject, args);
        }
    }

    private long logUpdateInner(String smrUpdateFunction, final boolean keepUpcallResult,
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
                log.warn("Update[{}] Exception: {}", this, e);
                this.abortTransaction(e);
            }
        }

        // If we aren't in a transaction, we can just write the modification.
        // We need to add the acquired token into the pending upcall list.
        SMREntry smrEntry = new SMREntry(smrUpdateFunction, args, serializer);
        long address = underlyingObject.logUpdate(smrEntry, keepUpcallResult);
        log.trace("Update[{}] {}@{} ({}) conflictObj={}",
                this, smrUpdateFunction, address, args, conflictObject);
        correctnessLogger.trace("Version, {}", address);
        return address;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R> R getUpcallResult(long timestamp, Object[] conflictObject) {
        try (Timer.Context context = MetricsUtils.getConditionalContext(timerUpcall);) {
            return getUpcallResultInner(timestamp, conflictObject);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sync() {
        // Linearize this read against a timestamp
        final long timestamp =
                rt.getSequencerView()
                        .nextToken(Collections.singleton(streamID), 0).getToken()
                        .getTokenValue();

        log.debug("Sync[{}] {}", this, timestamp);

        // Acquire locks and perform read.
        underlyingObject.update(o -> {
            o.syncObjectUnsafe(timestamp);
            return null;
        });
    }

    private <R> R getUpcallResultInner(long timestamp, Object[] conflictObject) {
        // If we aren't coming from a transactional context,
        // redirect us to a transactional context first.
        if (TransactionalContext.isInTransaction()) {
            try {
                return (R) TransactionalContext.getCurrentContext()
                        .getUpcallResult(this, timestamp, conflictObject);
            } catch (Exception e) {
                log.warn("UpcallResult[{}] Exception: {}", this, e);
                this.abortTransaction(e);
            }
        }

        // Check first if we have the upcall, if we do
        // we can service the request right away.
        if (underlyingObject.upcallResults.containsKey(timestamp)) {
            log.trace("Upcall[{}] {} Direct", this, timestamp);
            R ret = (R) underlyingObject.upcallResults.get(timestamp);
            underlyingObject.upcallResults.remove(timestamp);
            return ret == VersionLockedObject.NullValue.NULL_VALUE ? null : ret;
        }

        try {
            return underlyingObject.update(o -> {
                o.syncObjectUnsafe(timestamp);
                if (o.upcallResults.containsKey(timestamp)) {
                    log.trace("Upcall[{}] {} Sync'd", this, timestamp);
                    R ret = (R) o.upcallResults.get(timestamp);
                    o.upcallResults.remove(timestamp);
                    return ret == VersionLockedObject.NullValue.NULL_VALUE ? null : ret;
                }

                // The version is already ahead, but we don't have the result.
                // The only way to get the correct result
                // of the upcall would be to rollback. For now, we throw an exception
                // since this is generally not expected. --- and probably a bug if it happens.
                throw new RuntimeException("Attempted to get the result "
                        + "of an upcall@" + timestamp + " but we are @"
                        + underlyingObject.getVersionUnsafe()
                        + " and we don't have a copy");
            });
        } catch (TrimmedException ex) {
            throw new TrimmedUpcallException(timestamp);
        }
    }

    /**
     * Get the ID of the stream this proxy is subscribed to.
     *
     * @return The UUID of the stream this proxy is subscribed to.
     */
    @Override
    public UUID getStreamID() {
        return streamID;
    }

    /**
     * Run in a transactional context.
     *
     * @param txFunction The function to run in a transactional context.
     * @return The value supplied by the function.
     */
    @Override
    public <R> R TXExecute(Supplier<R> txFunction) {
        boolean isEnabled = MetricsUtils.isMetricsCollectionEnabled();
        try (Timer.Context context = MetricsUtils.getConditionalContext(isEnabled, timerTxn)) {
            return TXExecuteInner(txFunction, isEnabled);
        }
    }

    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings({"checkstyle:membername", "checkstyle:abbreviation"}) // Due to deprecation
    private <R> R TXExecuteInner(Supplier<R> txFunction, boolean isMetricsEnabled) {
        // Don't nest transactions if we are already running transactionally
        if (TransactionalContext.isInTransaction()) {
            try {
                return txFunction.get();
            } catch (Exception e) {
                log.warn("TXExecute[{}] Abort with Exception: {}", this, e);
                this.abortTransaction(e);
            }
        }
        long sleepTime = 1L;
        final long maxSleepTime = 1000L;
        int retries = 1;
        while (true) {
            try {
                rt.getObjectsView().TXBegin();
                R ret = txFunction.get();
                rt.getObjectsView().TXEnd();
                return ret;
            } catch (TransactionAbortedException e) {
                // If TransactionAbortedException is due to a 'Network Exception' do not keep
                // retrying a nested transaction indefinitely (this could go on forever).
                // If this is part of an outer transaction abort and remove from context.
                // Re-throw exception to client.
                log.warn("TXExecute[{}] Abort with exception {}", this, e);
                if (e.getAbortCause() == AbortCause.NETWORK) {
                    if (TransactionalContext.getCurrentContext() != null) {
                        TransactionalContext.getCurrentContext().abortTransaction(e);
                        TransactionalContext.removeContext();
                        throw e;
                    }
                }

                if (retries == 1) {
                    MetricsUtils
                            .incConditionalCounter(isMetricsEnabled, counterTxnRetry1, 1);
                }
                MetricsUtils.incConditionalCounter(isMetricsEnabled, counterTxnRetryN, 1);
                log.debug("Transactional function aborted due to {}, retrying after {} msec",
                        e, sleepTime);
                Sleep.MILLISECONDS.sleepUninterruptibly(sleepTime);
                sleepTime = min(sleepTime * 2L, maxSleepTime);
                retries++;
            } catch (Exception e) {
                log.warn("TXExecute[{}] Abort with Exception: {}", this, e);
                this.abortTransaction(e);
            }
        }
    }

    /**
     * Get an object builder to build new objects.
     *
     * @return An object which permits the construction of new objects.
     */
    @Override
    public IObjectBuilder<?> getObjectBuilder() {
        return rt.getObjectsView().build();
    }

    /**
     * Return the type of the object being replicated.
     *
     * @return The type of the replicated object.
     */
    @Override
    public Class<T> getObjectType() {
        return type;
    }

    /**
     * Get the latest version read by the proxy.
     *
     * @return The latest version read by the proxy.
     */
    @Override
    public long getVersion() {
        return access(o -> underlyingObject.getVersionUnsafe(),
                null);
    }

    /**
     * Get a new instance of the real underlying object.
     *
     * @return An instance of the real underlying object
     */
    @SuppressWarnings("unchecked")
    private T getNewInstance() {
        try {
            T ret = null;
            if (args == null || args.length == 0) {
                ret = type.newInstance();
            } else {
                // This loop is not ideal, but the easiest way to get around Java boxing,
                // which results in primitive constructors not matching.
                for (Constructor<?> constructor : type.getDeclaredConstructors()) {
                    try {
                        ret = (T) constructor.newInstance(args);
                        break;
                    } catch (Exception e) {
                        // just keep trying until one works.
                    }
                }
            }
            if (ret instanceof ICorfuSMRProxyWrapper) {
                ((ICorfuSMRProxyWrapper<T>) ret).setProxy$CORFUSMR(this);
            }
            return ret;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return type.getSimpleName() + "[" + Utils.toReadableId(streamID) + "]";
    }

    private void abortTransaction(Exception e) {
        long snapshotTimestamp;
        AbortCause abortCause;
        TransactionAbortedException tae;

        AbstractTransactionalContext context = TransactionalContext.getCurrentContext();

        if (e instanceof TransactionAbortedException) {
            tae = (TransactionAbortedException) e;
        } else {
            if (e instanceof NetworkException) {
                // If a 'NetworkException' was received within a transactional context, an attempt to
                // 'getSnapshotTimestamp' will also fail (as it requests it to the Sequencer).
                // A new NetworkException would prevent the earliest to be propagated and encapsulated
                // as a TransactionAbortedException.
                snapshotTimestamp = -1L;
                abortCause = AbortCause.NETWORK;
            } else if (e instanceof UnsupportedOperationException) {
                snapshotTimestamp = context.getSnapshotTimestamp();
                abortCause = AbortCause.UNSUPPORTED;
            } else {
                log.error("abortTransaction[{}] Abort Transaction with Exception {}", this, e);
                snapshotTimestamp = context.getSnapshotTimestamp();
                abortCause = AbortCause.UNDEFINED;
            }

            TxResolutionInfo txInfo = new TxResolutionInfo(
                    context.getTransactionID(), snapshotTimestamp);
            tae = new TransactionAbortedException(txInfo, null, getStreamID(),
                    abortCause, e, context);
            context.abortTransaction(tae);
        }

        TransactionalContext.removeContext();
        throw tae;
    }
}
