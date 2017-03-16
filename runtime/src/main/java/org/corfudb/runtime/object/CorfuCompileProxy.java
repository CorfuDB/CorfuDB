package org.corfudb.runtime.object;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.netty.util.internal.ConcurrentSet;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.ISMRConsumable;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.NoRollbackException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.Utils;
import org.corfudb.util.serializer.ISerializer;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import static java.lang.Long.min;

/**
 * Created by mwei on 11/11/16.
 */
@Slf4j
public class CorfuCompileProxy<T> implements ICorfuSMRProxyInternal<T> {

    /** The underlying object. This object stores the actual
        state as well as the version of the object. It also
        provides locks to access the object safely from a
        multi-threaded context. */
    @Getter
    VersionLockedObject<T> underlyingObject;

    /** The CorfuRuntime. This allows us to interact with the
     * Corfu log.
     */
    CorfuRuntime rt;

    /** The ID of the stream of the log.
     */
    UUID streamID;

    /** The type of the underlying object. We use this to instantiate
     * new instances of the underlying object.
     */
    Class<T> type;

    /** The serializer SMR entries will use to serialize their
     * arguments.
     */
    ISerializer serializer;

    /** A map containing upcall targets. This takes a SMRMethod string
     * and converts it to an upcall function which transitions an object
     * to a new state given arguments.
     */
    @Getter
    final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;

    /** An undo target map. This map contains functions which take a
     * SMRMethod string and arguments and reverses a transition given
     * an undoRecord.
     */
    @Getter
    final Map<String, IUndoFunction<T>> undoTargetMap;

    /** An undo record target map. This map contains functions which
     * take a SMRMethod string and arguments and generates an undoRecord
     * which is used by the undo functions.
     */
    @Getter
    final Map<String, IUndoRecordFunction<T>> undoRecordTargetMap;


    /** The reset set contains a list of methods which reset the object,
     * using their SMRMethod string.
     */
    @Getter
    final Set<String> resetSet;

    /** The arguments this proxy was created with.
     *
     */
    final Object[] args;
    /**
     * Metrics: meter (counter), histogram
     */
    private MetricRegistry metrics = CorfuRuntime.getMetrics();
    private String mpObj = CorfuRuntime.getMpObj();
    private Timer timerAccess = metrics.timer(mpObj + "access");
    private Timer timerLogWrite = metrics.timer(mpObj + "log-write");
    private Timer timerTxn = metrics.timer(mpObj + "txn");
    private Timer timerUpcall = metrics.timer(mpObj + "upcall");
    private Counter counterAccessOptimistic = metrics.counter(mpObj + "access-optimistic");
    private Counter counterAccessLocked = metrics.counter(mpObj + "access-locked");
    private Counter counterTxnRetry1 = metrics.counter(mpObj + "txn-first-retry");
    private Counter counterTxnRetryN = metrics.counter(mpObj + "txn-extra-retries");

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

        this.upcallTargetMap = upcallTargetMap;
        this.undoTargetMap = undoTargetMap;
        this.undoRecordTargetMap = undoRecordTargetMap;
        this.resetSet = resetSet;

        underlyingObject = getNewVersionLockedObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R> R access(ICorfuSMRAccess<R, T> accessMethod,
                        Object[] conflictObject) {
        boolean isEnabled = MetricsUtils.isMetricsCollectionEnabled();
        try (Timer.Context context = MetricsUtils.getConditionalContext(isEnabled, timerAccess)){
            return accessInner(accessMethod, conflictObject, isEnabled);
        }
    }

    private <R> R accessInner(ICorfuSMRAccess<R, T> accessMethod,
                              Object[] conflictObject, boolean isMetricsEnabled) {
        if (TransactionalContext.isInTransaction()) {
            return TransactionalContext.getCurrentContext()
                    .access(this, accessMethod, conflictObject);
        }

        // Linearize this read against a timestamp
        final long timestamp =
                rt.getSequencerView()
                .nextToken(Collections.singleton(streamID), 0).getToken();
        log.trace("Access[{}] Linearized to {}", this, timestamp);

        // Acquire locks and perform read.
        return underlyingObject.optimisticallyReadThenReadLockThenWriteOnFail(
             (ver,o) -> {
            // If not in a transaction, check if the version is
            // at least that of the linearized read requested.
            if (ver >= timestamp
                    && !o.isOptimisticallyModifiedUnsafe()) {
                MetricsUtils.incConditionalCounter(isMetricsEnabled, counterAccessOptimistic, 1);
                log.trace("Access [{}] Direct (readlock) access at {}", this, ver);
                return accessMethod.access(o.getObjectUnsafe());
            }
            // We don't have the right version, so we need to write
            // throwing this exception causes us to take a write lock.
            MetricsUtils.incConditionalCounter(isMetricsEnabled, counterAccessLocked, 1);
            throw new ConcurrentModificationException();
        },
            //  The read did not acquire the right version, so we
            //  have now acquired a write lock.
        (ver, o) -> {
            // In the off chance that someone updated the version
            // for us.
            if (ver >= timestamp
                    && !o.isOptimisticallyModifiedUnsafe()) {
                log.trace("Access [{}] Direct (writelock) access at {}", this, ver);
                return accessMethod.access(underlyingObject.getObjectUnsafe());
            }

            // Now we sync forward while we have the lock, since we don't have
            // the right version still.
            o.syncObjectUnsafe(timestamp);
            log.trace("Access [{}] Sync'd (writelock) access at {}", this, ver);
            return accessMethod.access(o.getObjectUnsafe());
        });
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
            // We generate an entry to avoid exposing the serializer to the tx context.
            SMREntry entry = new SMREntry(smrUpdateFunction, args, serializer);
            return TransactionalContext.getCurrentContext()
                    .logUpdate(this, entry, conflictObject);
        }


        // If we aren't in a transaction, we can just write the modification.
        // We need to add the acquired token into the pending upcall list.
        SMREntry smrEntry = new SMREntry(smrUpdateFunction, args, serializer);
        long address = underlyingObject.logUpdate(smrEntry, keepUpcallResult);
        log.trace("LogUpdate[{}] {}@{} ({}) conflictObj={}",
                this, smrUpdateFunction, address, args, conflictObject);
        return address;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
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
                        .nextToken(Collections.singleton(streamID), 0).getToken();

        log.debug("sync this object {} to timestamp={}", getStreamID()
                .getLeastSignificantBits(), timestamp);

        // Acquire locks and perform read.
        underlyingObject.writeReturnVoid((v,o) -> o.syncObjectUnsafe(timestamp));
    }

    private <R> R getUpcallResultInner(long timestamp, Object[] conflictObject) {
        // If we aren't coming from a transactional context,
        // redirect us to a transactional context first.
        if (TransactionalContext.isInTransaction()) {
            return (R) TransactionalContext.getCurrentContext()
                    .getUpcallResult(this, timestamp, conflictObject);
        }

        // Check first if we have the upcall, if we do
        // we can service the request right away.
        if (underlyingObject.upcallResults.containsKey(timestamp)) {
            log.trace("Upcall[{}] {} Direct", this, timestamp);
            R ret = (R) underlyingObject.upcallResults.get(timestamp);
            underlyingObject.upcallResults.remove(timestamp);
            return ret == VersionLockedObject.NullValue.NULL_VALUE ? null : ret;
        }

        // if someone took a writelock on the object, we
        // should just wait for it, since the object will
        // have our upcall after...
        if (underlyingObject.isWriteLocked()) {
            underlyingObject.waitOnLock();
            if (underlyingObject.upcallResults.containsKey(timestamp)) {
                log.trace("Upcall[{}] {} Post-Lock", this, timestamp);
                R ret = (R) underlyingObject.upcallResults.get(timestamp);
                underlyingObject.upcallResults.remove(timestamp);
                return ret == VersionLockedObject.NullValue.NULL_VALUE ? null : ret;
            }
        }

        // Otherwise we need to sync the object by taking
        // the correct locks.
        underlyingObject.writeReturnVoid(
                (v, o) -> {
                    o.syncObjectUnsafe(timestamp);});

        if (underlyingObject.upcallResults.containsKey(timestamp)) {
            log.trace("Upcall[{}] {} Sync'd", this,  timestamp);
            R ret = (R) underlyingObject.upcallResults.get(timestamp);
            underlyingObject.upcallResults.remove(timestamp);
            return ret == VersionLockedObject.NullValue.NULL_VALUE ? null : ret;
        }

        // The version is already ahead, but we don't have the result.
        // The only way to get the correct result
        // of the upcall would be to rollback. For now, we throw an exception
        // since this is generally not expected. --- and probably a bug if it happens.
        throw new RuntimeException("Attempted to get the result " +
                "of an upcall@" + timestamp + " but we are @"
                + underlyingObject.getVersionUnsafe() +
                " and we don't have a copy");
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

    private <R> R TXExecuteInner(Supplier<R> txFunction, boolean isMetricsEnabled) {
        long sleepTime = 1L;
        int retries = 1;
        while (true) {
            try {
                rt.getObjectsView().TXBegin();
                R ret = txFunction.get();
                rt.getObjectsView().TXEnd();
                return ret;
            } catch (TransactionAbortedException e) {
                if (retries == 1) {
                    MetricsUtils.incConditionalCounter(isMetricsEnabled, counterTxnRetry1, 1);
                }
                MetricsUtils.incConditionalCounter(isMetricsEnabled, counterTxnRetryN, 1);
                log.debug("Transactional function aborted due to {}, retrying after {} msec", e, sleepTime);
                try {Thread.sleep(sleepTime); }
                catch (Exception ex) {}
                sleepTime = min(sleepTime * 2L, 1000L);
                retries++;
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
        return underlyingObject.getVersionUnsafe();
    }


    /**
     * Get a new version locked object, with a fresh stream view.
     * @return  A new version locked object.
     */
    @SuppressWarnings("unchecked")
    private VersionLockedObject<T> getNewVersionLockedObject() {
        try {
            return new VersionLockedObject<T>(this::getNewInstance,
                    new StreamViewSMRAdapter(rt, rt.getStreamsView().get(streamID)),
                    getUpcallTargetMap(), getUndoRecordTargetMap(),
                    getUndoTargetMap(), getResetSet());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Get a new instance of the real underlying object.
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
        return type.getSimpleName() + "[" + Utils.toReadableID(streamID) + "]";
    }
}
