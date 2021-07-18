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
import org.corfudb.runtime.exceptions.TrimmedUpcallException;
import org.corfudb.runtime.object.transactions.AbstractTransactionalContext;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.Address;
import org.corfudb.util.ReflectionUtils;
import org.corfudb.util.Sleep;
import org.corfudb.util.Utils;
import org.corfudb.util.serializer.ISerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.lang.Long.min;

/**
 * In the Corfu runtime, on top of a stream,
 * an SMR object layer implements objects whose history of updates
 * are backed by a stream.
 * <p>
 * <p>This class implements the methods that an in-memory corfu-object proxy carries
 * in order to by in sync with a stream.
 * <p>
 * <p>We refer to the program's object as the -corfu object-,
 * and to the internal object implementation as the -proxy-.
 * <p>
 * <p>If a Corfu object's method is an Accessor, it invokes the proxy's
 * access() method.
 * <p>
 * <p>If a Corfu object's method is a Mutator or Accessor-Mutator, it invokes the
 * proxy's logUpdate() method.
 * <p>
 * <p>Finally, if a Corfu object's method is an Accessor-Mutator,
 * it obtains a result by invoking getUpcallResult().
 * <p>
 * <p>Created by mwei on 11/11/16.
 */
@Slf4j
public class CorfuCompileProxy<T extends ICorfuSMR<T>> implements ICorfuSMRProxyInternal<T> {

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
    final CorfuRuntime rt;

    /**
     * The ID of the stream of the log.
     */
    @SuppressWarnings("checkstyle:abbreviation")
    final UUID streamID;

    /**
     * The type of the underlying object. We use this to instantiate
     * new instances of the underlying object.
     */
    final Class<T> type;

    /**
     * The serializer SMR entries will use to serialize their
     * arguments.
     */
    @Getter
    ISerializer serializer;

    /**
     * Stream tags for streaming transactional updates
     */
    @Getter
    Set<UUID> streamTags;

    /**
     * The arguments this proxy was created with.
     */
    private final Object[] args;

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
     * @param streamTags          Tags applied to the stream
     * @param wrapperObject       The wrapped object
     */
    @SuppressWarnings("checkstyle:abbreviation") // Due to deprecation
    CorfuCompileProxy(CorfuRuntime rt, UUID streamID, Class<T> type, Object[] args,
                      ISerializer serializer, Set<UUID> streamTags, ICorfuSMR<T> wrapperObject) {
        this.rt = rt;
        this.streamID = streamID;
        this.type = type;
        this.args = args;
        this.serializer = serializer;
        this.streamTags = streamTags;

        // Since the VLO is thread safe we don't need to use a thread safe stream implementation
        // because the VLO will control access to the stream
        underlyingObject = new VersionLockedObject<T>(this::getNewInstance,
                new StreamViewSMRAdapter(rt, rt.getStreamsView().getUnsafe(streamID)),
                wrapperObject);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public  <R> R passThrough(Function<T, R> method) {
        return underlyingObject.passThrough(method);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R> R access(ICorfuSMRAccess<R, T> accessMethod,
                        Object[] conflictObject) {
        return MicroMeterUtils.time(() -> accessInner(accessMethod, conflictObject),
                "vlo.read.timer", "streamId", streamID.toString());
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
        AtomicLong timestamp = new AtomicLong(rt.getSequencerView().query(getStreamID()));

        log.debug("Access[{}] conflictObj={} version={}", this, conflictObject, timestamp);

        Function<VersionLockedObject<T>, Boolean> accessChecker = vlo ->
                vlo.getVersionUnsafe() >= timestamp.get() && !vlo.isOptimisticallyModifiedUnsafe();

        // Perform underlying access
        Consumer<VersionLockedObject<T>> updateFunction = vlo -> {
            for (int retry = 0; retry < rt.getParameters().getTrimRetry(); retry++) {
                try {
                    vlo.syncObjectUnsafe(timestamp.get());
                    break;
                } catch (TrimmedException te) {
                    String errMsg = "accessInner: Encountered trimmed address space " +
                            "while accessing version {} of stream {} on attempt {}";
                    log.info(errMsg, timestamp.get(), getStreamID(), retry);

                    vlo.resetUnsafe();

                    if (retry == rt.getParameters().getTrimRetry() - 1) {
                        throw te;
                    }

                    timestamp.set(rt.getSequencerView().query(getStreamID()));
                }
            }
        };

        return underlyingObject.access(accessChecker, updateFunction, accessMethod::access);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long logUpdate(String smrUpdateFunction, final boolean keepUpcallResult,
                          Object[] conflictObject, Object... args) {
        return MicroMeterUtils.time(
                () -> logUpdateInner(smrUpdateFunction, keepUpcallResult, conflictObject, args),
                "vlo.write.timer", "streamId", streamID.toString());
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
                log.warn("Update[{}]", this, e);
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
        VloVersionListener.submit(address);
        return address;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R> R getUpcallResult(long timestamp, Object[] conflictObject) {
        return getUpcallResultInner(timestamp, conflictObject);
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
        if (underlyingObject.getUpcallResults().containsKey(timestamp)) {
            log.trace("Upcall[{}] {} Direct", this, timestamp);
            R ret = (R) underlyingObject.getUpcallResults().get(timestamp);
            underlyingObject.getUpcallResults().remove(timestamp);
            return ret == VersionLockedObject.NullValue.NULL_VALUE ? null : ret;
        }

        return underlyingObject.update(o -> {
            for (int x = 0; x < rt.getParameters().getTrimRetry(); x++) {
                try {
                    o.syncObjectUnsafe(timestamp);
                    if (o.getUpcallResults().containsKey(timestamp)) {
                        log.trace("Upcall[{}] {} Sync'd", this, timestamp);
                        R ret = (R) o.getUpcallResults().get(timestamp);
                        o.getUpcallResults().remove(timestamp);
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
                } catch (TrimmedException ex) {
                    log.info("getUpcallResultInner: Encountered trimmed address space " +
                                    "while accessing version {} of stream {} on attempt {}",
                            timestamp, getStreamID(), x);
                    // We encountered a TRIM during sync, reset the object
                    o.resetUnsafe();
                }
            }

            throw new TrimmedUpcallException(timestamp);
        });
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
        return MicroMeterUtils.time(() -> TXExecuteInner(txFunction),
                "vlo.tx.timer", "streamId", streamID.toString());
    }

    @SuppressWarnings({"checkstyle:membername", "checkstyle:abbreviation"})
    private <R> R TXExecuteInner(Supplier<R> txFunction) {
        // Don't nest transactions if we are already running in a transaction
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
                if (e.getAbortCause() == AbortCause.NETWORK && TransactionalContext.getCurrentContext() != null) {
                    TransactionalContext.getCurrentContext().abortTransaction(e);
                    TransactionalContext.removeContext();
                    throw e;
                }
                log.debug("Transactional function aborted due to {}, retrying after {} msec",
                        e, sleepTime);
                Sleep.sleepUninterruptibly(Duration.ofMillis(sleepTime));
                sleepTime = min(sleepTime * 2L, maxSleepTime);
                retries++;
            } catch (Exception e) {
                log.warn("TXExecute[{}] Abort with Exception: {}", this, e);
                this.abortTransaction(e);
            }
        }
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
            T ret = (T) ReflectionUtils
                    .findMatchingConstructor(type.getDeclaredConstructors(), args);
            if (ret instanceof ICorfuSMRProxyWrapper) {
                ((ICorfuSMRProxyWrapper<T>) ret).setProxy$CORFUSMR(this);
            }
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
}

