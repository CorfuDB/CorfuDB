package org.corfudb.runtime.object;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.StaleObjectVersionException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;

@Slf4j
public class MultiVersionObject<T extends ICorfuSMR<T>> {

    /**
     * A lock, which controls access to modifications to the object.
     * Any access to unsafe methods should obtain the lock.
     */
    private final StampedLock lock;

    /**
     * The stream view this object is backed by.
     */
    private final ISMRStream smrStream;

    /**
     * The upcall map for this object.
     */
    private final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;

    /**
     * A function that generates a new instance of this object.
     */
    private final Supplier<T> newObjectFn;

    /**
     * Metadata on object versions generated.
     */
    private final StreamAddressSpace addressSpace;

    /**
     * Current state of the underlying object. We maintain a reference here explicitly
     * so that a version is always available to sync from.
     *
     * Note: this field is not made volatile since it is only read and assigned to once
     * the write lock is acquired. In Java, all lock implementations must enforce the same
     * memory synchronization semantics as provided by the built-in monitor locks. Hence,
     * releasing the write lock creates the required memory barrier.
     * See: https://stackoverflow.com/questions/45956249/why-dont-we-need-volatile-with-stampedlock
     * and https://docs.oracle.com/javase/8/docs/api/index.html?java/util/concurrent/package-summary.html
     */
    private T object;

    /**
     * All versions up to (and including) materializedUpTo have had their versions
     * materialized in the MVOCache. Since this timestamp corresponds to a materialized
     * version of this stream, it is safe against potential sequencer regressions.
     */
    private volatile long materializedUpTo = Address.NON_EXIST;

    /**
     * The MVOCache used to store and retrieve underlying object versions.
     */
    private final MVOCache<T> mvoCache;

    private final Logger correctnessLogger = LoggerFactory.getLogger("correctness");

    /**
     * Number of times to retry a sync when a TrimmedException is encountered.
     */
    private final int trimRetry;

    private static final String CORRECTNESS_LOG_MSG = "Version, {}";

    /**
     * Create a new MultiVersionObject.
     * @param corfuRuntime  The Corfu runtime containing the MVOCache used to store and
     *                      retrieve versions for this object.
     * @param newObjectFn   A function passed to instantiate a new instance of this object.
     * @param smrStream     The stream View backing this object.
     * @param wrapperObject The wrapper over the actual object.
     */
    public MultiVersionObject(@Nonnull CorfuRuntime corfuRuntime, @Nonnull Supplier<T> newObjectFn,
               @Nonnull StreamViewSMRAdapter smrStream, @Nonnull ICorfuSMR<T> wrapperObject) {
        this.lock = new StampedLock();
        this.smrStream = smrStream;
        this.upcallTargetMap = wrapperObject.getSMRUpcallMap();
        this.newObjectFn = newObjectFn;
        this.addressSpace = new StreamAddressSpace();
        this.object = this.newObjectFn.get();

        this.mvoCache = corfuRuntime.getObjectsView().getMvoCache();
        this.trimRetry = corfuRuntime.getParameters().getTrimRetry();
        this.mvoCache.registerMVO(getID(),this);
        wrapperObject.closeWrapper();
    }

    /**
     * Obtain a snapshot proxy, able to serve accesses and mutations, which contains the most recent state
     * of the object for the provided timestamp.
     * @param timestamp The desired version of the object.
     * @return A snapshot proxy containing the most recent state of the object for the provided timestamp.
     */
    public ICorfuSMRSnapshotProxy<T> getSnapshotProxy(long timestamp) {
        long lockTs = lock.tryOptimisticRead();
        if (lockTs != 0) {
            try {
                if (isTimestampMaterializedUnsafe(timestamp)) {
                    // Find the latest version materialized for this object that is visible from timestamp.
                    final long streamTs = addressSpace.floor(timestamp);
                    final VersionedObjectIdentifier voId = new VersionedObjectIdentifier(getID(), streamTs);

                    if (log.isTraceEnabled()) {
                        log.trace("SnapshotProxy[{}] optimistic request at {}", Utils.toReadableId(getID()), streamTs);
                    }

                    T versionedObject = mvoCache.get(voId)
                            .orElseThrow(() -> new StaleObjectVersionException(getID(), streamTs));

                    if (lock.validate(lockTs)) {
                        correctnessLogger.trace(CORRECTNESS_LOG_MSG, streamTs);
                        return new SnapshotProxy<>(versionedObject, streamTs, upcallTargetMap);
                    }
                }
            } catch (Exception e) {
                // If we have an exception, we didn't get a chance to validate the lock.
                // If it's still valid, then we should re-throw the exception.
                if (lock.validate(lockTs)) {
                    throw e;
                }
            }
        }

        if (log.isTraceEnabled()) {
            log.trace("SnapshotProxy[{}] optimistic request failed - Upgrading write lock", Utils.toReadableId(getID()));
        }

        try {
            lockTs = lock.tryConvertToWriteLock(lockTs);

            if (lockTs == 0) {
                lockTs = lock.writeLock();
            }

            // Check if our timestamp has since been materialized by another thread.
            if (isTimestampMaterializedUnsafe(timestamp)) {
                final long streamTs = addressSpace.floor(timestamp);

                final VersionedObjectIdentifier voId = new VersionedObjectIdentifier(getID(), streamTs);
                T versionedObject = mvoCache.get(voId)
                        .orElseThrow(() -> new StaleObjectVersionException(getID(), streamTs));

                correctnessLogger.trace(CORRECTNESS_LOG_MSG, streamTs);
                return new SnapshotProxy<>(versionedObject, streamTs, upcallTargetMap);
            }

            // If not, perform a sync on the stream.
            for (int x = 0; x < trimRetry; x++) {
                try {
                    syncStreamUnsafe(timestamp);
                    break;
                } catch (TrimmedException te) {
                    log.warn("SnapshotProxy[{}] encountered trimmed addresses {} during sync to {}",
                            Utils.toReadableId(getID()), te.getTrimmedAddresses(), timestamp);

                    resetUnsafe();

                    if (!te.isRetriable() || x == (trimRetry - 1)) {
                        throw te;
                    }
                }
            }

            final long streamTs = addressSpace.getTail();
            if (log.isTraceEnabled()) {
                log.trace("SnapshotProxy[{}] write lock request at {}", Utils.toReadableId(getID()), streamTs);
            }

            correctnessLogger.trace(CORRECTNESS_LOG_MSG, streamTs);
            return new SnapshotProxy<>(object, addressSpace.getTail(), upcallTargetMap);
        } finally {
            lock.unlock(lockTs);
        }
    }

    /**
     * Determine if a particular timestamp/version has been materialized by the MVO.
     * @param timestamp The timestamp that needs determination.
     * @return True if and only if this timestamp has been materialized.
     */
    private boolean isTimestampMaterializedUnsafe(long timestamp) {
        if (timestamp == Address.NON_EXIST && materializedUpTo == Address.NON_EXIST) {
            return false;
        }

        if (timestamp < addressSpace.getFirst()) {
            throw new TrimmedException(timestamp);
        }

        return timestamp <= materializedUpTo;
    }

    /**
     * Sync the SMR stream by playing updates forward in the stream until the given timestamp.
     * Updates are applied on the object through the upcallTargetMap and populated into the MVOCache.
     * Since we don't allow any rollbacks, undo records do not need to be computed and stored.
     * @param timestamp The timestamp to sync up to.
     */
    private void syncStreamUnsafe(long timestamp) {
        if (log.isTraceEnabled()) {
            log.trace("Sync[{}] to {}", Utils.toReadableId(getID()), timestamp);
        }

        Runnable syncStreamRunnable = () ->
                smrStream.streamUpToInList(timestamp)
                        .forEachOrdered(entryList -> {
                            try {
                                if (!entryList.isEmpty()) {
                                    // Apply all updates in a MultiSMREntry, which is treated as one version.
                                    final long globalAddress = entryList.get(0).getGlobalAddress();
                                    entryList.forEach(this::applyUpdateUnsafe);

                                    final VersionedObjectIdentifier voId = new VersionedObjectIdentifier(getID(), globalAddress);

                                    // Populate the new version in the MVOCache and update version metadata.
                                    mvoCache.put(voId, object);
                                    addressSpace.addAddress(globalAddress);

                                    // The globalAddress can be equal to materializedUpTo when processing checkpoint
                                    // entries that consist of multiple continuation entries. These will all share the
                                    // globalAddress of the no-op operation. There is no correctness issue by putting
                                    // these prematurely in the cache, as optimistic reads will be invalid.
                                    Preconditions.checkState(globalAddress >= materializedUpTo,
                                            "globalAddress %s not >= materialized %s", globalAddress, materializedUpTo);

                                    materializedUpTo = globalAddress;
                                }
                            } catch (Exception e) {
                                log.error("Sync[{}] couldn't execute upcall due to {}", Utils.toReadableId(getID()), e);
                                throw new UnrecoverableCorfuError(e);
                            }
                        });

        MicroMeterUtils.time(syncStreamRunnable, "mvo.sync.timer",
                "streamId", getID().toString());
    }

    /**
     * Apply a single SMR entry on the current state of the object.
     * @param updateEntry The SMR entry to apply.
     */
    private void applyUpdateUnsafe(@Nonnull SMREntry updateEntry) {
        if (log.isTraceEnabled()) {
            log.trace("Apply[{}] of {}@{} ({})", Utils.toReadableId(getID()),
                    updateEntry.getSMRMethod(), updateEntry.getGlobalAddress(), updateEntry.getSMRArguments());
        }

        final ICorfuSMRUpcallTarget<T> target = upcallTargetMap.get(updateEntry.getSMRMethod());

        if (target == null) {
            throw new IllegalStateException("Unknown upcall " + updateEntry.getSMRMethod());
        }

        object = (T) target.upcall(object, updateEntry.getSMRArguments());
    }

    /**
     * Get the ID of the stream backing this object.
     * @return The ID of the stream backing this object.
     */
    public UUID getID() {
        return smrStream.getID();
    }

    /**
     * Get the position of the pointer into the SMR stream.
     * @return The position of the pointer into the SMR stream.
     */
    public long getVersionUnsafe() {
        return smrStream.pos();
    }

    /**
     * Run GC on this object.
     *
     * Since the stream that backs this object, and the corresponding version metadata are
     * not thread-safe, synchronization between GC and external access is needed.
     * @param trimMark Perform GC up to this address.
     */
    public void gc(long trimMark) {
        long lockTs = 0;

        try {
            lockTs = lock.writeLock();
            addressSpace.trim(trimMark);
            smrStream.gc(trimMark);
        } finally {
            lock.unlock(lockTs);
        }
    }

    /**
     * Perform a non-transactional update on this object.
     * @param entry The SMR entry to log.
     * @return The address the update was logged at.
     */
    public long logUpdate(@Nonnull SMREntry entry) {
        return smrStream.append(entry,
                t -> true,
                t -> true
        );
    }

    /**
     * Reset the state of this object to an uninitialized state.
     */
    private void resetUnsafe() {
        log.debug("Reset[{}] MVO", Utils.toReadableId(getID()));
        object.close();
        object = newObjectFn.get();
        smrStream.reset();
    }

    @VisibleForTesting
    public ISMRStream getSmrStream() {
        return smrStream;
    }

    @VisibleForTesting
    public StreamAddressSpace getAddressSpace() {
        return addressSpace;
    }
}
