package org.corfudb.runtime.object;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;

/**
 * Provides the necessary synchronization primitives for allowing safe access to the SMR
 * object in a multi-threaded context. SMR objects should be immutable and persistent.
 * @param <T> The type of the underlying SMR object.
 *
 * Created by jielu, munshedm, and zfrenette.
 */
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
     * A fixed instance of a new instance of this object.
     */
    private final T newInstanceObject;

    private final ObjectOpenOption objectOpenOption;

    /**
     * Metadata on object versions generated.
     */
    private volatile StreamAddressSpace addressSpace;

    /**
     * Current state of the underlying object. We maintain a reference here explicitly
     * so that a version is always available to sync from, as it is possible that the
     * MVOCache has no versions associated with this underlying object.
     */
    private volatile T currentObject;

    private volatile InstrumentedSMRSnapshot<T> currentInstrumentedObject;

    /**
     * All versions up to (and including) materializedUpTo have had their versions
     * materialized in the MVOCache. This value is always in sync with currentObject,
     * even after a resetUnsafe operation. Since this timestamp corresponds to a
     * materialized version of this stream, it is safe against potential sequencer
     * regressions.
     *
     * Note: There is no guarantee that the materializedUpTo version is always
     * present within the MVOCache. It only indicates that this is the most recent
     * version materialized by the MVO.
     */
    private volatile long materializedUpTo = Address.NON_ADDRESS;

    /**
     * Used for optimization purposes, this value is similar to materializedUpTo.
     * However, this timestamp is not safe against sequencer regressions.
     *
     * This should eventually be made epoch aware.
     */
    private volatile long resolvedUpTo = Address.NON_ADDRESS;

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

    private static final String STREAM_ID_TAG_NAME = "streamId";

    /**
     * Create a new MultiVersionObject.
     * @param corfuRuntime  The Corfu runtime containing the MVOCache used to store and
     *                      retrieve versions for this object.
     * @param newObjectFn   A function passed to instantiate a new instance of this object.
     * @param smrStream     The stream View backing this object.
     * @param wrapperObject The wrapper over the actual object.
     */
    public MultiVersionObject(@Nonnull CorfuRuntime corfuRuntime, @Nonnull Supplier<T> newObjectFn,
                              @Nonnull StreamViewSMRAdapter smrStream, @Nonnull ICorfuSMR<T> wrapperObject,
                              @Nonnull ObjectOpenOption objectOpenOption) {
        this.lock = new StampedLock();
        this.smrStream = smrStream;
        this.upcallTargetMap = wrapperObject.getSMRUpcallMap();
        this.newObjectFn = newObjectFn;
        this.objectOpenOption = objectOpenOption;
        this.addressSpace = new StreamAddressSpace();

        this.newInstanceObject = newObjectFn.get();
        this.currentObject = newObjectFn.get();
        this.currentInstrumentedObject = new InstrumentedSMRSnapshot<>(currentObject);

        this.mvoCache = corfuRuntime.getObjectsView().getMvoCache();
        this.trimRetry = corfuRuntime.getParameters().getTrimRetry();
        wrapperObject.closeWrapper();
    }

    /**
     * Obtain a snapshot proxy, able to serve accesses and mutations, which contains the most recent state
     * of the object for the provided timestamp.
     * @param timestamp The desired version of the object.
     * @return A snapshot proxy containing the most recent state of the object for the provided timestamp.
     */
    public ICorfuSMRSnapshotProxy<T> getSnapshotProxy(long timestamp) {
        // A timestamp of Address.NON_EXIST is currently only returned for a stream tail
        // query of a non-transactional access - The instance will always be without updates.
        // A timestamp of Address.NON_ADDRESS corresponds to a global tail that sees no
        // updates. In both cases, we can serve the snapshot immediately.
        if (timestamp == Address.NON_EXIST || timestamp == Address.NON_ADDRESS) {
            return new SnapshotProxy<>(newInstanceObject, timestamp, upcallTargetMap);
        }

        final VersionedObjectIdentifier voId = new VersionedObjectIdentifier(getID(), timestamp);
        long lockTs = lock.tryOptimisticRead();
        if (lockTs != 0) {
            try {
                Optional<ICorfuSMRSnapshotProxy<T>> snapshot = getFromCacheUnsafe(voId, lockTs);
                if (snapshot.isPresent()) {
                    // Lock was validated within getFromCacheUnsafe.
                    return snapshot.get();
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
                long startTime = System.nanoTime();
                lockTs = lock.writeLock();
                MicroMeterUtils.time(Duration.ofNanos(System.nanoTime() - startTime),
                        "mvo.lock.wait", STREAM_ID_TAG_NAME, getID().toString());
            }

            // Check if our timestamp has since been materialized by another thread.
            Optional<ICorfuSMRSnapshotProxy<T>> snapshot = getFromCacheUnsafe(voId, lockTs);
            if (snapshot.isPresent()) {
                return snapshot.get();
            }

            // If not, perform a sync on the stream.
            for (int x = 0; x < trimRetry; x++) {
                try {
                    syncStreamUnsafe(timestamp);
                    break;
                } catch (TrimmedException te) {
                    log.warn("SnapshotProxy[{}] encountered trimmed addresses {} during sync to {} on attempt {} of {}",
                            Utils.toReadableId(getID()), te.getTrimmedAddresses(), timestamp, x + 1, trimRetry);

                    resetUnsafe();

                    if (!te.isRetriable() || x == (trimRetry - 1)) {
                        throw te;
                    }
                } catch (Exception ex) {
                    log.warn("SnapshotProxy[{}] encountered an exception during sync to {} on attempt {} of {}",
                            Utils.toReadableId(getID()), timestamp, x + 1, trimRetry, ex);

                    resetUnsafe();
                    throw ex;
                }
            }

            resolvedUpTo = Long.max(timestamp, resolvedUpTo);

            final long streamTs = addressSpace.getTail();
            if (log.isTraceEnabled()) {
                log.trace("SnapshotProxy[{}] write lock request at {}", Utils.toReadableId(getID()), streamTs);
            }

            updateSMRSnapshotMetrics(currentInstrumentedObject, false);

            correctnessLogger.trace(CORRECTNESS_LOG_MSG, streamTs);
            return new SnapshotProxy<>(currentInstrumentedObject.getSnapshot(), streamTs, upcallTargetMap);
        } finally {
            lock.unlock(lockTs);
        }
    }

    private void updateSMRSnapshotMetrics(@NonNull InstrumentedSMRSnapshot<T> snapshot, boolean fromCache) {
        if (fromCache) {
            snapshot.getMetrics().requestedWhileCached();
        }
        snapshot.getMetrics().setLastAccessedTs(System.nanoTime());
    }

    /**
     * Determine if a particular timestamp/version has been materialized by the MVO.
     * @param timestamp The timestamp that needs determination.
     * @return True if and only if this timestamp has been materialized.
     */
    private boolean isTimestampMaterializedUnsafe(long timestamp) {
        if (timestamp < addressSpace.getFirst()) {
            // Special handling here for tables without updates, since we cannot insert
            // Address.NON_ADDRESS into the StreamAddressSpace metadata.
            if (addressSpace.getTrimMark() == Address.NON_ADDRESS) {
                return true;
            }

            throw new TrimmedException(timestamp,
                    String.format("Trimmed address: %s. StreamAddressSpace: %s.", timestamp, addressSpace.toString()));
        }

        return timestamp <= resolvedUpTo && addressSpace.size() != 0;
    }

    /**
     * Attempt to retrieve a particular version of this object, if available.
     * @param voId   The desired version of the object.
     * @param lockTs The stamp previously obtained from the stamped lock.
     * @return If available, a snapshot proxy containing the most recent state of the object for
     * the provided timestamp.
     */
    private Optional<ICorfuSMRSnapshotProxy<T>> getFromCacheUnsafe(@Nonnull VersionedObjectIdentifier voId, long lockTs) {
        long startTime = System.nanoTime();

        try {
            if (isTimestampMaterializedUnsafe(voId.getVersion())) {
                // Find the latest version materialized for this object that is visible from timestamp.
                final long streamTs = addressSpace.floor(voId.getVersion());
                voId.setVersion(streamTs);

                if (log.isTraceEnabled()) {
                    log.trace("SnapshotProxy[{}] optimistic request at {}", Utils.toReadableId(getID()), streamTs);
                }

                InstrumentedSMRSnapshot<T> versionedObject;
                boolean fromCache = true;

                // The latest visible version for the provided timestamp is equal to what is maintained
                // by the MVO. In this case, there is no need to query the cache. This check is required
                // since the MVOCache might have evicted all versions associated with this object, even
                // though streamTs is the most recent tail for this stream.
                if (streamTs == materializedUpTo) {
                    versionedObject = currentInstrumentedObject;
                    fromCache = false;
                } else {
                    versionedObject = mvoCache.get(voId).orElseThrow(() -> new TrimmedException(streamTs,
                            String.format("Trimmed address %s has been evicted from MVOCache. StreamAddressSpace: %s.",
                                    streamTs, addressSpace.toString())));
                }

                if (lock.validate(lockTs)) {
                    correctnessLogger.trace(CORRECTNESS_LOG_MSG, streamTs);
                    updateSMRSnapshotMetrics(versionedObject, fromCache);
                    return Optional.of(new SnapshotProxy<>(versionedObject.getSnapshot(), streamTs, upcallTargetMap));
                }
            }

            return Optional.empty();
        } finally {
            MicroMeterUtils.time(Duration.ofNanos(System.nanoTime() - startTime), "mvo.cache.query");
        }

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
                .forEachOrdered(addressUpdates -> {
                    try {
                        // Apply all updates in a MultiSMREntry, which is treated as one version.
                        final long globalAddress = addressUpdates.getGlobalAddress();

                        // The globalAddress can be equal to materializedUpTo when processing checkpoint
                        // entries that consist of multiple continuation entries. These will all share the
                        // globalAddress of the no-op operation. There is no correctness issue by prematurely
                        // updating version information, as optimistic reads will be invalid.
                        Preconditions.checkState(globalAddress >= materializedUpTo,
                                "globalAddress %s not >= materialized %s", globalAddress, materializedUpTo);

                        // Perform similar validation for resolvedUpTo.
                        if (globalAddress < resolvedUpTo) {
                            log.warn("Sync[{}]: globalAddress {} not >= resolved {}",
                                    Utils.toReadableId(getID()), globalAddress, resolvedUpTo);
                            throw new TrimmedException();
                        }

                        // If we observe a new version, place the previous one into the MVOCache.
                        if (globalAddress > materializedUpTo && objectOpenOption == ObjectOpenOption.CACHE) {
                            final VersionedObjectIdentifier voId = new VersionedObjectIdentifier(getID(), materializedUpTo);
                            mvoCache.put(voId, currentInstrumentedObject);
                        }

                        // In the case where addressUpdates corresponds to a HOLE, getSmrEntryList() will
                        // produce an empty list and the below will be a no-op. This means that there can
                        // be multiple versions that correspond to the same exact object.
                        addressUpdates.getSmrEntryList().forEach(this::applyUpdateUnsafe);
                        currentInstrumentedObject = new InstrumentedSMRSnapshot<>(currentObject);
                        addressSpace.addAddress(globalAddress);
                        materializedUpTo = globalAddress;
                        resolvedUpTo = globalAddress;
                    } catch (TrimmedException e) {
                        // The caller catches this TrimmedException and resets the object before retrying.
                        throw e;
                    } catch (Exception e) {
                        log.error("Sync[{}] couldn't execute upcall due to {}", Utils.toReadableId(getID()), e);
                        throw new UnrecoverableCorfuError(e);
                    }
                });

        MicroMeterUtils.time(syncStreamRunnable, "mvo.sync.timer", STREAM_ID_TAG_NAME, getID().toString());
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

        currentObject = (T) target.upcall(currentObject, updateEntry.getSMRArguments());
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
        currentObject.close();
        currentObject = newObjectFn.get();
        currentInstrumentedObject = new InstrumentedSMRSnapshot<>(currentObject);
        addressSpace = new StreamAddressSpace();
        materializedUpTo = Address.NON_ADDRESS;
        resolvedUpTo = Address.NON_ADDRESS;
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
