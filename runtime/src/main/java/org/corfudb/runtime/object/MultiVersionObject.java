package org.corfudb.runtime.object;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuOptions;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.object.ISMRStream.SingleAddressUpdates;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Supplier;

import static org.corfudb.runtime.CorfuOptions.ConsistencyModel.READ_COMMITTED;

/**
 * An implementation of a versioned SMR object that caches previously generated
 * versions. Cached versions are stored in an MVOCache, which is provided via the constructor.
 *
 * @param <S> The type of SMR object that provides snapshot generation capabilities.
 *            <p>
 *            Created by jielu, munshedm, and zfrenette.
 */
@Slf4j
public class MultiVersionObject<S extends SnapshotGenerator<S> & ConsistencyView> {

    private static final int snapshotFifoSize = 2;
    private static final String CORRECTNESS_LOG_MSG = "Version, {}";
    private static final String STREAM_ID_TAG_NAME = "streamId";

    /**
     * The stream view this object is backed by.
     */
    private final ISMRStream smrStream;
    /**
     * The MVOCache used to store and retrieve underlying object versions.
     */
    @Getter
    private final MVOCache<S> mvoCache;

    /**
     * A lock, which controls access to modifications to the object.
     * Any access to unsafe methods should obtain the lock.
     */
    private final StampedLock lock;

    /**
     * The upcall map for this object.
     */
    private final Map<String, ICorfuSMRUpcallTarget<S>> upcallTargetMap;

    /**
     * A function that generates a new instance of this object.
     */
    private final Supplier<S> newObjectFn;

    /**
     * Number of times to retry a sync when a TrimmedException is encountered.
     */
    private final int trimRetry;

    /**
     * Metadata on object versions generated.
     */
    protected volatile StreamAddressSpace addressSpace;

    /**
     * Current state of the underlying object. We maintain a reference here explicitly,
     * so that a version is always available to sync from, regardless of the underlying
     * caching strategy.
     */
    @Getter
    protected volatile S currentObject;

    /**
     * All versions up to (and including) materializedUpTo have had their versions
     * materialized. This value is always in sync with currentObject, even after a
     * resetUnsafe operation. Since this timestamp corresponds to a materialized version
     * of this stream, it is safe against potential sequencer regressions.
     */
    protected volatile long materializedUpTo = Address.NON_ADDRESS;

    /**
     * Used for optimization purposes, this value is similar to materializedUpTo.
     * However, unlike materializedUpTo, this timestamp is not safe against sequencer
     * regressions and should eventually be made epoch aware.
     */
    protected volatile long resolvedUpTo = Address.NON_ADDRESS;

    /**
     * For the purposes of caching, we need to know how this object was opened.
     */
    private final ObjectOpenOption objectOpenOption;

    /**
     * We always keep two snapshots: [Previous Snapshot, Current Snapshot]
     * The FIFO acts as a sliding window.
     */
    protected volatile ArrayDeque<SMRSnapshot<S>> snapshotFifo
            = new ArrayDeque<>(snapshotFifoSize);
    private final Logger correctnessLogger = LoggerFactory.getLogger("correctness");

    /**
     * Create a new MultiVersionObject.
     *
     * @param corfuRuntime  The Corfu runtime containing the MVOCache used to store and
     *                      retrieve versions for this object.
     * @param newObjectFn   A function passed to instantiate a new instance of this object.
     * @param smrStream     The stream View backing this object.
     * @param wrapperObject The wrapper over the actual object.
     */
    public MultiVersionObject(
            @Nonnull CorfuRuntime corfuRuntime, @Nonnull Supplier<S> newObjectFn,
            @Nonnull StreamViewSMRAdapter smrStream, @Nonnull ICorfuSMR wrapperObject,
            @Nonnull MVOCache<S> mvoCache, @Nonnull ObjectOpenOption objectOpenOption) {

        this.mvoCache = mvoCache;
        this.objectOpenOption = objectOpenOption;

        this.smrStream = smrStream;
        this.upcallTargetMap = wrapperObject.getSMRUpcallMap();
        this.newObjectFn = newObjectFn;

        this.lock = new StampedLock();
        this.addressSpace = new StreamAddressSpace();
        this.currentObject = newObjectFn.get();
        this.snapshotFifo.add(this.currentObject.generateSnapshot(
                new VersionedObjectIdentifier(getID(), Address.NON_EXIST)));
        this.trimRetry = corfuRuntime.getParameters().getTrimRetry();
    }

    /**
     * Retrieve a particular version of this object.
     * Allows subclasses to control caching behaviour, if applicable.
     *
     * @param voId The desired version of the object.
     */
    private SMRSnapshot<S> retrieveSnapshotUnsafe(@Nonnull VersionedObjectIdentifier voId) {
        if (voId.getVersion() == materializedUpTo) {
            return getCurrentSnapshot();
        }

        return mvoCache.get(voId).orElseThrow(() -> new TrimmedException(voId.getVersion(),
                String.format("Trimmed address %s has been evicted from MVOCache. StreamAddressSpace: %s.",
                        voId.getVersion(), addressSpace.toString())));
    }

    private void applySingleAddressUpdates(SingleAddressUpdates addressUpdates) {
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

            // Update the current object.
            addressUpdates.getSmrEntryList().forEach(this::applyUpdateUnsafe);

            // If we observe a new version, place the previous one into the MVOCache.
            if (globalAddress >= materializedUpTo) {
                final VersionedObjectIdentifier previousId = new VersionedObjectIdentifier(getID(), materializedUpTo);
                final VersionedObjectIdentifier currentId = new VersionedObjectIdentifier(getID(), globalAddress);
                currentObject.generateIntermediarySnapshot(currentId, objectOpenOption)
                        .ifPresent(newSnapshot -> {
                            final SMRSnapshot<S> previousSnapshot = setCurrentSnapshot(newSnapshot);
                            if (globalAddress == materializedUpTo) {
                                // The checkpoint sync will contain multiple SMR
                                // entries with the same address. Only cache the
                                // latest one, and release all the previous ones.
                                previousSnapshot.release();
                            }
                            mvoCache.put(previousId, previousSnapshot);
                        });
            }

            // In the case where addressUpdates corresponds to a HOLE, getSmrEntryList() will
            // produce an empty list and the below will be a no-op. This means that there can
            // be multiple versions that correspond to the same exact object.
            addressSpace.addAddress(globalAddress);
            materializedUpTo = globalAddress;
            resolvedUpTo = globalAddress;
        } catch (TrimmedException e) {
            // The caller catches this TrimmedException and resets the object before retrying.
            throw e;
        } catch (Exception e) {
            log.error("Sync[{}] couldn't execute upcall due to:", Utils.toReadableId(getID()), e);
            throw new UnrecoverableCorfuError(e);
        }
    }

    /**
     * Sync the SMR stream by playing updates forward in the stream until the given timestamp.
     *
     * @param timestamp The timestamp to sync up to.
     */
    private void syncStreamUnsafe(long timestamp) {
        if (log.isTraceEnabled()) {
            log.trace("Sync[{}] to {}", Utils.toReadableId(getID()), timestamp);
        }

        Runnable syncStreamRunnable = () -> {
            smrStream.streamUpToInList(timestamp)
                    .forEachOrdered(this::applySingleAddressUpdates);

            final VersionedObjectIdentifier voId = new VersionedObjectIdentifier(getID(), materializedUpTo);
            currentObject.generateTargetSnapshot(voId, objectOpenOption, getCurrentSnapshot())
                    .ifPresent(this::setCurrentSnapshot);
        };

        MicroMeterUtils.time(syncStreamRunnable, "mvo.sync.timer", STREAM_ID_TAG_NAME, getID().toString());
    }

    private SMRSnapshot<S> getCurrentSnapshot() {
        return snapshotFifo.getLast();
    }

    private SMRSnapshot<S> setCurrentSnapshot(SMRSnapshot<S> newSnapshot) {
        snapshotFifo.addLast(newSnapshot);
        return removeAndGetPreviousSnapshot();
    }

    private SMRSnapshot<S> removeAndGetPreviousSnapshot() {
        if (snapshotFifo.size() < snapshotFifoSize) {
            throw new IllegalStateException("Number of snapshots must always be constant.");
        }
        return snapshotFifo.remove();
    }

    /**
     * Obtain a snapshot proxy, able to serve accesses and mutations, which contains the most recent state
     * of the object for the provided timestamp.
     *
     * @param timestamp The desired version of the object.
     * @return A snapshot proxy containing the most recent state of the object for the provided timestamp.
     */
    public ICorfuSMRSnapshotProxy<S> getSnapshotProxy(long timestamp) {
        final VersionedObjectIdentifier voId = new VersionedObjectIdentifier(getID(), timestamp);
        long lockTs = lock.tryOptimisticRead();

        if (log.isTraceEnabled()) {
            log.trace("SnapshotProxy[{}] optimistic request at ts {}", Utils.toReadableId(getID()), timestamp);
        }

        if (lockTs != 0) {
            try {
                Optional<ICorfuSMRSnapshotProxy<S>> snapshot = getSnapshotUnsafe(voId, lockTs);
                if (snapshot.isPresent()) {
                    // Lock was validated within getSnapshotUnsafe.
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
            Optional<ICorfuSMRSnapshotProxy<S>> snapshot = getSnapshotUnsafe(voId, lockTs);
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
                log.trace("SnapshotProxy[{}] write lock request at ts {} served by ts {}",
                        Utils.toReadableId(getID()), timestamp, streamTs);
            }

            correctnessLogger.trace(CORRECTNESS_LOG_MSG, streamTs);
            return new SnapshotProxy<>(getCurrentSnapshot(), getVersionSupplier(streamTs), upcallTargetMap);
        } finally {
            lock.unlock(lockTs);
        }
    }

    /**
     * Determine if a particular timestamp/version has been materialized by this versioned object.
     *
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
     *
     * @param voId   The desired version of the object.
     * @param lockTs The stamp previously obtained from the stamped lock.
     * @return If available, a snapshot proxy containing the most recent state of the object for
     * the provided timestamp.
     */
    private Optional<ICorfuSMRSnapshotProxy<S>> getSnapshotUnsafe(@Nonnull VersionedObjectIdentifier voId, long lockTs) {
        final long startTime = System.nanoTime();
        boolean isLockStampValid = false;
        SnapshotProxy<S> snapshotProxy = null;

        try {
            if (isTimestampMaterializedUnsafe(voId.getVersion())) {
                // Find the latest version materialized for this object that is visible from timestamp.
                final long streamTs = addressSpace.floor(voId.getVersion());
                voId.setVersion(streamTs);

                // The snapshot is acquired before validating the lock, as the state of the
                // underlying object can change afterward.
                final SMRSnapshot<S> versionedObject = retrieveSnapshotUnsafe(voId);
                snapshotProxy = new SnapshotProxy<>(versionedObject, getVersionSupplier(streamTs), upcallTargetMap);

                if (lock.validate(lockTs)) {
                    correctnessLogger.trace(CORRECTNESS_LOG_MSG, streamTs);
                    isLockStampValid = true;
                    return Optional.of(snapshotProxy);
                }
            }

            return Optional.empty();
        } finally {
            // If the lock stamp was not valid, release the consumed snapshot.
            if (!isLockStampValid && !Objects.isNull(snapshotProxy)) {
                // Do not release the snapshot itself since the snapshot is always
                // going to be valid (albeit the wrong version potentially).
                // Instead, just release the view.
                snapshotProxy.releaseView();
            }

            MicroMeterUtils.time(Duration.ofNanos(System.nanoTime() - startTime), "mvo.snapshot.query");
        }
    }

    /**
     * Apply a single SMR entry on the current state of the object.
     * This method does not compute a new snapshot.
     *
     * @param updateEntry The SMR entry to apply.
     */
    private void applyUpdateUnsafe(@Nonnull SMREntry updateEntry) {
        if (log.isTraceEnabled()) {
            log.trace("Apply[{}] of {}@{} ({})", Utils.toReadableId(getID()),
                    updateEntry.getSMRMethod(), updateEntry.getGlobalAddress(), updateEntry.getSMRArguments());
        }

        final ICorfuSMRUpcallTarget<S> target = upcallTargetMap.get(updateEntry.getSMRMethod());

        if (target == null) {
            throw new IllegalStateException("Unknown upcall " + updateEntry.getSMRMethod());
        }

        currentObject = (S) target.upcall(currentObject, updateEntry.getSMRArguments());
    }

    /**
     * Get the ID of the stream backing this object.
     *
     * @return The ID of the stream backing this object.
     */
    private UUID getID() {
        return smrStream.getID();
    }

    private Supplier<Long> getVersionSupplier(final long streamTs) {
        if (currentObject.getConsistencyModel() == READ_COMMITTED) {
            return () -> materializedUpTo;
        }

        return () -> streamTs;
    }

    /**
     * Run GC on this object.
     * <p>
     * Since the stream that backs this object, and the corresponding version metadata are
     * not thread-safe, synchronization between GC and external access is needed.
     *
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
     *
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
        log.debug("Reset[{}]", Utils.toReadableId(getID()));
        materializedUpTo = Address.NON_ADDRESS;
        resolvedUpTo = Address.NON_ADDRESS;

        snapshotFifo.forEach(SMRSnapshot::release);

        long cacheSize = mvoCache.size();
        mvoCache.invalidateAllVersionsOf(getSmrStream().getID());
        long newCacheSize = mvoCache.size();
        log.info("MVO Cache size before and after invalidation ({}, {}).",
                cacheSize, newCacheSize);

        snapshotFifo = new ArrayDeque<>(snapshotFifoSize);
        currentObject.close();
        currentObject = newObjectFn.get();

        snapshotFifo.add(currentObject.generateSnapshot(new VersionedObjectIdentifier(getID(), Address.NON_EXIST)));
        addressSpace = new StreamAddressSpace();
        smrStream.reset();
    }

    /**
     * Close the underlying resources bound to currentObject and its snapshot.
     */
    public void close() {
        long lockTs = 0;

        try {
            lockTs = lock.writeLock();
            snapshotFifo.forEach(SMRSnapshot::release);
            currentObject.close();
        } finally {
            lock.unlock(lockTs);
        }
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
