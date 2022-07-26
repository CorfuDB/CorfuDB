package org.corfudb.runtime.object;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.StaleObjectVersionException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

@Slf4j
public class MultiVersionObject<T extends ICorfuSMR<T>> {

    private CorfuRuntime runtime;
    private MVOCache<T> mvoCache;

    private final StampedLock lock;
    private final ISMRStream smrStream;
    private final UUID streamID;

    private final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;
    private final Supplier<T> newObjectFn;

    private final Logger correctnessLogger = LoggerFactory.getLogger("correctness");

    private static final int TRIM_RETRY = 9;

    private final ISnapshotProxyGenerator<T> snapshotReferenceProxyGenerator;

    private final ISnapshotProxyGenerator<T> snapshotProxyGenerator;

    private volatile long trimMark;

    private volatile long currentSyncVersion = -10;

    private volatile long highestResolvedAddress = -10;

    private final Map<Long, CompletableFuture<ICorfuSMRSnapshotProxy<T>>> freeRides;

    MultiVersionObject(CorfuRuntime corfuRuntime,
                       Supplier<T> newObjectFn,
                       StreamViewSMRAdapter smrStream,
                       ICorfuSMR<T> wrapperObject,
                       UUID streamID) {
        this.runtime = corfuRuntime;
        this.mvoCache = runtime.getObjectsView().getMvoCache();
        this.newObjectFn = newObjectFn;
        this.smrStream = smrStream;
        this.upcallTargetMap = wrapperObject.getSMRUpcallMap();
        this.streamID = streamID;
        this.freeRides = new ConcurrentHashMap<>();

        this.snapshotReferenceProxyGenerator = (voId, object) ->
                new SnapshotReferenceProxy<>(voId.getObjectId(), voId.getVersion(), object, upcallTargetMap, SoftReference::new);

        this.snapshotProxyGenerator = (voId, object) ->
                new SnapshotProxy<>(object, voId.getVersion(), upcallTargetMap);

        wrapperObject.closeWrapper();
        this.lock = new StampedLock();

        this.mvoCache.registerMVO(this.streamID,this);
    }

    /**
     * Get snapshot proxy for a given timestamp.
     * 1. If called from non-transactional access, timestamp is stream tail and
     *    there should be an exact match in MVOCache if not yet evicted.
     * 2. If called from transactional access, timestamp is transaction snapshot
     *    address
     *    a. if timestamp > highestResolvedAddress, query sequencer for latest updates.
     *    b. if timestamp <= highestResolvedAddress, run MVOCache::floorEntry()
     *
     * @param timestamp the timestamp to query
     * @return the snapshot proxy against which the snapshot access is run
     */
    public ICorfuSMRSnapshotProxy<T> getSnapshotProxy(long timestamp) {

        // 1. Non-transactional access
        Optional<ICorfuSMRSnapshotProxy<T>> snapshotProxyOptional = mvoCache.get(
                new VersionedObjectIdentifier(streamID, timestamp),
                this::getSMRSnapshotProxy
        );
        if (snapshotProxyOptional.isPresent()) {
            return snapshotProxyOptional.get();
        }

        // 2. Transaction access
        if (timestamp > highestResolvedAddress || timestamp == Address.NON_EXIST) {
            // 2.a need to query sequencer for latest updates
            AtomicLong vloAccessedVersion = new AtomicLong();
            snapshotProxyOptional = getVersionedObjectUnderLock(timestamp, vloAccessedVersion::set);

            if (snapshotProxyOptional.isPresent()) {
                correctnessLogger.trace("Version, {}", vloAccessedVersion.get());
                log.trace("Access [{}] Updated (writelock) access at {}", this, vloAccessedVersion.get());
                return snapshotProxyOptional.get();
            }
        } else {
            // 2.b address has already been resolved
            snapshotProxyOptional = mvoCache.floorEntry(
                    new VersionedObjectIdentifier(streamID, timestamp),
                    this::getSMRSnapshotProxy
            );
            if (snapshotProxyOptional.isPresent()) {
                return snapshotProxyOptional.get();
            }
        }

        throw new StaleObjectVersionException(streamID, timestamp);
    }

    protected Optional<ICorfuSMRSnapshotProxy<T>> getVersionedObjectUnderLock(long timestamp, Consumer<Long> versionAccessed) {

        Preconditions.checkState(timestamp >= highestResolvedAddress);

        if (currentSyncVersion >= timestamp || highestResolvedAddress >= timestamp) {

            /*
                Both currentSyncVersion and highestResolvedAddress are set within the write-lock.
                currentSyncVersion is set to the timestamp before sync starts, and unset to -10 after sync completes.
                highestResolvedAddress is set to the timestamp after the sync completes, and it's monotonically increasing.

                a. If highestResolvedAddress >= timestamp, it means the address has been resolved and the
                   future of this timestamp has been completed in freeRides unless it has been GC-ed.
                   In the case of GC, all futures below trim mark will be completed with null.

                b. If currentSyncVersion >= timestamp, it means another thread is holding the lock and is
                   performing the syncing to a version that is >= my desired version.

                TODO: current thread cannot get a free ride if another thread has
                  just acquired the write-lock and has not updated currentSyncVersion
             */
            CompletableFuture<ICorfuSMRSnapshotProxy<T>> future =
                    freeRides.computeIfAbsent(timestamp, k -> new CompletableFuture<>());
            try {
                ICorfuSMRSnapshotProxy<T> ret = future.get();
                return Optional.ofNullable(ret);
            } catch (Exception e) {
                log.warn("failed to get a free ride for stream {} at versions {}", getID(), timestamp);
            }
        }

        long ts = 0;
        try {
            ts = lock.writeLock();
            currentSyncVersion = timestamp;

            Optional<ICorfuSMRSnapshotProxy<T>> snapshotProxyOptional = mvoCache.get(
                    new VersionedObjectIdentifier(streamID, timestamp), snapshotReferenceProxyGenerator);
            if (snapshotProxyOptional.isPresent()) {
                return snapshotProxyOptional;
            }

            for (int x = 0; x < TRIM_RETRY; x++) {
                try {
                    ICorfuSMRSnapshotProxy<T> snapshotProxy = syncObjectUnsafe(timestamp);
                    versionAccessed.accept(getVersionUnsafe());

                    if (snapshotProxy.getVersion() == Address.NON_EXIST) {
                        // Since there were no previous versions in the cache
                        // we keep the same proxy (not backed by SoftReference)
                        return Optional.of(snapshotProxy);
                    }

                    return mvoCache.get(
                            new VersionedObjectIdentifier(streamID, snapshotProxy.getVersion()),
                            snapshotReferenceProxyGenerator
                    );
                } catch (TrimmedException te) {
                    log.info("accessInner: Encountered trimmed address space " +
                                    "while accessing version {} of stream {} on attempt {}",
                            timestamp, streamID, x);

                    resetUnsafe();

                    if (x == (TRIM_RETRY - 1)) {
                        throw te;
                    }
                }
            }

            return Optional.empty();
        } finally {
            currentSyncVersion = -10;
            highestResolvedAddress = timestamp;
            lock.unlock(ts);
        }
    }

    public ICorfuSMRSnapshotProxy<T> syncObjectUnsafe(long timestamp) {
        ICorfuSMRSnapshotProxy<T> snapshotProxy = prepareObjectBeforeSync(timestamp);
        syncStreamUnsafe(snapshotProxy, smrStream, timestamp);
        return snapshotProxy;
    }

    private ICorfuSMRSnapshotProxy<T> prepareObjectBeforeSync(Long timestamp) {
        // The first access to this object should always proceed
        if (Boolean.FALSE.equals(mvoCache.containsObject(streamID))) {
            resetUnsafe();

            // Return a snapshot proxy that isn't backed by a SoftReference since
            // this snapshot is not yet tied to an MVO cache entry
            return getSMRSnapshotProxy(
                    new VersionedObjectIdentifier(streamID, Address.NON_EXIST),
                    newObjectFn.get(),
                    snapshotProxyGenerator
            );
        }

        // Find the entry with the greatest version less than or equal to the given version
        Optional<ICorfuSMRSnapshotProxy<T>> snapshotProxyOptional = mvoCache.floorEntry(
                new VersionedObjectIdentifier(streamID, timestamp),
                this::getSMRSnapshotProxy
        );

        if (!snapshotProxyOptional.isPresent()) {
            // Do not allow going back to previous versions
            resetUnsafe();
            throw new StaleObjectVersionException(streamID, timestamp);
        } else {
            // Next stream read begins from a given address (inclusive),
            // so +1 to avoid applying the same update twice
            smrStream.seek(snapshotProxyOptional.get().getVersion() + 1);
            return snapshotProxyOptional.get();
        }
    }

    protected void syncStreamUnsafe(@NonNull ICorfuSMRSnapshotProxy<T> snapshotProxy,
                                    @NonNull ISMRStream stream, long timestamp) {
        Runnable syncStreamRunnable = () ->
            stream.streamUpToInList(timestamp)
                .forEachOrdered(entryList -> {
                    try {
                        if (!entryList.isEmpty()) {
                            // Apply all updates in a MultiSMREntry
                            // TODO: is it possible that a MultiSMREntry has 0 SMREntry? - Seems like it.
                            final long globalAddress = entryList.get(0).getGlobalAddress();
                            snapshotProxy.logUpdate(entryList, () -> globalAddress);

                            VersionedObjectIdentifier voId = new VersionedObjectIdentifier(streamID, globalAddress);
                            mvoCache.put(voId, snapshotProxy.get());

                            freeRides.compute(globalAddress, (version, future) -> {
                                CompletableFuture<ICorfuSMRSnapshotProxy<T>> completableFuture = freeRides.get(version);
                                if (completableFuture == null) {
                                    completableFuture = new CompletableFuture<>();
                                }
                                completableFuture.complete(getSMRSnapshotProxy(voId, snapshotProxy.get()));
                                return completableFuture;
                            });

                            // TODO: handle StaleObjectVersionException
                        } else {
                            // TODO(Zach): Remove me
                            log.warn("Empty entry list");
                        }
                    } catch (Exception e) {
                        log.error("Sync[{}] Error: Couldn't execute upcall due to {}", this, e);
                        throw new UnrecoverableCorfuError(e);
                    }
                });

        MicroMeterUtils.time(syncStreamRunnable, "vlo.sync.timer",
                "streamId", getID().toString());
    }

    public long logUpdate(SMREntry entry) {
        return smrStream.append(entry,
            t -> true,
            t -> true);
    }

    public void resetUnsafe() {
        log.debug("Reset[{}]", this);
        smrStream.reset();
    }

    public long getVersionUnsafe() {
        return smrStream.pos();
    }

    public boolean containsVersion(Long target) {
        return mvoCache.containsKey(new VersionedObjectIdentifier(streamID, target));
    }

    public UUID getID() {
        return smrStream.getID();
    }

    private ICorfuSMRSnapshotProxy<T> getSMRSnapshotProxy(@NonNull VersionedObjectIdentifier voId, @NonNull T object) {
        return getSMRSnapshotProxy(voId, object, snapshotReferenceProxyGenerator);
    }

    private ICorfuSMRSnapshotProxy<T> getSMRSnapshotProxy(@NonNull VersionedObjectIdentifier voId, @NonNull T object,
                                                          @NonNull ISnapshotProxyGenerator<T> generator) {
        return generator.generate(voId, object);
    }

    /**
     * Prefix evict any versions smaller than the trimMark
     * @param trimMark trim up to this address, exclusive
     */
    public void gc(long trimMark) {
        if (trimMark == this.trimMark) {
            return;
        }
        this.trimMark = trimMark;

        // Remove all entries in freeRides whose version is below trim mark
        long finalTrimMark = trimMark;
        this.freeRides.forEach((key, value) -> {
            if (key < finalTrimMark) {
                value.complete(null);
                // Removal is performed by a single thread. The key is guaranteed
                // to be present from the time the CHM iterator is created.
                freeRides.remove(key);
            }
        });

        // Sequencer trim mark is exclusive. -=1 to convert it to inclusive
        trimMark -= 1;
        log.info("MVO GC evicts table {} versions up to {}", getID(), trimMark);
        mvoCache.getMvoCacheEviction().add(new VersionedObjectIdentifier(getID(), trimMark));
    }

    @VisibleForTesting
    public ISMRStream getSmrStream() {
        return smrStream;
    }
}
