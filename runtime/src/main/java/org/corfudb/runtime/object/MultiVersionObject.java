package org.corfudb.runtime.object;

import com.google.common.annotations.VisibleForTesting;
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

    private volatile long highestResolvedAddress = -10;

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

        this.snapshotReferenceProxyGenerator = (voId, object) ->
                new SnapshotReferenceProxy<>(voId.getObjectId(), voId.getVersion(), object, upcallTargetMap, SoftReference::new);

        this.snapshotProxyGenerator = (voId, object) ->
                new SnapshotProxy<>(object, voId.getVersion(), upcallTargetMap);

        wrapperObject.closeWrapper();
        this.lock = new StampedLock();

        this.mvoCache.registerMVO(this.streamID,this);
    }

    public ICorfuSMRSnapshotProxy<T> getSnapshotProxy(long timestamp) {
        Optional<ICorfuSMRSnapshotProxy<T>> snapshotProxyOptional = mvoCache.get(
                new VersionedObjectIdentifier(streamID, timestamp),
                this::getSMRSnapshotProxy
        );

        if (snapshotProxyOptional.isPresent()) {
            return snapshotProxyOptional.get();
        }

        AtomicLong vloAccessedVersion = new AtomicLong();
        snapshotProxyOptional = getVersionedObjectUnderLock(timestamp, vloAccessedVersion::set);

        if (snapshotProxyOptional.isPresent()) {
            correctnessLogger.trace("Version, {}", vloAccessedVersion.get());
            log.trace("Access [{}] Updated (writelock) access at {}", this, vloAccessedVersion.get());
            return snapshotProxyOptional.get();
        }

        throw new StaleObjectVersionException(streamID, timestamp);
    }

    protected Optional<ICorfuSMRSnapshotProxy<T>> getVersionedObjectUnderLock(long timestamp, Consumer<Long> versionAccessed) {

        long ts = 0;
        try {
            ts = lock.writeLock();

            Optional<ICorfuSMRSnapshotProxy<T>> snapshotProxyOptional = mvoCache.get(
                    new VersionedObjectIdentifier(streamID, timestamp), snapshotReferenceProxyGenerator);
            if (snapshotProxyOptional.isPresent()) {
                return snapshotProxyOptional;
            }

            // This retry is also performed within a transaction. Previously, the number of retries was
            // determined by the runtime parameters.
            // TODO(Zach): This should be adjusted to take into account the above.
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

                    if (!te.isRetriable() || x == (TRIM_RETRY - 1)) {
                        throw te;
                    }
                }
            }

            return Optional.empty();
        } finally {
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
     *
     * Note that when checkpoint and trim is involved, the latest checkpoint
     * will be removed from the cache. This is because checkpointing writes a
     * NO_OP entry followed by the real checkpoint entries (START, CONTINUATION,
     * and END) which sets the checkpoint snapshot address to the NO_NP address.
     * For example, the CP entries will have snapshot address set to 100. Then
     * prefix trim at 100 will delete all entries up to 100 (inclusive), and
     * MVOCache will evict the versioned object built by CP entries because the
     * version of the versioned object is also 100. However, this is not a bug
     * because the next sync on this object with still be able to apply the CP
     * entries and put the versioned object back to the cache.
     *
     * |     100 inserts    |NO_OP|   CP entries    |
     * | 0 | 1 | 2 |...| 99 | 100 | 101 | 102 | 103 |
     *
     * @param trimMark trim up to this address, exclusive
     */
    public void gc(long trimMark) {
        if (trimMark == this.trimMark) {
            return;
        }
        this.trimMark = trimMark;

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
