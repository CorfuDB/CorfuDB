package org.corfudb.runtime.object;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.ICorfuImmutable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.exceptions.StaleObjectVersionException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
public class MultiVersionObject<T extends ICorfuSMR<T>, O extends ICorfuImmutable<T>> {

    private CorfuRuntime runtime;
    private MVOCache mvoCache;

    private final StampedLock lock;
    private final ISMRStream smrStream;
    private final UUID streamID;

    private final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;
    private final Supplier<T> newObjectFn;

    private final Logger correctnessLogger = LoggerFactory.getLogger("correctness");

    private static final int TRIM_RETRY = 9;

    public MultiVersionObject(CorfuRuntime corfuRuntime,
                              Supplier<T> newObjectFn,
                              StreamViewSMRAdapter smrStream,
                              ICorfuSMR<T> wrapperObject,
                              UUID streamID) {
        this.runtime = corfuRuntime;
        this.mvoCache = runtime.getObjectsView().getMvoCache();
        this.newObjectFn = newObjectFn;
        this.smrStream = smrStream;
        this.upcallTargetMap = wrapperObject.getCorfuSMRUpcallMap();
        this.streamID = streamID;

        wrapperObject.closeWrapper();
        lock = new StampedLock();

        mvoCache.registerMVO(this.streamID,this);
    }

    /**
     * Access the MVO object at a certain timestamp (version) using the given
     * access function.
     *
     * @param timestamp the version of the object to access
     * @param accessFunction the access function
     * @param <R>
     * @return
     * @throws NullPointerException
     */
    public <R> R access(long timestamp, Function<T, R> accessFunction) throws NullPointerException{
        if (containsVersion(timestamp)) {
            log.trace("Access [{}] Direct (optimistic-read) access at {}",
                    this, timestamp);
            T object = (T)mvoCache.get(new VersionedObjectIdentifier(streamID, timestamp)).get();
            R ret = accessFunction.apply(object);
            correctnessLogger.trace("Version, {}", timestamp);
            return ret;
        }

        AtomicLong vloAccessedVersion = new AtomicLong();
        T object = getVersionedObjectUnderLock(timestamp, vloAccessedVersion::set);
        correctnessLogger.trace("Version, {}", vloAccessedVersion.get());
        log.trace("Access [{}] Updated (writelock) access at {}", this, vloAccessedVersion.get());
        return accessFunction.apply(object);
    }

    // TODO: Complete implementation.
    public SnapshotProxyAdapter<T, O> getSnapshotProxy(long timestamp) {
        // TODO: Eliminate type cast
        Reference<O> ref  = mvoCache.get(new VersionedObjectIdentifier(streamID, timestamp));

        // PersistentCorfuTable shell shared with cache, but allocating
        // a new shell no longer tied the SoftReference to cache.
        // T instCopy = newObjectFn.get();
        // instCopy.setImmutableState(ref.get().getImmutableState());

        // SoftRef(PersistentCorfuTable : PersistentHashMapWrapper/ImmutableState)

        if (ref.get() != null) {
            return new SnapshotProxyAdapter<>(ref, timestamp, upcallTargetMap);
        }
        // TODO: this should provide a Reference<>? Should all Reference<> be given out by cache?
        AtomicLong vloAccessedVersion = new AtomicLong();
        T object = getVersionedObjectUnderLock(timestamp, vloAccessedVersion::set);
        correctnessLogger.trace("Version, {}", vloAccessedVersion.get());
        log.trace("Access [{}] Updated (writelock) access at {}", this, vloAccessedVersion.get());
        ref  = new SoftReference<>((O)object.getImmutableState());
        return new SnapshotProxyAdapter<>(ref, timestamp, upcallTargetMap);
    }

    protected T getVersionedObjectUnderLock(long timestamp, Consumer<Long> versionAccessed) {

        long ts = 0;
        try {
            ts = lock.writeLock();

            Reference<T> ref = mvoCache.get(new VersionedObjectIdentifier(streamID, timestamp));
            if (ref.get() != null) {
                return ref.get();
            }

            T object = newObjectFn.get();
            for (int x = 0; x < TRIM_RETRY; x++) {
                try {
                    syncObjectUnsafe(object, timestamp);
                    versionAccessed.accept(getVersionUnsafe());
                    break;
                } catch (TrimmedException te) {
                    log.info("accessInner: Encountered trimmed address space " +
                                    "while accessing version {} of stream {} on attempt {}",
                            timestamp, streamID, x);

                    resetUnsafe(object);

                    if (x == (TRIM_RETRY - 1)) {
                        throw te;
                    }
                }
            }
            return object;
        } finally {
            lock.unlock(ts);
        }
    }

    public void syncObjectUnsafe(T object, long timestamp) {
        prepareObjectBeforeSync(object, timestamp);
        syncStreamUnsafe(object, smrStream, timestamp);
    }

    private void prepareObjectBeforeSync(T object, Long timestamp) {
        // The first access to this object should always proceed
        if (Boolean.FALSE.equals(mvoCache.containsObject(streamID))) {
            resetUnsafe(object);
            return;
        }

        // Find the entry with the greatest version less than or equal to the given version
        SoftReference<Map.Entry<VersionedObjectIdentifier, ICorfuImmutable<T>>> floorEntry =
                mvoCache.floorEntry(new VersionedObjectIdentifier(streamID, timestamp));
        if (floorEntry.get() == null) {
            resetUnsafe(object);
            // Do not allow going back to previous versions
            throw new StaleObjectVersionException(streamID, timestamp);
        } else {
            object.setImmutableState(floorEntry.get().getValue());
            // Next stream read begins from a given address (inclusive),
            // so +1 to avoid applying the same update twice
            smrStream.seek(floorEntry.get().getKey().getVersion() + 1);
        }
    }

    protected void syncStreamUnsafe(T object, ISMRStream stream, long timestamp) {
        // TODO: Need better way to do this
        AtomicReference<T> wrappedObject = new AtomicReference<>(object);

        Runnable syncStreamRunnable = () ->
            stream.streamUpToInList(timestamp)
                .forEachOrdered(entryList -> {
                    try {
                        // TODO: how expensive?
                        T nextVersion = newObjectFn.get();
                        nextVersion.setImmutableState(wrappedObject.get().getImmutableState());
                        // Apply all updates in a MultiSMREntry
                        for (SMREntry entry : entryList) {
                            applyUpdateUnsafe(nextVersion, entry);
                        }
                        // TODO: is it possible that a MultiSMREntry has 0 SMREntry?
                        VersionedObjectIdentifier vloId = new VersionedObjectIdentifier(
                                streamID, entryList.get(0).getGlobalAddress());
                        mvoCache.put(vloId, (ICorfuImmutable<T>) nextVersion.getImmutableState());
                        wrappedObject.set(nextVersion);
                    } catch (Exception e) {
                        log.error("Sync[{}] Error: Couldn't execute upcall due to {}", this, e);
                        throw new UnrecoverableCorfuError(e);
                    }
                });
        MicroMeterUtils.time(syncStreamRunnable, "vlo.sync.timer",
                "streamId", getID().toString());
        object.setImmutableState(wrappedObject.get().getImmutableState());
    }

    private void applyUpdateUnsafe(T object, SMREntry entry) {
        log.trace("Apply[{}] of {}@{} ({})", this, entry.getSMRMethod(),
                Address.isAddress(entry.getGlobalAddress()) ? entry.getGlobalAddress() : "OPT",
                entry.getSMRArguments());

        ICorfuSMRUpcallTarget<T> target = upcallTargetMap.get(entry.getSMRMethod());
        if (target == null) {
            throw new RuntimeException("Unknown upcall " + entry.getSMRMethod());
        }

        // Now invoke the upcall
        target.upcall(object, entry.getSMRArguments());
    }

    public long logUpdate(SMREntry entry) {
        return smrStream.append(entry,
            t -> true,
            t -> true);
    }

    public void resetUnsafe(T object) {
        log.debug("Reset[{}]", this);
        smrStream.reset();
        object.reset();
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

}
