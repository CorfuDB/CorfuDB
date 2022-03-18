package org.corfudb.runtime.object;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
public class MultiVersionObject<T extends ICorfuSMR<T>> {

    private MVOCache mvoCache;

    private final StampedLock lock;
    private final ISMRStream smrStream;
    private final UUID streamID;

    private final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;
    private final Supplier<T> newObjectFn;

    private final Logger correctnessLogger = LoggerFactory.getLogger("correctness");

    private static final int TRIM_RETRY = 9;

    public MultiVersionObject(MVOCache mvoCache,
                              Supplier<T> newObjectFn,
                              StreamViewSMRAdapter smrStream,
                              ICorfuSMR<T> wrapperObject,
                              UUID streamID) {
        this.mvoCache = mvoCache;
        this.newObjectFn = newObjectFn;
        this.smrStream = smrStream;
        this.upcallTargetMap = wrapperObject.getCorfuSMRUpcallMap();
        this.streamID = streamID;

        wrapperObject.closeWrapper();
        lock = new StampedLock();
    }

    public <R> R access(long timestamp, Function<T, R> accessFunction) throws NullPointerException{
        if (containsVersion(timestamp)) {
            long vloAccessedVersion = getVersionUnsafe();
            log.trace("Access [{}] Direct (optimistic-read) access at {}",
                    this, vloAccessedVersion);
            T object = (T)mvoCache.get(new VersionedObjectIdentifier(streamID, timestamp)).get();
            R ret = accessFunction.apply(object);
            correctnessLogger.trace("Version, {}", vloAccessedVersion);
            return ret;
        }

        long ts = 0;
        try {
            ts = lock.writeLock();

            T object = buildVersionWithRetry(timestamp);
            long vloAccessedVersion = getVersionUnsafe();
            correctnessLogger.trace("Version, {}", vloAccessedVersion);
            log.trace("Access [{}] Updated (writelock) access at {}", this, vloAccessedVersion);
            return accessFunction.apply(object);
        } finally {
            lock.unlock(ts);
        }
    }

    // TODO: Complete implementation.
    public SnapshotProxyAdapter<T> getSnapshotProxy(long ts) {
        return null;
    }

    private T buildVersionWithRetry(long timestamp) {
        T object = newObjectFn.get();
        for (int x = 0; x < TRIM_RETRY; x++) {
            try {
                syncObjectUnsafe(object, timestamp);
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
    }

    public void syncObjectUnsafe(T object, long timestamp) {
        prepareObjectBeforeSync(object, timestamp);
        syncStreamUnsafe(object, smrStream, timestamp);
    }

    private void prepareObjectBeforeSync(T object, Long timestamp) {
        // find the entry with the greatest version less than or equal to the given version
        SoftReference<Map.Entry<Long, ICorfuSMR>> floorEntry = mvoCache.floorEntry(
                new VersionedObjectIdentifier(streamID, timestamp));
        if (floorEntry.get() == null) {
            resetUnsafe(object);
        } else {
            object.setImmutableObject(floorEntry.get().getValue().getImmutableObject());
            smrStream.seek(floorEntry.get().getKey());
        }
    }

    protected void syncStreamUnsafe(T object, ISMRStream stream, long timestamp) {
        log.trace("Sync[{}] {}", this, (timestamp == Address.OPTIMISTIC)
                ? "Optimistic" : "to " + timestamp);
        long syncTo = (timestamp == Address.OPTIMISTIC) ? Address.MAX : timestamp;

        Runnable syncStreamRunnable = () ->
            stream.streamUpTo(syncTo)
                .forEachOrdered(entry -> {
                    try {
                        applyUpdateUnsafe(object, entry);
                        VersionedObjectIdentifier vloId =
                                new VersionedObjectIdentifier(streamID, entry.getGlobalAddress());
                        mvoCache.put(vloId, object);
                    } catch (Exception e) {
                        log.error("Sync[{}] Error: Couldn't execute upcall due to {}", this, e);
                        throw new UnrecoverableCorfuError(e);
                    }
                });
        MicroMeterUtils.time(syncStreamRunnable, "vlo.sync.timer",
                "streamId", getID().toString());
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
