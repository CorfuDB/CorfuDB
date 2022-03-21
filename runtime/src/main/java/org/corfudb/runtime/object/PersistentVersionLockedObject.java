package org.corfudb.runtime.object;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.corfudb.runtime.view.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.SoftReference;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

@Slf4j
public class PersistentVersionLockedObject<T extends ICorfuSMR<T>> {

    // Long timestamp -> PersistentHashMapWrapper object
    TreeMap<Long, Object> versionedMaps = new TreeMap<>();

    @Getter
    private T object;   // PersistentCorfuTable instance

    private final StampedLock lock;
    private final ISMRStream smrStream;

    private final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;
    private final Supplier<T> newObjectFn;

    private final Logger correctnessLogger = LoggerFactory.getLogger("correctness");


    public PersistentVersionLockedObject(Supplier<T> newObjectFn,
                                         StreamViewSMRAdapter smrStream,
                                         ICorfuSMR<T> wrapperObject) {
        this.smrStream = smrStream;
        this.upcallTargetMap = wrapperObject.getCorfuSMRUpcallMap();

        wrapperObject.closeWrapper();
        this.newObjectFn = newObjectFn;
        this.object = newObjectFn.get();
        lock = new StampedLock();
    }

    public <R> R access(Function<PersistentVersionLockedObject<T>, Boolean> directAccessCheckFunction,
                        Consumer<PersistentVersionLockedObject<T>> updateFunction,
                        Function<T, R> accessFunction,
                        LongConsumer versionAccessed) {

        if (Boolean.TRUE.equals(directAccessCheckFunction.apply(this))) {
            long vloAccessedVersion = getVersionUnsafe();
            log.trace("Access [{}] Direct (optimistic-read) access at {}",
                    this, vloAccessedVersion);
            R ret = accessFunction.apply(object.getContext(ICorfuExecutionContext.DEFAULT));
            correctnessLogger.trace("Version, {}", vloAccessedVersion);
            versionAccessed.accept(vloAccessedVersion);
            return ret;
        }

        long ts = 0;
        try {
            ts = lock.writeLock();

            // Check if direct access is possible (unlikely).
            if (directAccessCheckFunction.apply(this)) {
                long vloAccessedVersion = getVersionUnsafe();
                log.trace("Access [{}] Direct (writelock) access at {}", this, vloAccessedVersion);
                R ret = accessFunction.apply(object.getContext(ICorfuExecutionContext.DEFAULT));
                correctnessLogger.trace("Version, {}", vloAccessedVersion);
                versionAccessed.accept(vloAccessedVersion);
                return ret;
            }
            // If not, perform the update operations
            updateFunction.accept(this);
            long vloAccessedVersion = getVersionUnsafe();
            correctnessLogger.trace("Version, {}", vloAccessedVersion);
            log.trace("Access [{}] Updated (writelock) access at {}", this, vloAccessedVersion);
            versionAccessed.accept(vloAccessedVersion);
            return accessFunction.apply(object.getContext(ICorfuExecutionContext.DEFAULT));
            // And perform the access
        } finally {
            lock.unlock(ts);
        }
    }

    // TODO: Complete implementation.
    public SnapshotProxyAdapter<T> getSnapshotProxy(long ts) {
        return null;
    }

    public void syncObjectUnsafe(long timestamp) {
        prepareObjectBeforeSync(timestamp);
        syncObjectUnsafeInner(timestamp);
    }

    private void prepareObjectBeforeSync(Long timestamp) {
        // find the entry with the greatest version less than or equal to the given version
        Map.Entry<Long, Object> floorEntry = versionedMaps.floorEntry(timestamp);
        if (floorEntry == null) {
            resetUnsafe();
        }
        else {
            object.setImmutableObject(floorEntry.getValue());
            smrStream.seek(floorEntry.getKey());
        }
    }

    private void syncObjectUnsafeInner(long timestamp) {
        syncStreamUnsafe(smrStream, timestamp);
    }

    protected void syncStreamUnsafe(ISMRStream stream, long timestamp) {
        log.trace("Sync[{}] {}", this, (timestamp == Address.OPTIMISTIC)
                ? "Optimistic" : "to " + timestamp);
        long syncTo = (timestamp == Address.OPTIMISTIC) ? Address.MAX : timestamp;

        Runnable syncStreamRunnable = () ->
            stream.streamUpTo(syncTo)
                .forEachOrdered(entry -> {
                    try {
                        Object res = applyUpdateUnsafe(entry);
                        versionedMaps.put(entry.getGlobalAddress(), object.getImmutableObject());
                    } catch (Exception e) {
                        log.error("Sync[{}] Error: Couldn't execute upcall due to {}", this, e);
                        throw new UnrecoverableCorfuError(e);
                    }
                });
        MicroMeterUtils.time(syncStreamRunnable, "vlo.sync.timer",
                "streamId", getID().toString());
    }

    private Object applyUpdateUnsafe(SMREntry entry) {
        log.trace("Apply[{}] of {}@{} ({})", this, entry.getSMRMethod(),
                Address.isAddress(entry.getGlobalAddress()) ? entry.getGlobalAddress() : "OPT",
                entry.getSMRArguments());

        ICorfuSMRUpcallTarget<T> target = upcallTargetMap.get(entry.getSMRMethod());
        if (target == null) {
            throw new RuntimeException("Unknown upcall " + entry.getSMRMethod());
        }

        // Now invoke the upcall
        return target.upcall(object, entry.getSMRArguments());
    }

    public long logUpdate(SMREntry entry) {
        return smrStream.append(entry,
            t -> true,
            t -> true);
    }

    public void resetUnsafe() {
        log.debug("Reset[{}]", this);
        object.close();
        object = newObjectFn.get();
        smrStream.reset();
    }

    public long getVersionUnsafe() {
        return smrStream.pos();
    }

    public boolean containsVersion(Long target) {
        return versionedMaps.containsKey(target);
    }

    public UUID getID() {
        return smrStream.getID();
    }

}
