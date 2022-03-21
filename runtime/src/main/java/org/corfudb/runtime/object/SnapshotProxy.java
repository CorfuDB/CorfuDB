package org.corfudb.runtime.object;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Map;
import java.util.function.LongConsumer;

@NotThreadSafe
@Slf4j
public class SnapshotProxy<T extends ICorfuSMR<T>> {

    private T snapshotInstance;

    private final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;

    private final long baseSnapshotVersion;

    public SnapshotProxy(final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap,
                         T instance, final long baseSnapshotVersion) {
        this.upcallTargetMap = upcallTargetMap;
        this.snapshotInstance = instance;
        this.baseSnapshotVersion = baseSnapshotVersion;
    }

    public <R> R access(ICorfuSMRAccess<R, T> accessFunction, LongConsumer versionAccessed) {
        final R ret = accessFunction.access(snapshotInstance);
        versionAccessed.accept(baseSnapshotVersion);
        return ret;
    }


    public void logUpdate(SMREntry entry) {
        snapshotInstance = applyUpdateUnsafe(entry);
    }

    private T applyUpdateUnsafe(SMREntry entry) {
        final ICorfuSMRUpcallTarget<T> target = upcallTargetMap.get(entry.getSMRMethod());
        if (target == null) {
            throw new RuntimeException("Unknown upcall " + entry.getSMRMethod());
        }

        // Mutator should return new version of object after mutation is applied
        return (T) target.upcall(snapshotInstance, entry.getSMRArguments());
    }

    // TODO: getUpcall().
}
