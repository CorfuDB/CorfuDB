package org.corfudb.runtime.object;

import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Map;
import java.util.function.Supplier;
import org.corfudb.util.serializer.ISerializer;

import java.util.Set;
import java.util.UUID;

@NotThreadSafe
@Slf4j
public class SnapshotProxy<T extends SnapshotGenerator<T> & ConsistencyView> implements ICorfuSMRProxyMetadata, ICorfuSMRProxy<T> {

    private final SMRSnapshot<T> snapshot;
    private T snapshotView;
    @Getter
    private final Supplier<Long> snapshotVersionSupplier;
    private final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap;

    public SnapshotProxy(@NonNull final SMRSnapshot<T> snapshot, Supplier<Long> snapshotVersionSupplier,
                         @NonNull final Map<String, ICorfuSMRUpcallTarget<T>> upcallTargetMap) {
        this.snapshot = snapshot;
        this.snapshotView = snapshot.consume();
        this.snapshotVersionSupplier = snapshotVersionSupplier;
        this.upcallTargetMap = upcallTargetMap;
    }

    @Override
    public <R> R access(ICorfuSMRAccess<R, T> accessMethod, Object[] conflictObject) {
        return accessMethod.access(snapshotView);
    }

    public long logUpdate(String smrUpdateFunction, Object[] conflictObject, Object... args) {
        final ICorfuSMRUpcallTarget<T> target = upcallTargetMap.get(smrUpdateFunction);

        if (target == null) {
            throw new RuntimeException("Unknown upcall " + smrUpdateFunction);
        }

        snapshotView = (T) target.upcall(snapshotView, conflictObject);
        return 0;
    }

    public void release() {
        snapshot.release();
    }

    public void releaseView() {
        try {
            snapshotView.close();
        } catch (Exception e) {
            throw new UnrecoverableCorfuError(e);
        }
    }

    @Override
    public UUID getStreamID() {
        return null;
    }

    @Override
    public Set<UUID> getStreamTags() {
        return null;
    }

    @Override
    public boolean isObjectCached() {
        return false;
    }

    @Override
    public MultiVersionObject<T> getUnderlyingMVO() {
        return null;
    }

    @Override
    public ISerializer getSerializer() {
        return null;
    }
}
