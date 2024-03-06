package org.corfudb.runtime.object;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Map;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

@NotThreadSafe
@Slf4j
public class SnapshotProxy<T extends AutoCloseable> implements ICorfuSMRSnapshotProxy<T> {

    private final SMRSnapshot<T> snapshot;
    private T snapshotView;
    private final Supplier<Long> snapshotVersionSupplier;

    private final Map<String, CorfuSmrUpcallTarget<T>> upcallTargetMap;

    public SnapshotProxy(@NonNull final SMRSnapshot<T> snapshot, Supplier<Long> snapshotVersionSupplier,
                         @NonNull final Map<String, CorfuSmrUpcallTarget<T>> upcallTargetMap) {
        this.snapshot = snapshot;
        this.snapshotView = snapshot.consume();
        this.snapshotVersionSupplier = snapshotVersionSupplier;
        this.upcallTargetMap = upcallTargetMap;
    }

    public <R> R access(@NonNull ICorfuSMRAccess<R, T> accessFunction,
                        @NonNull LongConsumer versionAccessed) {
        final R ret = accessFunction.access(snapshotView);
        versionAccessed.accept(snapshotVersionSupplier.get());
        return ret;
    }

    public void logUpdate(@NonNull SMREntry updateEntry) {
        final CorfuSmrUpcallTarget<T> target = upcallTargetMap.get(updateEntry.getSMRMethod());

        if (target == null) {
            throw new RuntimeException("Unknown upcall " + updateEntry.getSMRMethod());
        }

        snapshotView = (T) target.upcall(snapshotView, updateEntry.getSMRArguments());
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
}
