package org.corfudb.runtime.object;

import lombok.Getter;
import lombok.NonNull;
import org.rocksdb.OptimisticTransactionDB;

/**
 * Provides read-committed consistency guarantees.
 *
 * @param <S> extends SnapshotGenerator
 */
public class AlwaysLatestSnapshot<S extends SnapshotGenerator<S>> implements SMRSnapshot<S> {
    private final OptimisticTransactionDB rocksDb;
    private final ViewGenerator<S> viewGenerator;

    @Getter
    private final VersionedObjectStats metrics;

    public AlwaysLatestSnapshot(@NonNull OptimisticTransactionDB rocksDb,
                                @NonNull ViewGenerator<S> viewGenerator) {
        this.rocksDb = rocksDb;
        this.viewGenerator = viewGenerator;
        this.metrics = new VersionedObjectStats();
    }

    @Override
    public S consume() {
        return viewGenerator.newView(new RocksDbReadCommittedTx(rocksDb));
    }

    @Override
    public void release() {
        // No-op.
    }
}
