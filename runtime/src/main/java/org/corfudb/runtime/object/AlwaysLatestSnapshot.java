package org.corfudb.runtime.object;

import lombok.NonNull;
import org.rocksdb.OptimisticTransactionDB;

public class AlwaysLatestSnapshot<S extends SnapshotGenerator<S>> implements SMRSnapshot<S> {
    private final OptimisticTransactionDB rocksDb;
    private final ViewGenerator<S> viewGenerator;
    private final RocksDbColumnFamilyRegistry cfRegistry;

    public AlwaysLatestSnapshot(@NonNull OptimisticTransactionDB rocksDb,
                                @NonNull ViewGenerator<S> viewGenerator,
                                @NonNull RocksDbColumnFamilyRegistry cfRegistry) {
        this.rocksDb = rocksDb;
        this.viewGenerator = viewGenerator;
        this.cfRegistry = cfRegistry;
    }

    @Override
    public S consume() {
        return viewGenerator.newView(new RocksDbReadCommittedTx<>(rocksDb, cfRegistry));
    }

    @Override
    public void release() {
    }
}
