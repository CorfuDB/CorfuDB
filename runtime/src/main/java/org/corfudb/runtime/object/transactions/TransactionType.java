package org.corfudb.runtime.object.transactions;

import java.util.function.Function;

import lombok.RequiredArgsConstructor;

/**
 * Created by mwei on 11/21/16.
 */
@RequiredArgsConstructor
public enum TransactionType {
    OPTIMISTIC(OptimisticTransaction::new),
    SNAPSHOT(SnapshotTransaction::new),
    WRITE_AFTER_WRITE(WriteAfterWriteTransaction::new),
    READ_AFTER_WRITE(ReadAfterWriteTransaction::new);

    final Function<TransactionBuilder, ? extends AbstractTransaction> get;
}
