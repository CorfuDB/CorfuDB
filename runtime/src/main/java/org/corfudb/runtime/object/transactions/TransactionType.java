package org.corfudb.runtime.object.transactions;

import java.util.function.Function;

import lombok.RequiredArgsConstructor;

/**
 * Created by mwei on 11/21/16.
 */
@RequiredArgsConstructor
public enum TransactionType {
    OPTIMISTIC(OptimisticTransactionalContext::new),
    SNAPSHOT(SnapshotTransactionalContext::new),
    WRITE_AFTER_WRITE(WriteAfterWriteTransactionalContext::new);

    final Function<TransactionBuilder, ? extends AbstractTransactionalContext> get;
}
