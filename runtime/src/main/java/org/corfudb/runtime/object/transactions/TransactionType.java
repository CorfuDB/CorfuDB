package org.corfudb.runtime.object.transactions;

import lombok.RequiredArgsConstructor;

import java.util.function.Function;

/**
 * Created by mwei on 11/21/16.
 */
@RequiredArgsConstructor
public enum TransactionType {
    OPTIMISTIC(OptimisticTransactionalContext::new),
    SNAPSHOT(SnapshotTransactionalContext::new),
    WRITE_AFTER_WRITE(WriteAfterWriteTransactionalContext::new);

    final Function<Transaction, ? extends AbstractTransactionalContext> get;
}
