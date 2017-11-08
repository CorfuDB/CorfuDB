package org.corfudb.runtime.object.transactions;

import java.util.function.BiFunction;

import lombok.RequiredArgsConstructor;

/**
 * Created by mwei on 11/21/16.
 */
@RequiredArgsConstructor
public enum TransactionType {
    /** Deprecated, no longer in use. */
    @Deprecated
    OPTIMISTIC(OptimisticTransaction::new),
    /** A snapshot transaction, which "locks" reads to a given snapshot. */
    SNAPSHOT(SnapshotTransaction::new),
    /** A transaction where a transaction is considered conflicting only if the
     * write set intersects with the write set of another transaction.
     */
    WRITE_AFTER_WRITE(WriteAfterWriteTransaction::new),
    /** A transaction where a transaction is considered conflicting only if the
     * read set intersects with the write set of another transaction.
     */
    READ_AFTER_WRITE(ReadAfterWriteTransaction::new);

    /** A function that generates a new instance of the transaction type. */
    final BiFunction<TransactionBuilder, TransactionContext, ? extends AbstractTransaction> get;
}
