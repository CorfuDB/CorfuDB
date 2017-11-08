package org.corfudb.runtime.object.transactions;

import javax.annotation.Nonnull;

/** This class is deprecated - please use {@Class ReadAfterWriteTransaction}
 *  instead.
 */
@Deprecated
public class OptimisticTransaction extends ReadAfterWriteTransaction {

    /** Generate a new optimistic (read-after-write) transactional context. */
    public OptimisticTransaction(@Nonnull TransactionBuilder builder,
                                 @Nonnull TransactionContext context) {
        super(builder, context);
        obtainSnapshotTimestamp();
    }
}
