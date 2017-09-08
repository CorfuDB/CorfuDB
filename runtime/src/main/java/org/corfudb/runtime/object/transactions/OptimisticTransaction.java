package org.corfudb.runtime.object.transactions;

/** This class is deprecated - please use {@Class ReadAfterWriteTransaction}
 *  instead.
 */
@Deprecated
public class OptimisticTransaction extends ReadAfterWriteTransaction {

    /** Generate a new optimistic (read-after-write) transactional context. */
    public OptimisticTransaction(TransactionBuilder builder) {
        super(builder);
        obtainSnapshotTimestamp();
    }
}
