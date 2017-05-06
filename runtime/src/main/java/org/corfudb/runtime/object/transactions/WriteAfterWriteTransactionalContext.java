package org.corfudb.runtime.object.transactions;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;

/** A write-after-write transactional context.
 *
 * A write-after-write transactional context behaves like an optimistic
 * context, except behavior during commit (for writes):
 *
 *   (1) Reads behave the same as in a regular optimistic
 *     transaction.
 *
 *   (2) Writes in a write-after-write transaction are guaranteed
 *     to commit atomically, if and only if none of the objects
 *     written (the "write set") were modified between the first read
 *     ("first read timestamp") and the time of commit.
 *
 * Created by mwei on 11/21/16.
 */
@Slf4j
public class WriteAfterWriteTransactionalContext
        extends OptimisticTransactionalContext {

    WriteAfterWriteTransactionalContext(TransactionBuilder builder) {
        super(builder);
        getSnapshotTimestamp();
    }

    @Override
    public long commitTransaction() throws TransactionAbortedException {
        log.debug("TX[{}] request write-write commit", this);

        return getConflictSetAndCommit(() -> collectWriteConflictParams());
    }

    @Override
    /** Add the proxy and conflict-params information to our read set.
     * @param proxy             The proxy to add
     * @param conflictObjects    The fine-grained conflict information, if
     *                          available.
     */
    public void addToReadSet(ICorfuSMRProxyInternal proxy, Object[] conflictObjects) {
        // do nothing! write-write conflict TXs do not need to keep track of
        // read sets.
    }
}
