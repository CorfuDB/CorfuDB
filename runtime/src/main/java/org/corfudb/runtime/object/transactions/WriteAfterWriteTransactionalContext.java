package org.corfudb.runtime.object.transactions;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.corfudb.runtime.view.ObjectsView.TRANSACTION_STREAM_ID;

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
    }

    @Override
    public long commitTransaction() throws TransactionAbortedException {


        // If the transaction is nested, fold the transaction.
        if (TransactionalContext.isInNestedTransaction()) {
            getParentContext().addTransaction(this);
            commitAddress = AbstractTransactionalContext.FOLDED_ADDRESS;
            return commitAddress;
        }

        // If the write set is empty, we're done and just return
        // NOWRITE_ADDRESS.
        if (writeSet.keySet().isEmpty()) {
            return NOWRITE_ADDRESS;
        }

        Set<UUID> affectedStreams = new HashSet<>(writeSet.keySet());
        if (this.builder.getRuntime().getObjectsView().isTransactionLogging()) {
            affectedStreams.add(TRANSACTION_STREAM_ID);
        }

        // Now we obtain a conditional address from the sequencer.
        // This step currently happens all at once, and we get an
        // address of -1L if it is rejected.
        long address = this.builder.runtime.getStreamsView()
                .acquireAndWrite(

                        // a set of stream-IDs that contains the affected streams
                        affectedStreams,

                        // a MultiObjectSMREntry that contains the update(s) to objects
                        collectWriteSetEntries(),

                        // nothing to do after successful acquisition and after deacquisition
                        t->true, t->true,

                        // TxResolution info:
                        // 1. snapshot timestamp
                        // 2. a map of conflict params, arranged by streamID's
                        // 3. a map of write conflict-params, arranged by streamID's
                        new TxResolutionInfo(getSnapshotTimestamp(),
                                collectWriteConflictParams(),
                                collectWriteConflictParams())
                );

        if (address == -1L) {
            log.debug("Transaction aborted due to sequencer rejecting request");
            abortTransaction();
            throw new TransactionAbortedException();
        }

        completionFuture.complete(true);
        commitAddress = address;

        // Update all proxies, committing the new address.
        updateAllProxies(x ->
                x.getUnderlyingObject()
                        .optimisticCommitUnsafe(commitAddress));

        return address;
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
