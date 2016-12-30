package org.corfudb.runtime.object.transactions;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.MultiObjectSMREntry;
import org.corfudb.protocols.logprotocol.MultiSMREntry;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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

    /**
     * Commit the transaction. If it is the last transaction in the stack,
     * write it to the log, otherwise merge it into a nested transaction.
     *
     * @return The address of the committed transaction.
     * @throws TransactionAbortedException If the transaction was aborted.
     */
    @Override
    public long commitTransaction() throws TransactionAbortedException {

        // If the transaction is nested, fold the transaction.
        if (TransactionalContext.isInNestedTransaction()) {
            getParentContext().addTransaction(this);
            commitAddress = AbstractTransactionalContext.FOLDED_ADDRESS;
            return commitAddress;
        }

        // we are going to compile the write-set in three parts:
        // 1. a MultiObjectSMREntry: This contains the update(s) to objects
        // 2. a set of stream-IDs : This is the set of affected streams
        // 3. a set of conflict-params : This is the set of affected conflict params

        // (item 1. in the comment above.)
        MultiObjectSMREntry entry = collectWriteSetEntries();

        // (item 2. in the comment above.)
        Set<UUID> affectedStreams = writeSet.keySet();

        // (item 3. in the comment above.)
        Set<Integer> writeConflictParams = collectWriteConflictParams();

        // compute the conflictSet
        TxResolutionInfo txResolutionInfo = new TxResolutionInfo(
                getSnapshotTimestamp(), affectedStreams, writeConflictParams
        );

        // Now we obtain a conditional address from the sequencer.
        // This step currently happens all at once, and we get an
        // address of -1L if it is rejected.
        long address = this.builder.runtime.getStreamsView()
                .acquireAndWrite(affectedStreams, entry, t->true, t->true, txResolutionInfo);
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


}
