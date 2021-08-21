package org.corfudb.infrastructure.remotecorfutable.loglistener.smr;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.apache.commons.collections.iterators.ReverseListIterator;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableDatabaseEntry;
import org.corfudb.common.remotecorfutable.RemoteCorfuTableVersionedKey;
import org.corfudb.infrastructure.remotecorfutable.DatabaseHandler;
import org.corfudb.runtime.collections.remotecorfutable.RemoteCorfuTableSMRMethods;
import org.rocksdb.RocksDBException;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * This operation performs a batched write of a single transaction from a MultiSMREntry.
 *
 * Created by nvaishampayan517 on 08/20/21
 */
@AllArgsConstructor
public class CompositeOperation implements SMROperation {
    @NonNull
    private final List<SMROperation> subOperations;
    @NonNull
    private final UUID streamId;

    /**
     * {@inheritDoc}
     * <p>
     *     Applies the composition of all SMROperations in the transaction.
     * </p>
     */
    @Override
    public void applySMRMethod(@NonNull DatabaseHandler dbHandler) throws RocksDBException {
        dbHandler.updateAll(getEntryBatch(), streamId);
    }

    /**
     *{@inheritDoc}
     */
    @Override
    public List<RemoteCorfuTableDatabaseEntry> getEntryBatch() {
        List<RemoteCorfuTableDatabaseEntry> entries = new LinkedList<>();
        //in the transaction, all operations are applied at the same timestamp
        //thus, we only keep the final state of the keys
        Set<RemoteCorfuTableVersionedKey> seen = new HashSet<>();
        //if we iterate in reverse order, and ignore any previously seen keys
        //we end up with the final state of each key
        ReverseListIterator revIter = new ReverseListIterator(subOperations);
        while (revIter.hasNext()) {
            SMROperation currOperation = (SMROperation) revIter.next();
            if (currOperation.getType() == RemoteCorfuTableSMRMethods.CLEAR) {
                //anything before the clear must be overwritten, thus we can break early
                break;
            }
            List<RemoteCorfuTableDatabaseEntry> operationBatch = currOperation.getEntryBatch();
            ReverseListIterator revBatchIter = new ReverseListIterator(operationBatch);
            while (revBatchIter.hasNext()) {
                RemoteCorfuTableDatabaseEntry entry = (RemoteCorfuTableDatabaseEntry) revBatchIter.next();
                if (!seen.contains(entry.getKey())) {
                    seen.add(entry.getKey());
                    entries.add(entry);
                }
            }
        }
        return entries;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RemoteCorfuTableSMRMethods getType() {
        return RemoteCorfuTableSMRMethods.COMPOSITE;
    }
}
