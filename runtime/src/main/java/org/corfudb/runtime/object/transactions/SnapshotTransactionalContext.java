package org.corfudb.runtime.object.transactions;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ConsistencyView;
import org.corfudb.runtime.object.ICorfuSMRAccess;
import org.corfudb.runtime.object.MVOCorfuCompileProxy;
import org.corfudb.runtime.object.SnapshotGenerator;
import org.corfudb.runtime.object.SnapshotGenerator.SnapshotGeneratorWithConsistency;

import java.util.List;
import java.util.UUID;

/**
 * A snapshot transactional context.
 *
 * <p>Given the snapshot (log address) given by the TxBuilder,
 * access all objects within the same snapshot during the course of
 * this transactional context.
 *
 * <p>Created by mwei on 11/22/16.
 */
public class SnapshotTransactionalContext extends AbstractTransactionalContext {

    public SnapshotTransactionalContext(Transaction transaction) {
        super(transaction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R, S extends SnapshotGeneratorWithConsistency<S>> R access(
            MVOCorfuCompileProxy<S> proxy, ICorfuSMRAccess<R, S> accessFunction, Object[] conflictObject) {
        long startAccessTime = System.nanoTime();
        try {
            return getAndCacheSnapshotProxy(proxy, getSnapshotTimestamp().getSequence())
                    .access(accessFunction, version -> updateKnownStreamPosition(proxy, version));
        } finally {
            dbNanoTime += (System.nanoTime() - startAccessTime);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long commitTransaction() throws TransactionAbortedException {
        // If the transaction has read monotonic objects, we instead return the min address
        // of all accessed streams. Although this avoids data loss, clients subscribing at
        // this point for delta/streaming may observe duplicate data.
        if (accessedReadCommittedObject) {
            return getMinAddressRead();
        } else {
            return getMaxAddressRead();
        }
    }

    /**
     * Log an SMR update to the Corfu log.
     *
     * @param proxy       The proxy which generated the update.
     * @param updateEntry The entry which we are writing to the log.
     * @return The address the update was written at.
     */
    @Override
    public long logUpdate(MVOCorfuCompileProxy<?> proxy,
                          SMREntry updateEntry, Object[] conflictObject) {
        throw new UnsupportedOperationException(
                "Can't modify object during a read-only transaction!");
    }

    @Override
    public void logUpdate(UUID streamId, SMREntry updateEntry) {
        throw new UnsupportedOperationException("Can't modify object during a read-only transaction!");
    }

    @Override
    public void logUpdate(UUID streamId, SMREntry updateEntry, List<UUID> streamTags) {
        throw new UnsupportedOperationException("Can't modify object during a read-only transaction!");
    }

    @Override
    public void logUpdate(UUID streamId, List<SMREntry> updateEntries) {
        throw new UnsupportedOperationException("Can't modify object during a read-only transaction!");
    }

    @Override
    public void addTransaction(AbstractTransactionalContext tc) {
        throw new UnsupportedOperationException("Can't merge into a readonly txn (yet)");
    }

    @Override
    public void addPreCommitListener(TransactionalContext.PreCommitListener preCommitListener) {
        throw new UnsupportedOperationException("Can't register precommit hooks in readonly txn");
    }
}
