package org.corfudb.runtime.object.transactions;

import java.util.List;
import java.util.UUID;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.CorfuCompileProxy;
import org.corfudb.runtime.object.ICorfuSMR;
import org.corfudb.runtime.object.ICorfuSMRAccess;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;

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
    public <R, T extends ICorfuSMR<T>> R access(ICorfuSMRProxyInternal<T> proxy,
                                                ICorfuSMRAccess<R, T> accessFunction,
                                                Object[] conflictObject) {
        long startAccessTime = System.nanoTime();
        try {
            // TODO: better proxy type check, or refactor to avoid.
            if (proxy instanceof CorfuCompileProxy) {
                // In snapshot transactions, there are no conflicts.
                // Hence, we do not need to add this access to a conflict set
                // do not add: addToReadSet(proxy, conflictObject);
                return proxy.getUnderlyingObject().access(o -> o.getVersionUnsafe()
                                == getSnapshotTimestamp().getSequence()
                                && !o.isOptimisticallyModifiedUnsafe(),
                        o -> syncWithRetryUnsafe(o, getSnapshotTimestamp(), proxy, null),
                        accessFunction::access,
                        version -> updateKnownStreamPosition(proxy, version));
            } else {
                return getAndCacheSnapshotProxy(proxy, getSnapshotTimestamp().getSequence())
                        .access(accessFunction, version -> updateKnownStreamPosition(proxy, version));

            }
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
        if (hasAccessedMonotonicObject) {
            return getMinAddressRead();
        } else {
            return getMaxAddressRead();
        }
    }

    /**
     * Get the result of an upcall.
     *
     * @param proxy     The proxy to retrieve the upcall for.
     * @param timestamp The timestamp to return the upcall for.
     * @return The result of the upcall.
     */
    @Override
    public <T extends ICorfuSMR<T>> Object getUpcallResult(ICorfuSMRProxyInternal<T> proxy,
                                                            long timestamp,
                                                            Object[] conflictObject) {
        throw new UnsupportedOperationException("Can't get upcall during a read-only transaction!");
    }

    /**
     * Log an SMR update to the Corfu log.
     *
     * @param proxy       The proxy which generated the update.
     * @param updateEntry The entry which we are writing to the log.
     * @return The address the update was written at.
     */
    @Override
    public <T extends ICorfuSMR<T>> long logUpdate(ICorfuSMRProxyInternal<T> proxy,
                                                    SMREntry updateEntry,
                                                    Object[] conflictObject) {
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
