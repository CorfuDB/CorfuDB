package org.corfudb.runtime.object.transactions;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

import lombok.Getter;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.object.ICorfuSMRAccess;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;
import org.corfudb.runtime.view.Address;

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

    /** In a snapshot transaction, no proxies are ever modified.
     *
     */
    @Getter
    private Set<ICorfuSMRProxyInternal> modifiedProxies = ImmutableSet.of();

    public SnapshotTransactionalContext(Transaction transaction) {
        super(transaction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <R, T> R access(ICorfuSMRProxyInternal<T> proxy,
                           ICorfuSMRAccess<R, T> accessFunction,
                           Object[] conflictObject) {

        // In snapshot transactions, there are no conflicts.
        // Hence, we do not need to add this access to a conflict set
        // do not add: addToReadSet(proxy, conflictObject);
        return proxy.getUnderlyingObject().access(o -> o.getVersionUnsafe()
                        == getSnapshotTimestamp().getSequence()
                        && !o.isOptimisticallyModifiedUnsafe(),
                o -> {
                    syncWithRetryUnsafe(o, getSnapshotTimestamp(), proxy, null);
                },
                o -> accessFunction.access(o));
    }

    /**
     * Get the result of an upcall.
     *
     * @param proxy     The proxy to retrieve the upcall for.
     * @param timestamp The timestamp to return the upcall for.
     * @return The result of the upcall.
     */
    @Override
    public <T> Object getUpcallResult(ICorfuSMRProxyInternal<T> proxy,
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
    public <T> long logUpdate(ICorfuSMRProxyInternal<T> proxy,
                              SMREntry updateEntry,
                              Object[] conflictObject) {
        throw new UnsupportedOperationException(
                "Can't modify object during a read-only transaction!");
    }

    @Override
    public void addTransaction(AbstractTransactionalContext tc) {
        throw new UnsupportedOperationException("Can't merge into a readonly txn (yet)");
    }
}
