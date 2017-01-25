package org.corfudb.runtime.object.transactions;

import com.google.common.collect.ImmutableSet;
import lombok.Getter;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.exceptions.NoRollbackException;
import org.corfudb.runtime.object.ICorfuSMRAccess;
import org.corfudb.runtime.object.ICorfuSMRProxyInternal;

import java.util.*;

/**
 * A snapshot transactional context.
 *
 * Given the snapshot (log address) given by the TransactionBuilder,
 * access all objects within the same snapshot during the course of
 * this transactional context.
 *
 * Created by mwei on 11/22/16.
 */
public class SnapshotTransactionalContext extends AbstractTransactionalContext {

    /** In a snapshot transaction, no proxies are ever modified.
     *
     */
    @Getter
    private Set<ICorfuSMRProxyInternal> modifiedProxies = ImmutableSet.of();

    public SnapshotTransactionalContext(TransactionBuilder builder) {
        super(builder);
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

        return proxy.getUnderlyingObject().optimisticallyReadThenReadLockThenWriteOnFail(
                (v, o) -> {
                    // We're lucky and the object has not been modified AND
                    // it's the right version.
                    if (v == getSnapshotTimestamp() &&
                            !proxy.getUnderlyingObject().isOptimisticallyModifiedUnsafe()) {
                        return accessFunction.access(o);
                    }
                    throw new ConcurrentModificationException();
                },
                (v, o) -> {
                    // We're lucky and the object has not been modified AND
                    // it's the right version. (Another writer modified it to this state).
                    if (v == getSnapshotTimestamp() &&
                            !proxy.getUnderlyingObject().isOptimisticallyModifiedUnsafe()) {
                        return accessFunction.access(o);
                    }
                    // Otherwise, we need to roll forward or backward.
                    // First undo any optimistic changes.
                    if (proxy.getUnderlyingObject().isOptimisticallyModifiedUnsafe()) {
                        try {
                            proxy.getUnderlyingObject().optimisticRollbackUnsafe();
                        } catch (NoRollbackException nre) {
                            // guess our only option is to start from scratch.
                            proxy.resetObjectUnsafe(proxy.getUnderlyingObject());
                        }
                    }
                    // Next check the version, if it is ahead, try undo
                    // We don't support this yet, so we just reset
                    if (proxy.getVersion() > getSnapshotTimestamp()) {
                        proxy.resetObjectUnsafe(proxy.getUnderlyingObject());
                    }

                    // Now we sync forward if we are behind
                    if (proxy.getVersion() < getSnapshotTimestamp()) {
                        proxy.syncObjectUnsafe(proxy.getUnderlyingObject(),
                                getSnapshotTimestamp());
                    }

                    // Now we do the access
                    return accessFunction.access(proxy.getUnderlyingObject()
                            .getObjectUnsafe());
                }
        );
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
        throw new UnsupportedOperationException("Can't modify object during a read-only transaction!");
    }

    @Override
    public void addTransaction(AbstractTransactionalContext tc) {
        throw new UnsupportedOperationException("Can't merge into a readonly txn (yet)");
    }

    @Override
    public long obtainSnapshotTimestamp() {
        return getBuilder().getSnapshot();
    }


}
