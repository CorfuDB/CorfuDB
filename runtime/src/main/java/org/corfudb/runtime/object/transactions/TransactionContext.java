package org.corfudb.runtime.object.transactions;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.runtime.object.IObjectManager;
import org.corfudb.runtime.object.IStateMachineStream;
import org.corfudb.runtime.view.Address;

/** A transaction context contains all the thread-local transaction context
 *  for a given thread. This includes the write set, the conflict set and
 *  the snapshot address.
 */
public class TransactionContext {

    /** The set of writes the current transaction has made. */
    @Getter
    final WriteSet writeSet = new WriteSet();

    /** The set of conflicts the transaction has made. */
    @Getter
    final ConflictSet conflictSet = new ConflictSet();

    /** A cache of state machine streams generated to service the current transaction. */
    @Getter(value = AccessLevel.PACKAGE)
    final Map<IObjectManager, IStateMachineStream> streamMap = new HashMap<>();

    /** The snapshot that all reads are being made against. */
    @Getter
    @Setter(value = AccessLevel.PACKAGE)
    long readSnapshot = Address.NO_SNAPSHOT;

    /** The address the context was committed at,
     * {@link org.corfudb.runtime.view.Address.OPTIMISTIC} if the write set has not yet
     * been committed, or {@link org.corfudb.runtime.view.Address.ABORTED} if the transaction
     * was aborted.
     */
    @Getter
    @Setter(value = AccessLevel.PACKAGE)
    long commitAddress = Address.OPTIMISTIC;

    /** The currently active transaction.
     */
    @Getter
    @Setter(value = AccessLevel.PACKAGE)
    AbstractTransaction current = null;

    /** Returns whether this context is the active context. There is exactly one
     * active context per thread.
     * @return
     */
    boolean isActive() {
        return Transactions.getContext().equals(this);
    }

    /** Get a state machine stream for the given object manager using the current transaction
     * and the given stream.
     * @param manager           The object manager to obtain a state machine stream for.
     * @param currentStream     The currently active stream.
     * @return                  A state machine stream for the given object manager.
     */
    public @Nonnull IStateMachineStream getStateMachineStream(@Nonnull IObjectManager manager,
                                            @Nonnull IStateMachineStream currentStream) {
        return streamMap.compute(manager,
                (k,v) -> current.getStateMachineStream(manager, v == null ? currentStream : v));
    }
}
