package org.corfudb.runtime.object.transactions;

import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.corfudb.runtime.object.IObjectManager;
import org.corfudb.runtime.object.IStateMachineOp;
import org.corfudb.runtime.object.IStateMachineStream;

/** Create a new read-after-write transaction. Read-after-write transactions conflict
 * when the read set of the transaction conflicts with a write from another transaction.
 */
public class ReadAfterWriteTransaction
        extends AbstractOptimisticTransaction {

    /** A stream for a read after write transaction. */
    static final class ReadAfterWriteStateMachineStream extends
            AbstractOptimisticStateMachineStream {

        /** Construct a new read-after-write state machine stream.
         *
         * @param manager   The manager to create a stream for.
         * @param parent    The parent of this stream.
         * @param address   The snapshot address reads should be locked to.
         */
        ReadAfterWriteStateMachineStream(@Nonnull IObjectManager manager,
                                         @Nonnull IStateMachineStream parent,
                                         @Nonnull TransactionContext context,
                                         long address) {
            super(manager, parent, context, address);
        }

        /**
         * {@inheritDoc}
         */
        @Nonnull
        @Override
        public Stream<IStateMachineOp> sync(long max, @Nullable Object[] conflictObjects) {
            // In read-after-write transactions, reading adds to the conflict set.
            writerContext.getConflictSet().add(manager,  conflictObjects);
            return super.sync(max, conflictObjects);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IStateMachineStream getStateMachineStream(@Nonnull IObjectManager manager,
                                                     @Nonnull IStateMachineStream current) {
        if (current.equals(this)) {
            return current;
        }
        return new ReadAfterWriteStateMachineStream(manager, current.getRoot(), context,
                obtainSnapshotTimestamp());
    }


    /** Construct a new read-after-write transaction.
     *
     * @param builder   The builder for this transaction
     * @param context   The current transaction context
     */
    ReadAfterWriteTransaction(@Nonnull TransactionBuilder builder,
                              @Nonnull TransactionContext context) {
        super(builder, context);
    }

}
