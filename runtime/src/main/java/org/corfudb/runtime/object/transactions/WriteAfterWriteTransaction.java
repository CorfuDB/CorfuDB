package org.corfudb.runtime.object.transactions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.object.IObjectManager;
import org.corfudb.runtime.object.IStateMachineStream;

/** A write-after-write transactional context.
 *
 * <p>A write-after-write transactional context behaves like an optimistic
 * context, except behavior during commit (for writes):
 *
 *   <p>(1) Reads behave the same as in a regular optimistic
 *     transaction.
 *
 *   <p>(2) Writes in a write-after-write transaction are guaranteed
 *     to commit atomically, if and only if none of the objects
 *     written (the "write set") were modified between the first read
 *     ("first read timestamp") and the time of commit.
 *
 * <p>Created by mwei on 11/21/16.
 */
@Slf4j
public class WriteAfterWriteTransaction
        extends AbstractOptimisticTransaction {

    static final class WriteAfterWriteStateMachineStream extends
    AbstractOptimisticStateMachineStream {

        WriteAfterWriteStateMachineStream(@Nonnull IObjectManager<?> manager,
                                          @Nonnull IStateMachineStream parent,
                                          @Nonnull TransactionContext context,
                                          long address) {
            super(manager, parent, context, address);
        }

        /** {@inheritDoc} */
        @Override
        public long append(@Nonnull String smrMethod,
                           @Nonnull Object[] smrArguments,
                           @Nullable Object[] conflictObjects, boolean keepEntry) {
            writerContext.getConflictSet().add(manager, conflictObjects);
            return super.append(smrMethod, smrArguments, conflictObjects, keepEntry);
        }


    }

    WriteAfterWriteTransaction(@Nonnull TransactionBuilder builder,
                               @Nonnull TransactionContext context) {
        super(builder, context);
        obtainSnapshotTimestamp();
    }

    /** {@inheritDoc} */
    @Override
    public IStateMachineStream getStateMachineStream(@Nonnull IObjectManager manager,
                                                     @Nonnull IStateMachineStream current) {
        if (current.equals(this)) {
            return current;
        }
        return new WriteAfterWriteStateMachineStream(manager, current.getRoot(),
                context, context.getReadSnapshot());
    }



}
