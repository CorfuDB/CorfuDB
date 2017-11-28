package org.corfudb.runtime.object.transactions;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import lombok.Getter;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.IObjectManager;
import org.corfudb.runtime.object.IStateMachineOp;
import org.corfudb.runtime.object.IStateMachineStream;
import org.corfudb.runtime.view.Address;

/**
 * A snapshot transactional context.
 *
 * <p>Given the snapshot (log address) given by the TransactionBuilder,
 * access all objects within the same snapshot during the course of
 * this transactional context.
 *
 * <p>Created by mwei on 11/22/16.
 */
public class SnapshotTransaction extends AbstractTransaction {


    /** A SnapshotStateMachineStream "locks" the parent stream to a specific snapshot.
     *  Note that the snapshot address might not be the same as the version of the object.
     *  For example, the object may have updates at address 5 and 10. Any snapshot address between
     *  5 and 10, inclusive will result in the object at address (version 5). This implementation
     *  remembers the version of the object it discovers, and updates the snapshot address accordingly.
     *  This reduces the need for unnecessary syncs and reads.
     */
    public static class SnapshotStateMachineStream implements IStateMachineStream {

        /**
         * The parent stream which a snapshot will be "locked" over.
         */
        @Getter
        final IStateMachineStream parent;

        /**
         * The snapshot address. Initially, this will be the address of the snapshot request,
         * but after the first sync will be updated with the "lowest" address necessary to obtain
         * that snapshot.
         */
        private long snapshotAddress;

        /**
         * Create a new {@link Class}.
         *
         * @param parent  The parent stream to obtain a snapshot over.
         * @param address The address of the snapshot.
         */
        SnapshotStateMachineStream(@Nonnull IStateMachineStream parent, long address) {
            this.parent = parent;
            snapshotAddress = address;
        }

        /**
         * {@inheritDoc}
         */
        @Nonnull
        @Override
        public Stream<IStateMachineOp> sync(long pos, @Nullable Object[] conflictObjects) {
            // We don't have any optimistic updates to undo
            if (pos == Address.OPTIMISTIC) {
                return Stream.empty();
                // We only support syncing to Address.MAX
            } else if (pos != Address.MAX) {
                throw new UnsupportedOperationException("Snapshot stream cannot sync to position " + pos);
                // If we are at the right position, there's nothing to do
            } else if (parent.pos() == snapshotAddress) {
                return Stream.empty();
                // Otherwise, move the parent to the correct position
            } else {
                Stream<IStateMachineOp> stream = parent.sync(snapshotAddress, conflictObjects);
                // Remember the absolute pos (which can be < the original snapshot address).
                snapshotAddress = parent.pos();
                return stream;
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long pos() {
            return parent.pos();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void reset() {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long check() {
            return parent.pos() == snapshotAddress ? Address.UP_TO_DATE : Address.MAX;
        }

        /**
         * {@inheritDoc}
         * Unsupported operation.
         */
        @Override
        public CompletableFuture<Object> append(@Nonnull String smrMethod,
                           @Nonnull Object[] smrArguments,
                           @Nullable Object[] conflictObjects, boolean returnUpcall) {
            throw new UnsupportedOperationException("Snapshot stream doesn't support append");
        }

        /**
         * {@inheritDoc}
         * Unsupported operation.
         */
        @Override
        @Nullable
        public Object getUpcallResult(long address) {
            throw new UnsupportedOperationException("Snapshot stream cannot keep entries");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public UUID getId() {
            return parent.getId();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SnapshotStateMachineStream that = (SnapshotStateMachineStream) o;
            return snapshotAddress == that.snapshotAddress
                    && Objects.equals(parent, that.parent);
        }

        @Override
        public int hashCode() {
            return Objects.hash(parent, snapshotAddress);
        }
    }

    final long previousSnapshot;

    SnapshotTransaction(@Nonnull TransactionBuilder builder,
                        @Nonnull TransactionContext context) {
        super(builder, context);
        previousSnapshot = context.getReadSnapshot();
        context.setReadSnapshot(builder.snapshot);
    }

    @Override
    public IStateMachineStream getStateMachineStream(@Nonnull IObjectManager manager,
                                                     @Nonnull IStateMachineStream current) {
        if (current.equals(this)) {
            return current;
        }
        return new SnapshotStateMachineStream(current.getRoot(), context.getReadSnapshot());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long commit() throws TransactionAbortedException {
        // Restore the previous snapshot on commit.
        context.setReadSnapshot(previousSnapshot);
        return super.commit();
    }

}
