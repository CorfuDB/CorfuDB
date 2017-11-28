package org.corfudb.runtime.object.transactions;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.object.IObjectManager;
import org.corfudb.runtime.object.IStateMachineOp;
import org.corfudb.runtime.object.IStateMachineStream;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.ObjectBuilder;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

/** An optimistic state machine stream which supports recording updates into the
 *  write set of a transaction, and undoing those updates when syncing to
 *  {@link org.corfudb.runtime.view.Address.OPTIMISTIC} is requested.
 *
 *  <p>Reads are redirected to the snapshot requested. This is done by the
 *  {@link SnapshotTransaction.SnapshotStateMachineStream} this class extends from.
 *
 *  <p>This is an abstract class.
 *  {@link ReadAfterWriteTransaction.ReadAfterWriteStateMachineStream},
 *  {@link WriteAfterWriteTransaction.WriteAfterWriteStateMachineStream}
 *  are concrete implementations that record conflict information as needed as the stream is
 *  manipulated.
 */
public abstract class AbstractOptimisticStateMachineStream extends
        SnapshotTransaction.SnapshotStateMachineStream {

    /** The pointer into the optimistic write set. */
    long optimisticPos = Address.NEVER_READ;

    /** The manager for the object for the stream. */
    protected final IObjectManager<?> manager;

    /** The transaction context of the writer. */
    final TransactionContext writerContext;

    /** Construct a new {@link Class}.
     *
     * @param manager   The manager for the stream.
     * @param parent    The parent stream.
     * @param context   The transaction context this stream is built over.
     * @param address   The address a snapshot will be obtained over.
     */
    public AbstractOptimisticStateMachineStream(@Nonnull IObjectManager<?> manager,
                                                @Nonnull IStateMachineStream parent,
                                                @Nonnull TransactionContext context,
                                                long address) {
        super(parent, address);
        this.manager = manager;
        this.writerContext = context;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Stream<IStateMachineOp> sync(long pos, @Nullable Object[] conflictObjects) {
        final List<SMREntry> updateList = writerContext.getWriteSet().getWriteSet()
                .getSMRUpdates(parent.getId());
        final long previousPos = optimisticPos;

        // Revert all optimistic updates
        if (pos == Address.OPTIMISTIC) {
            // If there are no optimistic updates (or updates were never applied),
            // we don't need to do anything!
            if (optimisticPos == Address.NEVER_READ || optimisticPos == 0) {
                optimisticPos = Address.NEVER_READ;
                return Stream.empty();
            }

            // Reset the optimistic position
            optimisticPos = Address.NEVER_READ;

            // If the optimistic updates have been committed, "rollback" means just
            // syncing forward to the commit position, ignoring the write set at this
            // address.
            if (writerContext.getCommitAddress() != Address.OPTIMISTIC
                    && writerContext.getCommitAddress() != Address.ABORTED) {
                return super.getRoot().sync(writerContext.getCommitAddress(), null)
                        .filter(x -> x.getAddress() != writerContext.getCommitAddress());
            }

            // Otherwise, build a reversed stream over list of optimistic undo records.
            return IntStream.rangeClosed(1, (int)previousPos)
                    .mapToObj(i -> updateList.get((int)previousPos - i).getUndoOperation());
        } else if (pos != Address.MAX) {
            throw new UnsupportedOperationException("Optimistic stream cannot sync to position");
        }

        optimisticPos = updateList.size();
        if (previousPos == Address.NEVER_READ) {
            return Stream.concat(super.sync(pos, conflictObjects), updateList.stream());
        }

        return updateList.subList((int) previousPos, updateList.size()).stream()
                .map(x -> (IStateMachineOp) x);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long check() {
        long base = super.check();
        if (base == Address.UP_TO_DATE
                && (optimisticPos == Address.NEVER_READ
                    || optimisticPos == writerContext.getWriteSet().getWriteSet()
                .getSMRUpdates(parent.getId()).size())) {
            return Address.UP_TO_DATE;
        }
        return Address.MAX;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Object> append(@Nonnull String smrMethod,
                       @Nonnull Object[] smrArguments,
                       @Nullable Object[] conflictObjects, boolean returnUpcall) {
        SMREntry smrEntry = new SMREntry(smrMethod, smrArguments,
                ((ObjectBuilder)manager.getBuilder()).getSerializer());
        writerContext.getWriteSet().add(manager, smrEntry, conflictObjects);
        if (returnUpcall) {
            CompletableFuture cf = new CompletableFuture();
            smrEntry.setUpcallConsumer(cf::complete);
            return cf;
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        AbstractOptimisticStateMachineStream that = (AbstractOptimisticStateMachineStream) o;
        return optimisticPos == that.optimisticPos
                && Objects.equals(manager, that.manager)
                && Objects.equals(writerContext, that.writerContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), optimisticPos, manager, writerContext);
    }
}
