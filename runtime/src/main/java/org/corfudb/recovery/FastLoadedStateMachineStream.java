package org.corfudb.recovery;

import com.google.common.collect.Streams;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.runtime.object.ICorfuWrapper;
import org.corfudb.runtime.object.IStateMachineOp;
import org.corfudb.runtime.object.IStateMachineStream;
import org.corfudb.runtime.view.Address;

import lombok.Data;

public class FastLoadedStateMachineStream implements IStateMachineStream {

    UUID id;

    IStateMachineStream parent;

    final List<FastOpWrapper> updates;

    long maxLearned = Address.NEVER_READ;
    long maxRead = Address.NEVER_READ;

    public FastLoadedStateMachineStream(UUID id, IStateMachineStream parent) {
        this.id = id;
        this.parent = parent;
        updates = new ArrayList<>();
    }

    @Data
    static class FastOpWrapper implements IStateMachineOp {

        public final SMREntry originalOp;
        public final long address;

        @Override
        public <T> T apply(ICorfuWrapper<T> wrapper, T object) {
            return originalOp.apply(wrapper, object);
        }

        @Override
        public void setUpcallConsumer(@Nonnull Consumer<Object> consumer) {
            originalOp.setUpcallConsumer(consumer);
        }

        IStateMachineOp getUndoOperation() {
            return originalOp.getUndoOperation();
        }
    }
    void learnEntry(SMREntry op, long address) {
        updates.add(new FastOpWrapper(op, address));
        maxLearned = Math.max(maxLearned, address);
    }

    @Nonnull
    @Override
    public Stream<IStateMachineOp> sync(long pos, @Nullable Object[] conflictObjects) {
        final long prevRead = maxRead;

        if (pos < maxLearned) {
            maxRead = pos;
            if (pos < prevRead) {
                // Undo previous ops
               return IntStream.rangeClosed(1, updates.size())
                        .mapToObj(i -> updates.get(updates.size() - i))
                        .filter(x -> x.getAddress() > pos)
                        .map(FastOpWrapper::getUndoOperation);
            }
            return updates.stream()
                    .filter(x -> x.getAddress() <= pos)
                    .map(x -> (IStateMachineOp) x);
        } else {
            if (maxRead < maxLearned) {
                maxRead = maxLearned;
                parent.seek(maxLearned + 1);
                return Streams.concat(updates.stream()
                        .filter(x -> x.getAddress() < pos
                                && x.getAddress() > prevRead)
                        .map(x -> (IStateMachineOp) x), parent.sync(pos, conflictObjects));
            }
            return parent.sync(pos, conflictObjects);
        }
    }

    @Override
    public long pos() {
        return (maxRead < maxLearned) ? maxLearned : parent.pos();
    }

    @Override
    public void reset() {
        parent.seek(maxLearned);
    }

    @Override
    public void seek(long pos) {
        if (pos < maxLearned) {
            maxRead = pos;
        } else {
            maxLearned = pos;
            parent.seek(pos);
        }
    }

    @Override
    public CompletableFuture<Object> append(@Nonnull String smrMethod,
                                            @Nonnull Object[] smrArguments,
                            Object[] conflictObjects, boolean returnUpcall) {
        return parent.append(smrMethod, smrArguments, conflictObjects, returnUpcall);
    }

    @Override
    @Nullable
    public Object getUpcallResult(long address) {
        return parent.getUpcallResult(address);
    }

    /**
     * Return the parent, which is null, since this stream must be the new root
     * @return  The parent stream, which is null.
     */
    @Nullable
    @Override
    public IStateMachineStream getParent() {
        return null;
    }

    @Override
    public UUID getId() {
        return id;
    }
}
