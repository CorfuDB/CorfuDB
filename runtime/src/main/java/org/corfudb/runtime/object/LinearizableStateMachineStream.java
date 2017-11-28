package org.corfudb.runtime.object;

import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.logprotocol.ISMRConsumable;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.UnrecoverableCorfuException;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.CFUtils;
import org.corfudb.util.serializer.ISerializer;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.SynchronizedLongObjectMap;

/**
 * An implementation of a state machine stream which has linearizable semantics over
 * a stream in the Corfu log.
 *
 * <p>This stream guarantees that all calls to
 * {@link #append(String, Object[], Object[], boolean)} ordered before a
 * {@link #sync(long, Object[])} to {@link org.corfudb.runtime.view.Address.MAX} in
 * real "wall clock" time are observed by that call to {@link #sync(long, Object[])}. In other
 * words, the stream returned by {@link #sync(long, Object[])} reflects all
 * {@link #sync(long, Object[])} which happens before it.
 */
public class LinearizableStateMachineStream implements IStateMachineStream {

    /** The stream view that this stream sources its updates from.
     *
     */
    private final IStreamView streamView;

    /** A reference to the runtime, which is used for deserialization.
     */
    private final CorfuRuntime runtime;

    /** A serializer to use for appending entries into the log. */
    private final ISerializer serializer;

    /** A map which keeps track of entries which have been requested to be saved for calls to
     * {@link #getUpcallResult(long)}.
     */
    final MutableLongObjectMap<CompletableFuture<Object>> entryMap =
        new SynchronizedLongObjectMap<>(new LongObjectHashMap<>());

    /** Generate a new linearizable state machine stream from a streamview.
     *
     * @param runtime       A runtime to use.
     * @param streamView    A stream view to obtain state machine updates from.
     * @param serializer    A serializer to use to encode updates into the stream.
     */
    public LinearizableStateMachineStream(CorfuRuntime runtime,
                                          IStreamView streamView,
                                          ISerializer serializer) {
        this.runtime = runtime;
        this.streamView = streamView;
        this.serializer = serializer;
    }

    private List<SMREntry> dataAndCheckpointMapper(ILogData logData) {
        if (logData.hasCheckpointMetadata()) {
            // This is a CHECKPOINT record.  Extract the SMREntries, if any.
            CheckpointEntry cp = (CheckpointEntry) logData.getPayload(runtime);
            if (cp.getSmrEntries() != null
                    && cp.getSmrEntries().getUpdates().size() > 0) {
                cp.getSmrEntries().getUpdates().forEach(e -> {
                    e.setRuntime(runtime);
                    e.setEntry(logData);
                });
                return cp.getSmrEntries().getUpdates();
            } else {
                return Collections.emptyList();
            }
        } else {
            return ((ISMRConsumable) logData.getPayload(runtime))
                    .getSMRUpdates(streamView.getId());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long pos() {
        return streamView.getCurrentGlobalPosition();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reset() {
        streamView.reset();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long check() {
        TokenResponse tr =  runtime.getSequencerView().query(streamView.getId());
        return tr.getTokenValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void seek(long globalAddress) {
        streamView.seek(globalAddress);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public Stream<IStateMachineOp> sync(long pos, @Nullable Object[] conflictObjects) {
        if (pos == pos()) {
            return Stream.empty();
        } else if (pos < pos()) {
            return StreamSupport.stream(new Spliterator<IStateMachineOp>() {
                ListIterator<SMREntry> currentIterator = null;

                @Override
                public boolean tryAdvance(Consumer<? super IStateMachineOp> action) {
                    if (currentIterator == null || !currentIterator.hasPrevious()) {
                        if (streamView.getCurrentGlobalPosition() <= pos) {
                            if (pos == Address.NEVER_READ) {
                                streamView.reset();
                            }
                            return false;
                        }
                        ILogData data = streamView.current();
                        if (data == null) {
                            if (pos == Address.NEVER_READ) {
                                streamView.reset();
                            }
                            return false;
                        }
                        while (true) {
                            if (data.getType() == DataType.DATA
                                    && data.getPayload(runtime) instanceof ISMRConsumable) {
                                List<SMREntry> list = ((ISMRConsumable) data.getPayload(runtime))
                                        .getSMRUpdates(streamView.getId());
                                currentIterator = list.listIterator(list.size());
                                data = streamView.previous();
                                if (data == null) {
                                    if (pos == Address.NEVER_READ) {
                                        streamView.reset();
                                    }
                                }
                                break;
                            }

                            data = streamView.previous();
                            if ((data == null
                                    || streamView.getCurrentGlobalPosition() <= pos)) {
                                if (pos == Address.NEVER_READ) {
                                    streamView.reset();
                                }
                                return false;
                            }
                        }
                    }

                    SMREntry entry = currentIterator.previous();
                    action.accept(entry.getUndoOperation());
                    return true;
                }

                @Override
                public Spliterator<IStateMachineOp> trySplit() {
                    return null; // Cannot be split
                }

                @Override
                public long estimateSize() {
                    return pos;
                }

                @Override
                public int characteristics() {
                    return Spliterator.ORDERED | Spliterator.DISTINCT
                            | Spliterator.IMMUTABLE | Spliterator.NONNULL;
                }
            }, false);
        } else {
            return streamView.streamUpTo(pos)
                    .filter(m -> m.getType() == DataType.DATA)
                    .filter(m -> m.getPayload(runtime) instanceof ISMRConsumable
                            || m.hasCheckpointMetadata())
                    .map(this::dataAndCheckpointMapper)
                    .flatMap(List::stream)
                    .map(x -> {
                        CompletableFuture<Object> cf = entryMap.get(x.getAddress());
                        if (cf != null) {
                            x.setUpcallConsumer(cf::complete);
                        }
                        return x;
                    });
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<Object> append(@Nonnull String smrMethod,
                       @Nonnull Object[] smrArguments,
                       @Nullable Object[] conflictObjects,
                       final boolean returnUpcall) {
        SMREntry entry = new SMREntry(smrMethod, smrArguments, serializer);
        if (returnUpcall) {
            CompletableFuture<Object> cf = new CompletableFuture<>();
            streamView.append(entry, t -> {
                entryMap.put(t.getTokenValue(), cf);
                return true;
            }, t -> {
                entryMap.remove(t.getTokenValue());
                return true;
            });
            return cf;
        } else {
            streamView.append(entry, t -> true, t -> true);
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UUID getId() {
        return streamView.getId();
    }

}
