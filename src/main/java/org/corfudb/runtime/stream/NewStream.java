package org.corfudb.runtime.stream;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.corfudb.runtime.*;
import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.smr.HoleFillingPolicy.IHoleFillingPolicy;
import org.corfudb.runtime.smr.HoleFillingPolicy.TimeoutHoleFillPolicy;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.runtime.view.IStreamAddressSpace;
import org.corfudb.runtime.view.StreamAddressSpace;
import org.corfudb.util.retry.ExponentialBackoffRetry;
import org.corfudb.util.retry.IRetry;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * This is the new stream implementation.
 * It relies on several components:
 * A stream address space (where the address space keeps track of streams)
 * * Consequently, this requires new logging units which track streams.
 * A new streaming sequencer, with backpointer support.
 * Created by mwei on 8/28/15.
 */

@Slf4j
@RequiredArgsConstructor
public class NewStream implements IStream {

    /** The ID of this stream. */
    @Getter
    final UUID streamID;

    /** The position of this stream, in the global log index */
    final transient AtomicLong streamPointer = new AtomicLong(0);

    /** A cache of pointers (contiguous) for this stream */
    final transient ConcurrentLinkedQueue<Long> nextPointers = new ConcurrentLinkedQueue<Long>();

    /** The hole filling policy to apply on this stream */
    @Getter
    @Setter
    transient IHoleFillingPolicy holeFillingPolicy = new TimeoutHoleFillPolicy();

    /** A reference to the instance. Upon deserialization, this needs to be restored.
     *  TODO: maybe this needs to be retrieved from TLS.
     */
    @Getter
    final transient ICorfuDBInstance instance;

    /** The batch counter for async reads. */
    final transient AtomicLong batchNumber = new AtomicLong();

    /**
     * Append an object to the stream. This operation may or may not be successful. For example,
     * a move operation may occur, and the append will not be part of the stream.
     *
     * @param data A serializable object to append to the stream.
     * @return A timestamp, which reflects the physical position and the epoch the data was written in.
     */
    @Override
    public ITimestamp append(Serializable data) throws IOException {
        return IRetry.build(ExponentialBackoffRetry.class, OutOfSpaceException.class, () -> {
            long nextToken = instance.getStreamingSequencer().getNext(streamID);
            instance.getStreamAddressSpace().write(nextToken, Collections.singleton(streamID), data);
            return new SimpleTimestamp(nextToken);
        }).onException(OverwriteException.class, (e,r) -> {
            log.debug("Tried to write to " + e.address + " but overwrite occurred, retrying...");
            return true;
        })
        .run();
    }

    /**
     * Reserves a given number of timestamps in this stream. This operation may or may not retrieve
     * valid timestamps. For example, a move operation may occur and these timestamps will not be valid on
     * the stream.
     *
     * @param numTokens The number of tokens to allocate.
     * @return A set of timestamps representing the tokens to allocate.
     */
    @Override
    public ITimestamp[] reserve(int numTokens) throws IOException {
        try {
            return reserveAsync(numTokens).get();
        } catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reserves a given number of timestamps in this stream. This operation may or may not retrieve
     * valid timestamps. For example, a move operation may occur and these timestamps will not be valid on
     * the stream.
     *
     * @param numTokens The number of tokens to allocate.
     * @return A set of timestamps representing the tokens to allocate.
     */
    @Override
    public CompletableFuture<ITimestamp[]> reserveAsync(int numTokens) {
        return instance.getNewStreamingSequencer().nextTokenAsync(streamID,numTokens)
                .thenApply(t -> {
                    ITimestamp[] r = new ITimestamp[numTokens];
                    for (int i = 0; i < numTokens; i++)
                    {
                        r[i] = toLogicalTimestamp(t + i);
                    }
                    return r;
                });
    }

    /**
     * Write to a specific, previously allocated log position.
     *
     * @param timestamp The timestamp to write to.
     * @param data      The data to write to that timestamp.
     * @throws OutOfSpaceException If there is no space left to write to that log position.
     * @throws OverwriteException  If something was written to that log position already.
     */
    @Override
    public void write(ITimestamp timestamp, Serializable data) throws OutOfSpaceException, OverwriteException, IOException {
        instance.getStreamAddressSpace().write(toPhysicalTimestamp(timestamp), Collections.singleton(streamID), data);
    }

    /**
     * Read the next entry in the stream as a IStreamEntry. This function
     * retrieves the next entry in the stream, or null, if there are no more entries in the stream.
     *
     * @return A CorfuDBStreamEntry containing the payload of the next entry in the stream.
     */
    @Override
    public synchronized IStreamEntry readNextEntry() throws HoleEncounteredException, TrimmedException, IOException {
        throw new UnsupportedOperationException("readNextEntry no longer supported (use readToAsync!)");
    }

    /**
     * Given a timestamp, reads the entry at the timestamp
     *
     * @param timestamp The timestamp to read from.
     * @return The entry located at that timestamp.
     */
    @Override
    public IStreamEntry readEntry(ITimestamp timestamp) throws HoleEncounteredException, TrimmedException, IOException {
        long address = ((SimpleTimestamp) timestamp).address;
        StreamAddressSpace.StreamAddressSpaceEntry e = instance.getStreamAddressSpace().read(address);
        // make sure that this entry was a part of THIS stream.
        if (!e.containsStream(streamID))
        {
            throw new NonStreamEntryException("Requested entry but it was not part of this stream!", address);
        }
        return e;
    }

    /**
     * Given a timestamp, get the timestamp in the stream
     *
     * @param ts The timestamp to increment.
     * @return The next timestamp in the stream, or null, if there are no next timestamps in the stream.
     */
    @Override
    public ITimestamp getNextTimestamp(ITimestamp ts) {
        if (ts instanceof SimpleTimestamp)
        {
            return new SimpleTimestamp(((SimpleTimestamp)ts).address+1);
        }
        return LogicalAsyncTimestamp.getNextTimestamp(ts);
    }

    /**
     * Given a timestamp, get a proceeding timestamp in the stream.
     *
     * @param ts The timestamp to decrement.
     * @return The previous timestamp in the stream, or null, if there are no previous timestamps in the stream.
     */
    @Override
    public ITimestamp getPreviousTimestamp(ITimestamp ts) {
        return LogicalAsyncTimestamp.getPreviousTimestamp(ts);
    }

    /**
     * Given a timestamp, get the first timestamp in the stream.
     *
     * @return The first timestamp in the stream, or null, if there is no first timestamps in the stream.
     */
    @Override
    public ITimestamp getFirstTimestamp() {
        return new LogicalAsyncTimestamp(0, 0, Long.MAX_VALUE);
    }

    /**
     * Attempts to fill a hole at the given timestamp.
     *
     * @param ts A timestamp to fill a hole at.
     * @return True, if the hole was successfully filled, false otherwise.
     */
    @Override
    public boolean fillHole(ITimestamp ts) {
        instance.getStreamAddressSpace().fillHole(toPhysicalTimestamp(ts));
        return true;
    }

    /**
     * Returns a fresh or cached timestamp, which can serve as a linearization point. This function
     * may return a non-linearizable (invalid) timestamp which may never occur in the ordering
     * due to a move/epoch change.
     *
     * @param cached Whether or not the timestamp returned is cached.
     * @return A timestamp, which reflects the most recently allocated timestamp in the stream,
     * or currently read, depending on whether cached is set or not.
     */
    @Override
    public ITimestamp check(boolean cached) {
        return toLogicalTimestamp(streamPointer.get());
    }

    /**
     * Asynchronously returns a new timestamp, which can serve as a linearization point.
     *
     * @return A completable future, which will return a timestamp when completed.
     */
    @Override
    public CompletableFuture<ITimestamp> checkAsync() {
        return instance.getNewStreamingSequencer().nextTokenAsync(this.getStreamID(), 0)
                .thenApply(SimpleTimestamp::new);
    }

    protected CompletableFuture<IStreamEntry> readAtAddress(long address) {
        return instance.getStreamAddressSpace().readAsync(address)
                .thenApplyAsync(
                        e -> {
                            if (e == null) {
                                e = IRetry.build(ExponentialBackoffRetry.class, () -> {
                                    IStreamAddressSpace.StreamAddressSpaceEntry ret =
                                            instance.getStreamAddressSpace().read(address);
                                    if (ret == null) {
                                        throw new HoleEncounteredException(toLogicalTimestamp(address));
                                    }
                                    return ret;
                                }).onException(HoleEncounteredException.class, he -> {
                                    holeFillingPolicy.apply(he, this);
                                    return true;
                                })
                                        .run();
                            }

                            if (e.containsStream(streamID)) {
                                return e;
                            }

                            return null;
                        });
    }

    @Override
    public CompletableFuture<IStreamEntry[]> readToAsync(ITimestamp point) {
        /** TODO: maybe use a lock here to improve performance */
        long startPoint;
        long batch;
        synchronized (batchNumber) {
            batch = batchNumber.getAndIncrement();
            startPoint = streamPointer.getAndAccumulate(toPhysicalTimestamp(point), Math::max);
        }
        if (startPoint > toPhysicalTimestamp(point)){
            return CompletableFuture.completedFuture(null);
        }
        else
        {
            List<CompletableFuture<IStreamEntry>> requestList = new ArrayList<>();
            final AtomicLong logicalCounter = new AtomicLong(0);
            for (long i = startPoint; i < toPhysicalTimestamp(point); i++)
            {
                requestList.add(readAtAddress(i));
            }
            return CompletableFuture.allOf(requestList.toArray(new CompletableFuture[requestList.size()]))
                    .thenApply(v ->
                            {
                                IStreamEntry[] rl = requestList.stream()
                                        .map(CompletableFuture::join)
                                        .filter(x -> x != null)
                                        .toArray(IStreamEntry[]::new);
                                int entriesRead = rl.length;

                                Arrays.stream(rl)
                                        .forEach(x -> x.setLogicalTimestamp(
                                                new LogicalAsyncTimestamp(
                                                        batch,
                                                        logicalCounter.getAndIncrement(),
                                                        entriesRead)));

                                if (rl.length == 0)
                                {
                                    //this batch was empty but we need to expire this batch
                                    rl = new IStreamEntry[] {
                                      new IStreamAddressSpace.StreamAddressSpaceEntry<>(
                                              Collections.emptySet(),
                                              Long.MIN_VALUE,
                                              IStreamAddressSpace.StreamAddressEntryCode.EMPTY_BATCH,
                                              null
                                      )
                                    };

                                    rl[0].setLogicalTimestamp(
                                            new LogicalAsyncTimestamp(
                                            batch,
                                            0,
                                            0));
                                }
                                return rl;
                            }
                    );
        }
    }

    /**
     * Gets the current position the stream has read to (which may not point to an entry in the
     * stream).
     *
     * @return A timestamp, which reflects the most recently read address in the stream.
     */
    @Override
    public ITimestamp getCurrentPosition() {
        if (streamPointer.get() <= 0)
        {
            return ITimestamp.getMinTimestamp();
        }
        return new SimpleTimestamp(streamPointer.get()-1);
    }

    /**
     * Requests a trim on this stream. This function informs the configuration master that the
     * position on this stream is trimmable, and moves the start position of this stream to the
     * new position.
     *
     * @param address
     */
    @Override
    public void trim(ITimestamp address) {
        instance.getStreamAddressSpace().trim(streamID, toPhysicalTimestamp(address));
    }

    /**
     * Close the stream. This method must be called to free resources.
     */
    @Override
    public void close() {

    }

    /**
     * Move the stream pointer to the given position.
     *
     * @param pos The position to seek to. The next read will occur AFTER this position.
     */
    @Override
    public void seek(ITimestamp pos) {
        log.info("requested seek to " + pos.toString());
        this.streamPointer.set(toPhysicalTimestamp(pos));
    }

    protected long toPhysicalTimestamp(ITimestamp timestamp)
    {
        if (timestamp instanceof SimpleTimestamp)
        {
            return ((SimpleTimestamp) timestamp).address;
        }
        throw new RuntimeException("Unknown timestamp type " + timestamp.getClass() + " given!");
    }

    protected ITimestamp toLogicalTimestamp(long timestamp)
    {
        return new SimpleTimestamp(timestamp);
    }
}
