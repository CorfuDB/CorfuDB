package org.corfudb.runtime.stream;

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
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

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
    final transient AtomicLong streamPointer = new AtomicLong();

    /** A cache of pointers (contiguous) for this stream */
    final transient ConcurrentLinkedQueue<Long> nextPointers = new ConcurrentLinkedQueue<Long>();

    /** The hole filling policy to apply on this stream */
    @Getter
    @Setter
    transient IHoleFillingPolicy holeFillingPolicy = new TimeoutHoleFillPolicy();

    /** A reference to the instance. Upon deserialization, this needs to be restored.
     *  TODO: maybe this needs to be retrieved from TLS.
     */
    final transient ICorfuDBInstance instance;

    /**
     * Append an object to the stream. This operation may or may not be successful. For example,
     * a move operation may occur, and the append will not be part of the stream.
     *
     * @param data A serializable object to append to the stream.
     * @return A timestamp, which reflects the physical position and the epoch the data was written in.
     */
    @Override
    public ITimestamp append(Serializable data) throws OutOfSpaceException, IOException {
        return IRetry.build(ExponentialBackoffRetry.class, OutOfSpaceException.class, () -> {
            long nextToken = instance.getStreamingSequencer().getNext(streamID);
            instance.getStreamAddressSpace().write(nextToken, Collections.singleton(streamID), data);
            return new SimpleTimestamp(nextToken);
        }).onException(OverwriteException.class, (e,r) -> {
            log.debug("Tried to write to " + e.address + " but overwrite occured, retrying...");
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
        return new ITimestamp[] {new SimpleTimestamp(instance.getStreamingSequencer().getNext(streamID))};
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
    public IStreamEntry readNextEntry() throws HoleEncounteredException, TrimmedException, IOException {
        /* We have several options here:
            1) If we have pointers to the next entry in our cache, we need to use them.
            2) Get information from the streaming sequencer. Here we can use backpointers
                if we are relatively close to the end of the stream.
            3) Otherwise, read from the beginning of the global index, using next pointers
                if we can find them...
         */

        // Here, we read using our cache.
        Long nextPointer = nextPointers.poll();
        if (nextPointer != null)
        {
            // Increment the stream pointer. The pointer may have been updated by another
            // thread which is ahead of us, so we use the max.
            streamPointer.getAndUpdate(i -> Math.max(i, nextPointer));

            // We assume that next pointers are correct. If for some reason they are not,
            // we're going to have issues. Fetch the next item . If the item was trimmed,
            // we need to return that error to the client, which needs to figure out where
            // the next non-trimmed section is.
            IStreamAddressSpace.StreamAddressSpaceEntry e = instance.getStreamAddressSpace().read(nextPointer);

            // Check to make sure that our stream belongs to this entry. If not, this
            // is a serious problem.
            if (!e.containsStream(streamID))
            {
                log.error("Retrieved entry " + nextPointer + " from pointer cache but it did not belong to this stream!");
                throw new RuntimeException("Incorrect stream entry for "+ streamID + " at " + nextPointer);
            }
            return e;
        }

        // Nothing was in the cache so we ask the streaming sequencer.
        // TODO: fix streaming sequencer implementation.
        long currentPointer = instance.getStreamingSequencer().getCurrent(streamID);
        long nextRead;
        // The streaming sequencer wasn't able to give us any information, and we have
        // no next pointers. scan ahead...
        while ((nextRead = streamPointer.getAndIncrement()) < currentPointer)
        {
            final long thisRead = nextRead;
            IStreamAddressSpace.StreamAddressSpaceEntry e = IRetry.build(ExponentialBackoffRetry.class, () -> {
                IStreamAddressSpace.StreamAddressSpaceEntry r = instance.getStreamAddressSpace().read(thisRead);
                if (r == null) {
                    throw new HoleEncounteredException(new SimpleTimestamp(thisRead));
                }
                return r;
            }).onException(HoleEncounteredException.class, ex -> {
                log.info("Hole encountered at address={}, applying hole filling policy.", ex.getAddress());
                holeFillingPolicy.apply(ex, this);
                return true;
            }).run();

            if (e.containsStream(streamID) && e.getCode() != IStreamAddressSpace.StreamAddressEntryCode.HOLE)
            {
                return e;
            }
            // TODO: If the read doesn't belong to our stream, we should be able to tell
            // other streams this is an entry for them?...

        }

        // There were no stream entries belonging to us left in the log to retrieve.
        return null;
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
        return null;
    }

    /**
     * Given a timestamp, get a proceeding timestamp in the stream.
     *
     * @param ts The timestamp to decrement.
     * @return The previous timestamp in the stream, or null, if there are no previous timestamps in the stream.
     */
    @Override
    public ITimestamp getPreviousTimestamp(ITimestamp ts) {
        return null;
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
        return null;
    }

    /**
     * Gets the current position the stream has read to (which may not point to an entry in the
     * stream).
     *
     * @return A timestamp, which reflects the most recently read address in the stream.
     */
    @Override
    public ITimestamp getCurrentPosition() {
        if (streamPointer.get() == 0)
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

    protected long toPhysicalTimestamp(ITimestamp timestamp)
    {
        if (timestamp instanceof SimpleTimestamp)
        {
            return ((SimpleTimestamp) timestamp).address;
        }
        throw new RuntimeException("Unknown timestamp type " + timestamp.getClass() + " given!");
    }
}
