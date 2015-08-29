package org.corfudb.runtime.stream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.HoleEncounteredException;
import org.corfudb.runtime.OutOfSpaceException;
import org.corfudb.runtime.OverwriteException;
import org.corfudb.runtime.TrimmedException;
import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.view.ICorfuDBInstance;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This is the new stream implementation.
 * It relies on several components:
 * A stream address space (where the address space keeps track of streams)
 * * Consequently, this requires new logging units which track streams.
 * A new streaming sequencer.
 * Created by mwei on 8/28/15.
 */

@Slf4j
@RequiredArgsConstructor
public class NewStream implements IStream {

    final UUID streamID;
    final transient AtomicLong streamPointer = new AtomicLong();
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
        while (true) {
            long nextToken = instance.getStreamingSequencer().getNext(streamID);
            try {
                instance.getStreamAddressSpace().writeObject(nextToken, Collections.singleton(streamID), data);
                return new SimpleTimestamp(nextToken);
            } catch (OverwriteException oe) {
                log.debug("Tried to write to " + nextToken + " but overwrite occured, retrying...");
            }
        }
    }

    /**
     * Read the next entry in the stream as a IStreamEntry. This function
     * retrieves the next entry in the stream, or null, if there are no more entries in the stream.
     *
     * @return A CorfuDBStreamEntry containing the payload of the next entry in the stream.
     */
    @Override
    public IStreamEntry readNextEntry() throws HoleEncounteredException, TrimmedException, IOException {
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
        return null;
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
        return null;
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

    }

    /**
     * Close the stream. This method must be called to free resources.
     */
    @Override
    public void close() {

    }

    /**
     * Get the ID of the stream.
     *
     * @return The ID of the stream.
     */
    @Override
    public UUID getStreamID() {
        return streamID;
    }
}
