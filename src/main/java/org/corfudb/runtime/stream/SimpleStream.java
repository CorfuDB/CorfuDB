package org.corfudb.runtime.stream;

import org.corfudb.infrastructure.thrift.Hints;
import org.corfudb.runtime.entries.CorfuDBStreamHoleEntry;
import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.entries.SimpleStreamEntry;
import org.corfudb.runtime.exceptions.*;
import org.corfudb.runtime.view.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by mwei on 4/30/15.
 */
public class SimpleStream implements IStream {
    private Logger log = LoggerFactory.getLogger(SimpleStream.class);

    UUID streamID;
    AtomicLong streamPointer;
    transient ICorfuDBInstance instance;

    public SimpleStream(UUID streamID, ICorfuDBInstance instance)
    {
        this.instance = instance;
        this.streamID = streamID;
        this.streamPointer = new AtomicLong();
    }

    /**
     * Append an object to the stream. This operation may or may not be successful. For example,
     * a move operation may occur, and the append will not be part of the stream.
     *
     * @param data A serializable object to append to the stream.
     * @return A timestamp, which reflects the physical position and the epoch the data was written in.
     */
    public ITimestamp append(Serializable data) throws OutOfSpaceException, IOException {
        long sequence = instance.getSequencer().getNext();
        SimpleTimestamp timestamp = new SimpleTimestamp(sequence);
        instance.getAddressSpace().write(sequence, new SimpleStreamEntry(streamID, data, timestamp));
        return timestamp;
    }

    /**
     * Append an object to the stream. This operation may or may not be successful. For example,
     * a move operation may occur, and the append will not be part of the stream.
     *
     * @param data A serializable object to append to the stream.
     * @return A timestamp, which reflects the physical position and the epoch the data was written in.
     */
    @Override
    public ITimestamp append(Object data) throws OutOfSpaceException, IOException {
        return null;
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
        long sequence = instance.getSequencer().getNext(numTokens);
        ITimestamp[] s = new ITimestamp[numTokens];
        for (int i = 0; i < numTokens; i++)
        {
            s[i] = new SimpleTimestamp(sequence + i);
        }
        return s;
    }

    /**
     * Write to a specific, previously allocated log position.
     *
     * @param timestamp The timestamp to write to.
     * @param data      The data to write to that timestamp.
     * @throws OutOfSpaceException If there is no space left to write to that log position.
     * @throws OverwriteException  If something was written to that log position already.
     */
    public void write(ITimestamp timestamp, Serializable data) throws OutOfSpaceException, OverwriteException, IOException {
        instance.getAddressSpace().write(((SimpleTimestamp) timestamp).address, new SimpleStreamEntry(streamID, data, timestamp));
    }


    /**
     * Read the next entry in the stream as a CorfuDBStreamEntry. This function
     * retrieves the next entry in the stream, or returns null if there are no entries in the stream
     * to be read.
     *
     * @return A CorfuDBStreamEntry containing the payload of the next entry in the stream.
     */
    @Override
    public IStreamEntry readNextEntry() throws HoleEncounteredException, TrimmedException, IOException {
        long current = instance.getSequencer().getCurrent();
        long oldPointer = streamPointer.get()-1;
        // TODO: Need to make sure streamPointer is pointing to something in the stream?
        Hints hint = null;
        if (oldPointer != -1) {
            try {
                hint = instance.getAddressSpace().readHints(oldPointer);
            } catch (UnwrittenException ue) {
                //hole, should fill.
                throw new HoleEncounteredException(new SimpleTimestamp((ue.address)));
            }
        }
        if (hint == null || !hint.isSetNextMap() || (hint.getNextMap().get(streamID.toString()) == null)) {
            synchronized (this) {
                for (long i = streamPointer.get(); i < current; i++) {
                    try {
                        IStreamEntry sse = (IStreamEntry) instance.getAddressSpace().readObject(i);
                        sse.setTimestamp(new SimpleTimestamp(i));
                        streamPointer.set(i + 1);
                        if (sse.containsStream(streamID)) {
                            if (oldPointer != -1) {
                                //instance.getAddressSpace().setHintsNext(oldPointer, streamID, i);
                                //log.info("just set next of {} to {}, stream: {}", oldPointer, i, streamID);
                            }
                            return sse;
                        }
                    } catch (ClassNotFoundException | ClassCastException e) {
                        //ignore, not a entry we understand.
                    } catch (UnwrittenException ue) {
                        //hole, should fill.
                        throw new HoleEncounteredException(new SimpleTimestamp(ue.address));
                    }
                }
            }
        } else {
            try {
                IStreamEntry sse = (IStreamEntry) instance.getAddressSpace().readObject(hint.getNextMap().get(streamID.toString()));
                sse.setTimestamp(new SimpleTimestamp(hint.getNextMap().get(streamID.toString())));
                streamPointer.set(hint.getNextMap().get(streamID.toString())+1);
                if (!sse.containsStream(streamID)) {
                    throw new RuntimeException("Inconsistent metadata");
                }
                return sse;
            } catch (ClassNotFoundException | ClassCastException e) {
                //ignore, not a entry we understand.
            } catch (UnwrittenException ue) {
                //hole, should fill.
                throw new HoleEncounteredException(new SimpleTimestamp(ue.address));
            }
        }
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
        try {
            IStreamEntry sse = (IStreamEntry) instance.getAddressSpace().readObject(((SimpleTimestamp) timestamp).address);
            sse.setTimestamp(new SimpleTimestamp(((SimpleTimestamp)timestamp).address));
            if (sse.containsStream(streamID)) {
                return sse;
            }
        } catch (ClassNotFoundException | ClassCastException e) {
            throw new HoleEncounteredException(timestamp);
        }
        catch (UnwrittenException ue)
        {
            //hole, should fill.
            throw new HoleEncounteredException(timestamp);
        }
        throw new HoleEncounteredException(timestamp);
    }

    /**
     * Given a timestamp, get the timestamp in the stream
     *
     * @param ts The timestamp to increment.
     * @return The next timestamp in the stream, or null, if there are no next timestamps in the stream.
     */
    @Override
    public ITimestamp getNextTimestamp(ITimestamp ts) {
        if (ITimestamp.isMin(ts))
        {
            return new SimpleTimestamp(0);
        }
        return new SimpleTimestamp(((SimpleTimestamp)ts).address + 1);
    }

    /**
     * Given a timestamp, get a proceeding timestamp in the stream.
     *
     * @param ts The timestamp to decrement.
     * @return The previous timestamp in the stream, or null, if there are no previous timestamps in the stream.
     */
    @Override
    public ITimestamp getPreviousTimestamp(ITimestamp ts) {
        if (ITimestamp.isMin(ts))
        {
            return ITimestamp.getMinTimestamp();
        }
        return new SimpleTimestamp(((SimpleTimestamp)ts).address - 1);
    }

    /**
     * Attempts to fill a hole at the given timestamp.
     *
     * @param ts A timestamp to fill a hole at.
     * @return True, if the hole was successfully filled, false otherwise.
     */
    @Override
    public boolean fillHole(ITimestamp ts) {
        try {
            instance.getAddressSpace().write(((SimpleTimestamp) ts).address, new CorfuDBStreamHoleEntry(new HashMap()));
            return true;
        }
        catch (IOException ie)
        {
            return false;
        }
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
        return new SimpleTimestamp(instance.getSequencer().getCurrent()-1);
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

    /**
     * Get the instance that this stream belongs to.
     *
     * @return The instance the stream belongs to.
     */
    @Override
    public ICorfuDBInstance getInstance() {
        return instance;
    }

    /**
     * Move the stream pointer to the given position.
     *
     * @param pos The position to seek to. The next read will occur AFTER this position.
     */
    @Override
    public void seek(ITimestamp pos) {
        this.streamPointer.set(((SimpleTimestamp)pos).address);
    }

}
