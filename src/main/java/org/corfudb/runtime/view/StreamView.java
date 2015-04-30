package org.corfudb.runtime.view;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

/**
 * This class represents a view of the streams in the system.
 */
public class StreamView {

    private static final Logger log = LoggerFactory.getLogger(StreamView.class);
    private Map<UUID, StreamData> streamMap;

    /**
     * Default constructor
     */
    public StreamView() {
        this.streamMap = new ConcurrentHashMap<UUID, StreamData>();
    }

    /**
     * Returns the data in view for a given stream.
     *
     * @param stream    The UUID of the stream to query.
     *
     * @return          The data for the stream in the view.
     */
    public StreamData getStream(UUID stream)
    {
        return streamMap.get(stream);
    }

    /**
     * Get all streams that we know about.
     *
     * @return          A list of streams in the view.
     */
    public Set<UUID> getAllStreams()
    {
        return streamMap.keySet();
    }

    /**
     * Adds a stream to the view, if it does not exist, and returns whether or not
     * a new stream was added.
     *
     * @param stream        The UUID of the stream to add.
     * @param startLog      The UUID of the log the stream starts on.
     * @param startPos      The position that the stream starts at.
     *
     * @return              True, if the stream was added to the view, or false, if
     *                      the stream already existed in the view.
     */
    public boolean addStream(UUID stream, UUID startLog, long startPos)
    {
        return streamMap.putIfAbsent(stream, new StreamData(stream, startLog, startLog, 0, startPos)) == null;
    }

    /**
     * Returns whether the stream has been updated to the given logical position.
     *
     * @param stream        The UUID of the stream.
     * @param logPos        The logical position to check.
     *
     * @return              True, if the stream has been updated to that position.
     *                      False otherwise.
     */
    public boolean checkStream(UUID stream, long logPos)
    {
        StreamData sd = streamMap.get(stream);
        if (sd == null) { return false; }
        if (logPos > sd.lastUpdate) { return false; }
        return true;
    }

    /**
     * Adds or updates a learned stream to the view, and returns whether or not
     * a stream was added or updated.
     *
     * @param stream        The UUID of the stream.
     * @param currentLog    The UUID of the log the stream is currently in.
     * @param startLog      The UUID of the log the stream resides on.
     * @param epoch         The epoch the stream is currently in.
     * @param startPos      The position the log currently starts on.
     * @param lastUpdate    The logical position this log was last updated at
     */
    public boolean learnStream(UUID stream, UUID currentLog, UUID startLog, long startPos, long epoch, long lastUpdate)
    {
        StreamData old = streamMap.get(stream);
        if (old == null) {
            if (currentLog == null || startLog == null || epoch == -1 || startPos == -1)
            {
                return false;
            }
            else{
                return streamMap.putIfAbsent(stream, new StreamData(stream, currentLog, startLog, epoch, startPos, lastUpdate)) == null;
            }
        }

        synchronized(old)
        {
            if (currentLog != null) { old.currentLog = currentLog; }
            if (startLog != null) { old.startLog = startLog; }
            if (epoch != -1) {old.epoch = epoch; }
            if (startPos != -1) {old.startPos = startPos; }
            old.lastUpdate = lastUpdate;
            return true;
        }
    }
}
