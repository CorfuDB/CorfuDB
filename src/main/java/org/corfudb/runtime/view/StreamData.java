package org.corfudb.runtime.view;

import java.util.UUID;

/**
 * This class contains information about streams in the system.
 */
public class StreamData {
    /** The ID of the stream */
    public UUID streamID;
    /** The log the stream is currently on (at lastupdate) */
    public UUID currentLog;
    /** The log the stream starts on. */
    public UUID startLog;
    /** The epoch the stream is in (at lastupdate) */
    public long epoch;
    /** The start position of the stream */
    public long startPos;
    /** The last update we saw on this stream */
    public long lastUpdate;

    /** Creates a new StreamData instance */
    public StreamData(UUID streamID, UUID currentLog, UUID startLog, long epoch, long startPos)
    {
        this.streamID = streamID;
        this.currentLog = currentLog;
        this.startLog = startLog;
        this.epoch = epoch;
        this.startPos = startPos;
        this.lastUpdate = 0;
    }

    /** Creates a new StreamData instance */
    public StreamData(UUID streamID, UUID currentLog, UUID startLog, long epoch, long startPos, long lastupdate)
    {
        this.streamID = streamID;
        this.currentLog = currentLog;
        this.startLog = startLog;
        this.epoch = epoch;
        this.startPos = startPos;
        this.lastUpdate = lastupdate;
    }

    public StreamData()
    {

    }
}


