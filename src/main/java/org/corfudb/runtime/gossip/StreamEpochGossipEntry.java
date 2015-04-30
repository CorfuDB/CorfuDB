package org.corfudb.runtime.gossip;

import java.util.UUID;

/** This gossip message is sent whenever the epoch changes for a stream.
 */
public class StreamEpochGossipEntry implements IGossip {
    private static final long serialVersionUID = 0L;
    /** The stream that is changing epochs */
    public UUID streamID;
    /** The new log the stream lives in */
    public UUID logID;
    /** The new epoch that the stream is in */
    public long epoch;
    /** The logical position the epoch change occurs at */
    public long logPos;
    /** Whether or not this message was from a configuration master */
    public boolean fromMaster;

    public StreamEpochGossipEntry(UUID uuid, UUID logID, long epoch, long logPos)
    {
        this.streamID = uuid;
        this.epoch = epoch;
        this.logID = logID;
        this.logPos = logPos;
        this.fromMaster = false;
    }
}
