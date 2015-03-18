package org.corfudb.client.gossip;

import java.util.UUID;
import java.io.Serializable;

/** This gossip message is sent whenever a new stream is created.
 */
public class StreamCreatedGossip implements IGossip {
    private static final long serialVersionUID = 0L;
    /** The stream that was created */
    public UUID streamID;
    /** The new log the stream lives in */
    public UUID logID;
    /** The position that the stream starts at */
    public long startPos;

    public StreamCreatedGossip(UUID uuid, UUID logID, long startPos)
    {
        this.streamID = uuid;
        this.startPos = startPos;
        this.logID = logID;
    }
}
