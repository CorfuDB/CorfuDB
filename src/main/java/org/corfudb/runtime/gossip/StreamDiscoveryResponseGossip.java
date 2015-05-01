package org.corfudb.runtime.gossip;

import java.util.UUID;

/** This gossip message is sent in response to a discovery request.
 */
public class StreamDiscoveryResponseGossip implements IGossip {
    private static final long serialVersionUID = 0L;
    /** The stream that we wanted to learn about */
    public UUID streamID;
    /** The stream the stream currently resides on */
    public UUID currentLog;
    /** The stream the stream starts on */
    public UUID startLog;
    /** The current start position of the stream*/
    public long startPos;
    /** The current epoch the stream is on*/
    public long epoch;
    /** The last logical stream position we got an epoch update*/
    public long logPos;

    public StreamDiscoveryResponseGossip(UUID uuid, UUID currentLog, UUID startLog, long startPos, long epoch, long logPos)
    {
        this.streamID = uuid;
        this.currentLog = currentLog;
        this.startLog = startLog;
        this.startPos = startPos;
        this.epoch = epoch;
        this.logPos = logPos;
    }
}
