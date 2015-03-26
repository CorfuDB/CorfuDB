package org.corfudb.client.gossip;

import java.util.UUID;
import java.util.Map;
import java.io.Serializable;

/** This gossip entry is used to tell the configuration master that a stream is being pulled.
 *
 * Note: This isn't really a gossip entry per se, but it uses the gossip server, so it
 * is implemented as a gossip entry.
 */
public class StreamBundleGossip implements IGossip {
    private static final long serialVersionUID = 0L;
    public UUID streamID;
    /** The stream that is being pulled */
    public Map<UUID,Long> epochMap;
    /** The destination log of the stream */
    public UUID destinationLog;
    /** The destination stream of the stream */
    public UUID destinationStream;
    /** The destination position of the stream */
    public long physicalPos;
    /** The destination epoch */
    public long destinationEpoch;
    /** The reservation size */
    public int reservation;
    /** The duration */
    public int duration;
    /** The payload, if it exists */
    public Serializable payload;
    public long offset;
    public StreamBundleGossip(UUID streamID, Map<UUID,Long> epochMap, UUID destinationLog, UUID destinationStream, long physicalPos, long destinationEpoch, int reservation, int duration, Serializable payload, long offset)
    {
        this.streamID = streamID;
        this.epochMap = epochMap;
        this.destinationLog = destinationLog;
        this.destinationStream = destinationStream;
        this.destinationEpoch = destinationEpoch;
        this.physicalPos = physicalPos;
        this.reservation = reservation;
        this.duration = duration;
        this.payload = payload;
        this.offset = offset;
    }
}
