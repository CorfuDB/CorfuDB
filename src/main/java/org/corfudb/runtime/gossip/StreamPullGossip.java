package org.corfudb.runtime.gossip;

import java.util.UUID;

/** This gossip entry is used to tell the configuration master that a stream is being pulled.
 *
 * Note: This isn't really a gossip entry per se, but it uses the gossip server, so it
 * is implemented as a gossip entry.
 */
public class StreamPullGossip implements IGossip {
    private static final long serialVersionUID = 0L;
    /** The stream that is being pulled */
    public UUID streamID;
    /** The destination stream of the stream */
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
    public byte[] payload;

    public StreamPullGossip(UUID streamID, UUID destinationLog, UUID destinationStream, long physicalPos, long destinationEpoch, int reservation, int duration, byte[] payload)
    {
        this.streamID = streamID;
        this.destinationLog = destinationLog;
        this.destinationStream = destinationStream;
        this.destinationEpoch = destinationEpoch;
        this.physicalPos = physicalPos;
        this.reservation = reservation;
        this.duration = duration;
        this.payload = payload;
    }
}
