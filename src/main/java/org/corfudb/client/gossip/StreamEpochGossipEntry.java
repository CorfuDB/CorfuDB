package org.corfudb.client.gossip;

import java.util.UUID;
import java.io.Serializable;

public class StreamEpochGossipEntry implements IGossip {
    private static final long serialVersionUID = 0L;
    public UUID streamID;
    public long epoch;

    public StreamEpochGossipEntry(UUID uuid, long epoch)
    {
        this.streamID = uuid;
        this.epoch = epoch;
    }
}
