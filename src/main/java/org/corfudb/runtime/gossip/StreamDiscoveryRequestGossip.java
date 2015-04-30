package org.corfudb.runtime.gossip;

import java.util.UUID;

/** This gossip message is sent whenever a stream needs to be discovered.
 */
public class StreamDiscoveryRequestGossip implements IGossip {
    private static final long serialVersionUID = 0L;
    /** The stream that we want to learn about */
    public UUID streamID;

    public StreamDiscoveryRequestGossip(UUID uuid)
    {
        this.streamID = uuid;
    }
}
