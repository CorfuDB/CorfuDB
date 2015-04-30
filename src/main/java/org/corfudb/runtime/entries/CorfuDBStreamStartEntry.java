package org.corfudb.runtime.entries;

import java.util.Map;
import java.util.ArrayList;
import java.util.List;

import java.util.UUID;
import java.io.Serializable;

import java.io.IOException;

public class CorfuDBStreamStartEntry extends CorfuDBStreamEntry
{
    private static final long serialVersionUID = 0L;
    public List<UUID> streamID;

    public CorfuDBStreamStartEntry(UUID streamID, long epoch)
    {
        super(streamID, epoch);
        this.streamID = new ArrayList<UUID>();
        this.streamID.add(streamID);
    }

    public CorfuDBStreamStartEntry(Map<UUID, Long> streamMap, List<UUID> startStreams)
    {
        super(streamMap);
        streamID = startStreams;
    }

    public CorfuDBStreamStartEntry(Map<UUID, Long> streamMap, List<UUID> startStreams, byte[] payload)
    {
        super(streamMap);
        streamID = startStreams;
        this.payload = payload;
    }

    public CorfuDBStreamStartEntry(Map<UUID, Long> streamMap, List<UUID> startStreams, Serializable payload)
        throws IOException
    {
        super(streamMap, payload);
        streamID = startStreams;
    }


}
