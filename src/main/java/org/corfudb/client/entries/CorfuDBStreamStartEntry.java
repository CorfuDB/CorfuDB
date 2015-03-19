package org.corfudb.client.entries;

import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.Timestamp;

import java.util.Map;
import java.util.ArrayList;
import java.util.Queue;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.io.Serializable;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectInput;
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


}
