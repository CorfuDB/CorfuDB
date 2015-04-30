package org.corfudb.runtime.entries;

import java.util.Map;

import java.util.UUID;

public class CorfuDBStreamHoleEntry extends CorfuDBStreamEntry
{
    private static final long serialVersionUID = 0L;

    public CorfuDBStreamHoleEntry(Map<UUID, Long> streamMap)
    {
        super(streamMap);
        ts.setHole(true);
    }
}
