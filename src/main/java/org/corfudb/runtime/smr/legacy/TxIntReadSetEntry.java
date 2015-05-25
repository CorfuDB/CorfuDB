package org.corfudb.runtime.smr.legacy;

import org.corfudb.runtime.stream.ITimestamp;
import java.io.Serializable;
import java.util.UUID;

public class TxIntReadSetEntry implements Serializable
{
    public UUID objectid;
    public ITimestamp readtimestamp;
    public Serializable readsummary;
    public TxIntReadSetEntry(UUID tobjid, ITimestamp ttimestamp, Serializable treadsummary)
    {
        objectid = tobjid;
        readtimestamp = ttimestamp;
        readsummary = treadsummary;
    }
    public String toString()
    {
        return "(R(" + objectid + "," + readtimestamp + "," + readsummary + "))";
    }
}

