package org.corfudb.runtime.smr;

import org.corfudb.runtime.log.ITimestamp;
import java.io.Serializable;

public class TxIntReadSetEntry implements Serializable
{
    public long objectid;
    public ITimestamp readtimestamp;
    public Serializable readsummary;
    public TxIntReadSetEntry(long tobjid, ITimestamp ttimestamp, Serializable treadsummary)
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

