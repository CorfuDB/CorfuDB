package org.corfudb.runtime.smr.legacy;

import java.io.Serializable;
import java.util.UUID;

public class TxIntWriteSetEntry implements Serializable
{
    Serializable command;
    UUID objectid;
    Serializable key;
    public TxIntWriteSetEntry(Serializable tcommand, UUID tobjid, Serializable tkey)
    {
        command = tcommand;
        objectid = tobjid;
        key = tkey;
    }
    public String toString()
    {
        return "(W(" + objectid + "," + key + "))";
    }
}
