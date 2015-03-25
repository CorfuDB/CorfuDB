package org.corfudb.runtime;

import java.io.Serializable;

public class TxIntWriteSetEntry implements Serializable
{
    Serializable command;
    long objectid;
    Serializable key;
    public TxIntWriteSetEntry(Serializable tcommand, long tobjid, Serializable tkey)
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
