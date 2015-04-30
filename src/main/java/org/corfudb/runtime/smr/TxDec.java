package org.corfudb.runtime.smr;

import java.io.Serializable;
import java.util.UUID;
import org.corfudb.runtime.log.ITimestamp;

public class TxDec implements Serializable
{
    public long stream;
    public boolean decision; //true means commit, false means abort
    public ITimestamp txint_timestamp;
    public UUID txid;
    public TxDec(ITimestamp t_txint_timestamp, long t_stream, boolean t_dec, UUID ttxid)
    {
        txint_timestamp = t_txint_timestamp;
        stream = t_stream;
        decision = t_dec;
        txid = ttxid;
    }

}

