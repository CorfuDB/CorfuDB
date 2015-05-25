package org.corfudb.runtime.smr.legacy;

import java.io.Serializable;
import java.util.UUID;
import org.corfudb.runtime.stream.ITimestamp;

public class TxDec implements Serializable
{
    public UUID stream;
    public boolean decision; //true means commit, false means abort
    public ITimestamp txint_timestamp;
    public UUID txid;
    public TxDec(ITimestamp t_txint_timestamp, UUID t_stream, boolean t_dec, UUID ttxid)
    {
        txint_timestamp = t_txint_timestamp;
        stream = t_stream;
        decision = t_dec;
        txid = ttxid;
    }

}

