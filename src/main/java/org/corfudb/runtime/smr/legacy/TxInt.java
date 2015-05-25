package org.corfudb.runtime.smr.legacy;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.LinkedList;
import java.util.HashMap;
import org.corfudb.runtime.stream.ITimestamp;
import java.util.UUID;
import java.util.HashSet;
import java.util.Iterator;

public class TxInt implements Serializable //todo: custom serialization
{
    //command, object, key
    public final UUID txid;
    public List<TxIntWriteSetEntry> bufferedupdates;
    public Map<UUID, Integer> updatestreammap;

    //object, version, key
    public Set<TxIntReadSetEntry> readset;
    public Map<UUID, Integer> readstreammap; //maps from a stream id to a number denoting the insertion order of that id
    public Map<UUID, Integer> allstreammap; //todo: custom serialization so this doesnt get written out
    TxInt()
    {
        txid = UUID.randomUUID();
        bufferedupdates = new LinkedList();
        readset = new HashSet();
        updatestreammap = new HashMap();
        readstreammap = new HashMap();
        allstreammap = new HashMap();
    }
    void buffer_update(Serializable bs, UUID stream, Serializable key)
    {
        bufferedupdates.add(new TxIntWriteSetEntry(bs, stream, key));
        updatestreammap.put(stream, updatestreammap.size());
        if(!allstreammap.containsKey(stream))
            allstreammap.put(stream, allstreammap.size());
    }
    void mark_read(UUID object, ITimestamp timestamp, Serializable readsummary)
    {
        readset.add(new TxIntReadSetEntry(object, timestamp, readsummary));
        if(!readstreammap.containsKey(object))
            readstreammap.put(object, readstreammap.size());
        if(!allstreammap.containsKey(object))
        {
            allstreammap.put(object, allstreammap.size());
        }

    }
    Map<UUID, Integer> get_updatestreams()
    {
        return updatestreammap;
    }
    Map<UUID, Integer> get_readstreams()
    {
        return readstreammap;
    }
    Set<TxIntReadSetEntry> get_readset()
    {
        return readset;
    }
    List<TxIntWriteSetEntry> get_bufferedupdates()
    {
        return bufferedupdates;
    }
    Map<UUID, Integer> get_allstreams()
    {
        return allstreammap;
    }

    boolean has_read(UUID object, ITimestamp version, Serializable key)
    {
        Iterator<TxIntReadSetEntry> it = get_readset().iterator();
        while(it.hasNext()) {
            TxIntReadSetEntry trip = it.next();
            if(trip.objectid == object) {
//                if(key == null || trip.key == null || key.equals(trip.key))
                    return true;
            }
        }
        return false;
    }

    public boolean readsSomethingWrittenBy(TxInt T2)
    {
        Iterator<TxIntReadSetEntry> it = get_readset().iterator();
        while(it.hasNext())
        {
            TxIntReadSetEntry Trip1 = it.next();
            Iterator<TxIntWriteSetEntry> it2 = T2.get_bufferedupdates().iterator();
            while(it2.hasNext())
            {
                TxIntWriteSetEntry Trip2 = it2.next();
                //objects match?
                if(Trip1.objectid==Trip2.objectid)
                {
                    //keys match?
//                    if(Trip1.key==null || Trip2.key==null || Trip1.key.equals(Trip2.key))
                    {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public String toString()
    {
        return "TXINT: [[[WriteSet: " + bufferedupdates.toString() + "]]]\n"
                + "[[[ReadSet: " + readset.toString() + "]]]";
    }

    public UUID getTxid() {
        return txid;
    }

}
