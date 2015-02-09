package org.corfudb.runtime.collections;

import org.corfudb.runtime.AbstractRuntime;
import org.corfudb.runtime.CorfuDBObject;
import org.corfudb.runtime.CorfuDBObjectCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 *
 */
public class CorfuDBMap<K,V> extends CorfuDBObject implements Map<K,V>
{
    static Logger dbglog = LoggerFactory.getLogger(CorfuDBMap.class);
    //backing state of the map
    Map<K, V> backingmap;

    boolean optimizereads = false;

    public CorfuDBMap(AbstractRuntime tTR, long toid)
    {
        this(tTR, toid, false);
    }

    public CorfuDBMap(AbstractRuntime tTR, long toid, boolean remote)
    {
        super(tTR, toid, remote);
        backingmap = new HashMap<K,V>();
    }

    public void applyToObject(Object bs)
    {
        dbglog.debug("CorfuDBMap received upcall");
        MapCommand<K,V> cc = (MapCommand<K,V>)bs;
        if(optimizereads)
            lock(true);
        if(cc.getCmdType()==MapCommand.CMD_PUT)
        {
            backingmap.put(cc.getKey(), cc.getVal());
        }
        else if(cc.getCmdType()==MapCommand.CMD_PREPUT)
        {
            cc.setReturnValue(backingmap.get(cc.getKey()));
        }
        else if(cc.getCmdType()==MapCommand.CMD_REMOVE)
        {
            backingmap.remove(cc.getKey());
        }
        else if(cc.getCmdType()==MapCommand.CMD_CLEAR)
        {
            backingmap.clear();
        }
        else if(cc.getCmdType()==MapCommand.CMD_GET)
        {
            cc.setReturnValue(backingmap.get(cc.getKey()));
        }
        else if(cc.getCmdType()==MapCommand.CMD_SIZE)
        {
            cc.setReturnValue(backingmap.size());
        }
        else if(cc.getCmdType()==MapCommand.CMD_CONTAINSKEY)
        {
            cc.setReturnValue(backingmap.containsKey(cc.getKey()));
        }
        else if(cc.getCmdType()==MapCommand.CMD_CONTAINSVALUE)
        {
            cc.setReturnValue(backingmap.containsValue(cc.getVal()));
        }
        else if(cc.getCmdType()==MapCommand.CMD_ISEMPTY)
        {
            cc.setReturnValue(backingmap.isEmpty());
        }
        else
        {
            //need to unlock?
            throw new RuntimeException("Unrecognized command in stream!");
        }
        dbglog.debug("Map size is {}", backingmap.size());
        if(optimizereads)
            unlock(true);
    }

    //accessor
    @Override
    public int size()
    {
        if(optimizereads)
            return size_optimized();
        MapCommand sizecmd = new MapCommand(MapCommand.CMD_SIZE);
        TR.query_helper(this, null, sizecmd);
        return (Integer)sizecmd.getReturnValue();
    }

    public int size_optimized()
    {
        TR.query_helper(this);
        //what if the value changes between query_helper and the actual read?
        //in the linearizable case, we are safe because we see a later version that strictly required
        //in the transactional case, the tx will spuriously abort, but safety will not be violated...
        lock();
        int x = backingmap.size();
        unlock();
        return x;
    }

    //accessor
    @Override
    public boolean isEmpty()
    {
        if(optimizereads)
            return isEmpty_optimized();
        MapCommand isemptycmd = new MapCommand(MapCommand.CMD_ISEMPTY);
        TR.query_helper(this, null, isemptycmd);
        return (Boolean)isemptycmd.getReturnValue();
    }

    public boolean isEmpty_optimized()
    {
        TR.query_helper(this);
        lock();
        boolean x = backingmap.isEmpty();
        unlock();
        return x;
    }

    //accessor
    @Override
    public boolean containsKey(Object o)
    {
        if (optimizereads)
            return containsKey_optimized(o);
        MapCommand containskeycmd = new MapCommand(MapCommand.CMD_CONTAINSKEY, o);
        TR.query_helper(this, o.hashCode(), containskeycmd);
        return (Boolean)containskeycmd.getReturnValue();
    }

    public boolean containsKey_optimized(Object o)
    {
        TR.query_helper(this, o.hashCode());
        lock();
        boolean x = backingmap.containsKey(o);
        unlock();
        return x;
    }

    //accessor
    @Override
    public boolean containsValue(Object o)
    {
        if (optimizereads)
            return containsValue_optimized(o);
        MapCommand containsvalmd = new MapCommand(MapCommand.CMD_CONTAINSVALUE, null, o);
        TR.query_helper(this, null, containsvalmd);
        return (Boolean)containsvalmd.getReturnValue();
    }
    public boolean containsValue_optimized(Object o)
    {
        TR.query_helper(this);
        lock();
        boolean x = backingmap.containsValue(o);
        unlock();
        return x;
    }

    //accessor
    @Override
    public V get(Object o)
    {
        if (optimizereads)
            return get_optimized(o);
        MapCommand getcmd = new MapCommand(MapCommand.CMD_GET, o);
        TR.query_helper(this, o.hashCode(), getcmd);
        return (V)getcmd.getReturnValue();
    }
    public V get_optimized(Object o)
    {
        TR.query_helper(this, o.hashCode());
        lock();
        V x = backingmap.get(o);
        unlock();
        return x;
    }

    //accessor+mutator
    public V put(K key, V val)
    {
        HashSet<Long> H = new HashSet<Long>();
        H.add(this.getID());
        MapCommand<K,V> precmd = new MapCommand<K,V>(MapCommand.CMD_PREPUT, key);
        TR.query_then_update_helper(this, precmd, new MapCommand<K, V>(MapCommand.CMD_PUT, key, val), key.hashCode());
        return (V)precmd.getReturnValue();
    }

    //accessor+mutator
    @Override
    public V remove(Object o)
    {
        //will throw a classcast exception if o is not of type K, which seems to expected behavior for the Map interface
        HashSet<Long> H = new HashSet<Long>();
        H.add(this.getID());
        MapCommand<K,V> precmd = new MapCommand<K,V>(MapCommand.CMD_PREPUT, (K)o);
        TR.query_then_update_helper(this, precmd, new MapCommand<K, V>(MapCommand.CMD_REMOVE, (K) o), o.hashCode());
        return (V)precmd.getReturnValue();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map)
    {
        throw new RuntimeException("unimplemented");
    }

    //mutator
    @Override
    public void clear()
    {
        //will throw a classcast exception if o is not of type K, which seems to expected behavior for the Map interface
        HashSet<Long> H = new HashSet<Long>();
        H.add(this.getID());
        TR.update_helper(this, new MapCommand<K, V>(MapCommand.CMD_CLEAR));
    }

    @Override
    public Set<K> keySet()
    {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public Collection<V> values()
    {
        throw new RuntimeException("unimplemented");

    }

    @Override
    public Set<Entry<K, V>> entrySet()
    {
        throw new RuntimeException("unimplemented");
    }

}

class MapCommand<K,V> extends CorfuDBObjectCommand
{
    int cmdtype;
    static final int CMD_PUT = 0;
    static final int CMD_PREPUT = 1;
    static final int CMD_REMOVE = 2;
    static final int CMD_CLEAR = 3;
    //accessors
    static final int CMD_GET = 4;
    static final int CMD_ISEMPTY = 5;
    static final int CMD_CONTAINSKEY = 6;
    static final int CMD_CONTAINSVALUE = 7;
    static final int CMD_SIZE = 8;
    K key;
    V val;
    public K getKey()
    {
        return key;
    }
    public V getVal()
    {
        return val;
    }
    public MapCommand(int tcmdtype)
    {
        this(tcmdtype, null, null);
    }
    public MapCommand(int tcmdtype, K tkey)
    {
        this(tcmdtype, tkey, null);
    }

    public MapCommand(int tcmdtype, K tkey, V tval)
    {
        cmdtype = tcmdtype;
        key = tkey;
        val = tval;
    }
    public int getCmdType()
    {
        return cmdtype;
    }
};
