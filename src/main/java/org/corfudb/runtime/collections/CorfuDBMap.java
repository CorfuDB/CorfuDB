package org.corfudb.runtime.collections;

import org.corfudb.runtime.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 *
 */
public class CorfuDBMap<K,V> extends CorfuDBObject implements Map<K,V>
{
    static Logger dbglog = LoggerFactory.getLogger(CorfuDBMap.class);
    //backing state of the map
    Map<K, V> backingmap;

    boolean optimizereads = false;
    boolean finegrainconflictdetection = false;

    public CorfuDBMap(AbstractRuntime tTR, long toid)
    {
        this(tTR, toid, false);
    }

    public CorfuDBMap(AbstractRuntime tTR, long toid, boolean remote)
    {
        super(tTR, toid, remote);
        backingmap = new HashMap<K,V>();
    }

    public void applyToObject(Object bs, long timestamp)
    {
        MapCommand<K,V> cc = (MapCommand<K,V>)bs;
        dbglog.debug("CorfuDBMap received upcall: " + cc);
        if(optimizereads)
            lock(true);
        if(finegrainconflictdetection)
        {
            //custom conflict detection: accessors provide a read summary; mutators set the last update timestamp
            if (cc.getCmdType() == MapCommand.CMD_PREPUT || cc.getCmdType() == MapCommand.CMD_GET || cc.getCmdType() == MapCommand.CMD_SIZE
                    || cc.getCmdType() == MapCommand.CMD_CONTAINSKEY || cc.getCmdType() == MapCommand.CMD_CONTAINSVALUE || cc.getCmdType() == MapCommand.CMD_ISEMPTY)
                cc.summarizeRead(new Triple(cc.getCmdType(), cc.getKey(), getLastUpdateTS(cc.getCmdType(), cc.getKey())));
            else if (cc.getCmdType() == MapCommand.CMD_PUT || cc.getCmdType() == MapCommand.CMD_REMOVE || cc.getCmdType() == MapCommand.CMD_CLEAR)
                setLastUpdateTS(timestamp, cc.getCmdType(), cc.getKey());
            else
            {
                //need to unlock?
                throw new RuntimeException("Unrecognized command in stream!");
            }
        }
        if(cc.getCmdType()==MapCommand.CMD_PUT)
            backingmap.put(cc.getKey(), cc.getVal());
        else if(cc.getCmdType()==MapCommand.CMD_PREPUT)
            cc.setReturnValue(backingmap.get(cc.getKey()));
        else if(cc.getCmdType()==MapCommand.CMD_REMOVE)
            backingmap.remove(cc.getKey());
        else if(cc.getCmdType()==MapCommand.CMD_CLEAR)
            backingmap.clear();
        else if(cc.getCmdType()==MapCommand.CMD_GET)
            cc.setReturnValue(backingmap.get(cc.getKey()));
        else if(cc.getCmdType()==MapCommand.CMD_SIZE)
            cc.setReturnValue(backingmap.size());
        else if(cc.getCmdType()==MapCommand.CMD_CONTAINSKEY)
            cc.setReturnValue(backingmap.containsKey(cc.getKey()));
        else if(cc.getCmdType()==MapCommand.CMD_CONTAINSVALUE)
            cc.setReturnValue(backingmap.containsValue(cc.getVal()));
        else if(cc.getCmdType()==MapCommand.CMD_ISEMPTY)
            cc.setReturnValue(backingmap.isEmpty());
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
        TR.query_helper(this, new Pair(MapCommand.CMD_ISEMPTY, null), isemptycmd);
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
        TR.query_helper(this, new Pair(MapCommand.CMD_CONTAINSKEY, o.hashCode()), containskeycmd);
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
        TR.query_helper(this, new Pair(MapCommand.CMD_CONTAINSVALUE, o.hashCode()), containsvalmd);
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
        TR.query_helper(this, new Pair(MapCommand.CMD_GET, o.hashCode()), getcmd);
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
        MapCommand<K,V> precmd = new MapCommand<K,V>(MapCommand.CMD_PREPUT, key);
        TR.query_then_update_helper(this, precmd, new MapCommand<K, V>(MapCommand.CMD_PUT, key, val), new Pair(MapCommand.CMD_PUT, key.hashCode()));
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
        TR.query_then_update_helper(this, precmd, new MapCommand<K, V>(MapCommand.CMD_REMOVE, (K) o), new Pair(MapCommand.CMD_REMOVE, o.hashCode()));
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
        TR.update_helper(this, new MapCommand<K, V>(MapCommand.CMD_CLEAR), new Pair(MapCommand.CMD_CLEAR, null));
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

    //custom conflict detection upcalls
    //'X' implies that the mutator (column) always impacts the accessor (row)
    //'/' implies that the mutator (column) only impacts the accessor (row) for the same map key
    /*              put     remove     clear
    get             /       /           X
    size            X       X           X
    containsKey     /       /           X
    containsValue   X       X           X
    isEmpty         X       X           X
    put(accessor)   /       /           X
    remove(acc.)    /       /           X
     */

    //Theoretically, we need a single timestamp for each accessor type, tracking the last command
    //in the stream that impacted the return value of that accessor.
    //When a mutator is passed into setTimestamp, we then update the timestamps for accessors
    //whose return value is impacted by the mutator.
    //In practice, for the map example, all mutators affect all accessors, so a single timestamp can suffice.
    //However, some mutator/accessor pairs only conflict if the map key being modified is the same
    //Accordingly, we have two timestamps: a non-keyed one and a keyed one.
    AtomicLong globaltimestamp = new AtomicLong(); //used by size, isEmpty, containsValue
    Map<K, AtomicLong> localtimestamps = new HashMap(); //used by get, containsKey, put(accessor), remove(accessor)


    //returns the timestamp of the last mutator command in the stream that affected the outcome
    //of the passed in accessor
    public long getLastUpdateTS(int cmdtype, K key)
    {
        if(!finegrainconflictdetection) throw new RuntimeException("this should never get called if fine-grained conflict detection is disabled");
        if(cmdtype==MapCommand.CMD_SIZE || cmdtype==MapCommand.CMD_ISEMPTY || cmdtype==MapCommand.CMD_CONTAINSVALUE)
        {
//            System.out.println("returns global " + globaltimestamp.get());
            return globaltimestamp.get();
        }
        else if(cmdtype==MapCommand.CMD_GET || cmdtype==MapCommand.CMD_CONTAINSKEY || cmdtype==MapCommand.CMD_PUT || cmdtype==MapCommand.CMD_REMOVE || cmdtype==MapCommand.CMD_PREPUT)
        {
            if(!(localtimestamps.containsKey(key)))
                localtimestamps.put(key, new AtomicLong());
//            System.out.println("returns local " + localtimestamps.get(P.second).get());
            return localtimestamps.get(key).get(); //todo: what if it's a read on a map with no updates?
        }
        else
        {
            throw new RuntimeException("unknown cmd type: " + cmdtype);
        }
    }

    public boolean isStillValid(Serializable readsummary)
    {
        if(!finegrainconflictdetection) throw new RuntimeException("this should never get called if fine-grained conflict detection is disabled");
        Triple<Integer, K, Long> T = (Triple<Integer, K, Long>)readsummary;
        if(getLastUpdateTS(T.first, T.second)>T.third)
            return false;
        return true;
    }

    //for every accessor operation, we have to update its timestamp if its return value is impacted
    //e.g. if op is put, we need to update the get key-specific timestamp; the size timestamp; ...
    public void setLastUpdateTS(long newts, int cmdtype, K key)
    {
        if(!finegrainconflictdetection) throw new RuntimeException("this should never get called if fine-grained conflict detection is disabled");
//        System.out.println("ST: " + key + " " + newts);
        if(cmdtype==MapCommand.CMD_PUT || cmdtype==MapCommand.CMD_REMOVE)
        {
//            System.out.println("setting local");
            if(!(localtimestamps.containsKey(key)))
                localtimestamps.put(key, new AtomicLong());
            localtimestamps.get(key).set(newts);
        }
        else if(cmdtype==MapCommand.CMD_CLEAR)
        {
//            System.out.println("setting global");
            globaltimestamp.set(newts);
        }
        else
            throw new RuntimeException("unknown cmd type: " + cmdtype);
    }



}
