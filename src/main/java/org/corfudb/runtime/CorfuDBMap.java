/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.corfudb.runtime;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CorfuDBMap<K,V> extends CorfuDBObject implements Map<K,V>
{
    //backing state of the map
    Map<K, V> backingmap;

    public CorfuDBMap(AbstractRuntime tTR, long toid)
    {
        super(tTR, toid);
        maplock = new ReentrantReadWriteLock();
        backingmap = new HashMap<K,V>();
        TR = tTR;
        oid = toid;
        TR.registerObject(this);
    }

    public void apply(Object bs)
    {
        //System.out.println("dummyupcall");
        System.out.println("CorfuDBMap received upcall");
        MapCommand<K,V> cc = (MapCommand<K,V>)bs;
        maplock.writeLock().lock();
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
        else
        {
            maplock.writeLock().unlock();
            throw new RuntimeException("Unrecognized command in stream!");
        }
        System.out.println("Map size is " + backingmap.size());
        maplock.writeLock().unlock();
    }

    @Override
    public int size()
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

    @Override
    public boolean isEmpty()
    {
        TR.query_helper(this);
        lock();
        boolean x = backingmap.isEmpty();
        unlock();
        return x;
    }

    @Override
    public boolean containsKey(Object o)
    {
        TR.query_helper(this);
        lock();
        boolean x = backingmap.containsKey(o);
        unlock();
        return x;
    }

    @Override
    public boolean containsValue(Object o)
    {
        TR.query_helper(this);
        lock();
        boolean x = backingmap.containsValue(o);
        unlock();
        return x;
    }

    @Override
    public V get(Object o)
    {
        TR.query_helper(this);
        lock();
        V x = backingmap.get(o);
        unlock();
        return x;
    }

    public V put(K key, V val)
    {
        HashSet<Long> H = new HashSet<Long>();
        H.add(this.getID());
        MapCommand<K,V> precmd = new MapCommand<K,V>(MapCommand.CMD_PREPUT, key);
        TR.query_then_update_helper(this, precmd, new MapCommand<K, V>(MapCommand.CMD_PUT, key, val));
        return (V)precmd.getReturnValue();
    }

    @Override
    public V remove(Object o)
    {
        //will throw a classcast exception if o is not of type K, which seems to expected behavior for the Map interface
        HashSet<Long> H = new HashSet<Long>();
        H.add(this.getID());
        MapCommand<K,V> precmd = new MapCommand<K,V>(MapCommand.CMD_PREPUT, (K)o);
        TR.query_then_update_helper(this, precmd, new MapCommand<K, V>(MapCommand.CMD_REMOVE, (K) o));
        return (V)precmd.getReturnValue();
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map)
    {
        throw new RuntimeException("unimplemented");
    }

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


class MapCommand<K,V> implements Serializable
{
    int cmdtype;
    static final int CMD_PUT = 0;
    static final int CMD_PREPUT = 1;
    static final int CMD_REMOVE = 2;
    static final int CMD_CLEAR = 3;
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
    Object retval;
    public Object getReturnValue()
    {
        return retval;
    }
    public void setReturnValue(Object obj)
    {
        retval = obj;
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
