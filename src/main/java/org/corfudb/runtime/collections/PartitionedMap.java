package org.corfudb.runtime.collections;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PartitionedMap<K,V> implements Map<K,V>
{
    Map<K,V> maps[];

    public PartitionedMap(Map<K,V> tmaps[])
    {
        maps = tmaps;
    }

    int keytomap(Object O)
    {
        return O.hashCode()%maps.length;
    }

    @Override
    public int size()
    {
        int ret = 0;
        for(int i=0;i<maps.length;i++)
            ret += maps[i].size();
        return ret;
    }

    @Override
    public boolean isEmpty()
    {
        for(int i=0;i<maps.length;i++)
            if(!maps[i].isEmpty())
                return false;
        return true;
    }

    @Override
    public boolean containsKey(Object o)
    {
        return maps[keytomap(o)].containsKey(o);
    }

    @Override
    public boolean containsValue(Object o)
    {
        for(int i=0;i<maps.length;i++)
        {
            if(maps[i].containsValue(o)) return true;
        }
        return false;
    }

    @Override
    public V get(Object o)
    {
//4        System.out.println("getting " + o + " in map " + keytomap(o));
        return maps[keytomap(o)].get(o);
    }

    @Override
    public V put(K k, V v)
    {
//        System.out.println("putting " + k + " in map " + keytomap(k));
        return maps[keytomap(k)].put(k, v);
    }

    @Override
    public V remove(Object o)
    {
//        System.out.println("removing " + o + " from map " + keytomap(o));
        return maps[keytomap(o)].remove(o);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map)
    {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public void clear()
    {
        for(int i=0;i<maps.length;i++)
            maps[i].clear();
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