package org.corfudb.runtime.collections;

import java.util.*;

import org.corfudb.runtime.smr.legacy.AbstractRuntime;

public class PartitionedMap<K,V> implements Map<K,V>
{
    List<UUID> partitionlist;
    AbstractRuntime TR;

    public PartitionedMap(List<UUID> tpartitionlist, AbstractRuntime tTR)
    {
        partitionlist = tpartitionlist;
        TR = tTR;
    }

    //objectid to object instance map
    Map<UUID, CorfuDBMap<K,V>> soft_mapobjs = new HashMap();

    CorfuDBMap<K,V> objidtomap(UUID objid)
    {
        CorfuDBMap<K,V> curmap = null;
        if(soft_mapobjs.containsKey(objid))
            curmap = soft_mapobjs.get(objid);
        else
        {
            curmap = new CorfuDBMap<K,V>(TR, objid);
            soft_mapobjs.put(objid, curmap);
        }
        return curmap;
    }

    CorfuDBMap<K,V> keytomap(Object O)
    {
        int index = 0;
        if(O!=null)
            index = O.hashCode()%partitionlist.size();
//        System.out.println("XYZ " + partitionlist.size() + ", " + index + ": " + partitionlist.get(index));
        UUID objid = partitionlist.get(index);
        return objidtomap(objid);
    }

    @Override
    public int size()
    {
        int ret;
        while(true)
        {
            TR.BeginTX();
            ret = 0;
            for (int i = 0; i < partitionlist.size(); i++)
                ret += objidtomap(partitionlist.get(i)).size();
            if(TR.EndTX()) break;
        }
        return ret;
    }

    @Override
    public boolean isEmpty()
    {
        boolean ret = true;
        while(true)
        {
            TR.BeginTX();
            for (int i = 0; i < partitionlist.size(); i++)
                if (!objidtomap(partitionlist.get(i)).isEmpty())
                {
                    ret = false;
                    break;
                }
            if(TR.EndTX()) break;

        }
        return ret;
    }

    @Override
    public boolean containsKey(Object o)
    {
        boolean ret = false;
        while(true)
        {
            TR.BeginTX();
            ret = keytomap(o).containsKey(o);
            if(TR.EndTX()) break;
        }
        return ret;
    }

    @Override
    public boolean containsValue(Object o)
    {
        boolean ret = false;
        while(true)
        {
            TR.BeginTX();
            for (int i = 0; i < partitionlist.size(); i++)
            {
                if (objidtomap(partitionlist.get(i)).containsValue(o)) ret = true;
            }
            if(TR.EndTX()) break;
        }
        return ret;
    }

    @Override
    public V get(Object o)
    {
//4        System.out.println("getting " + o + " in map " + keytomap(o));
        V ret = null;
        while(true)
        {
            TR.BeginTX();
            ret = keytomap(o).get(o);
            if(TR.EndTX()) break;
        }
        return ret;
    }

    @Override
    public V put(K k, V v)
    {
//        System.out.println("putting " + k + " in map " + keytomap(k));
        V ret = null;
        while(true)
        {
            TR.BeginTX();
            ret = keytomap(k).put(k,v);
            if(TR.EndTX()) break;
        }
        return ret;
    }

    @Override
    public V remove(Object o)
    {
//        System.out.println("removing " + o + " from map " + keytomap(o));
        V ret = null;
        while(true)
        {
            TR.BeginTX();
            ret = keytomap(o).remove(o);
            if(TR.EndTX()) break;
        }
        return ret;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map)
    {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public void clear()
    {
        while(true)
        {
            TR.BeginTX();
            for (int i = 0; i < partitionlist.size(); i++)
                objidtomap(partitionlist.get(i)).clear();
            if(TR.EndTX()) break;
        }
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