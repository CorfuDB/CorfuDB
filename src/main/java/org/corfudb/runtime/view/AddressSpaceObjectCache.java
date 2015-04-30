package org.corfudb.runtime.view;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.UUID;

public class AddressSpaceObjectCache
{
    public static class CacheAddress {
        public UUID log;
        public Long physicalPos;
        public CacheAddress(UUID log, Long physicalPos)
        {
            this.log = log;
            this.physicalPos = physicalPos;
        }
        @Override
        public int hashCode()
        {
            return this.physicalPos.intValue();
        }
        @Override
        public boolean equals(Object o)
        {
            if(o instanceof CacheAddress)
            {
                CacheAddress c = (CacheAddress) o;
                if (c.log.equals(log) && c.physicalPos.equals(physicalPos))
                {
                    return true;
                }
                return false;
            }
            return false;
        }
    }

    public static ConcurrentMap<CacheAddress, Object> Cache = new ConcurrentLinkedHashMap.Builder<CacheAddress, Object>()
                                                                                        .maximumWeightedCapacity(4000)
                                                                                        .build();

    public static void put(UUID log, Long physicalPos, Object payload)
    {
        CacheAddress c = new CacheAddress(log, physicalPos);
        Cache.putIfAbsent(c, payload);
    }

    public static Object get(UUID log, Long physicalPos)
    {
        CacheAddress c = new CacheAddress(log, physicalPos);
        return Cache.get(c);
    }
}
