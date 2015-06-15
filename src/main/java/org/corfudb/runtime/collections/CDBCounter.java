package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.ICorfuDBObject;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by mwei on 6/12/15.
 */
public class CDBCounter implements ICorfuDBObject<AtomicInteger> {

    public Integer get()
    {
        return accessorHelper((i,o)-> i.get());
    }

    public void reset() {
        mutatorHelper((i,o)-> i.set(0));
    }

    public Integer increment()
    {
        return mutatorAccessorHelper((i,o)-> i.getAndIncrement());
    }
}
