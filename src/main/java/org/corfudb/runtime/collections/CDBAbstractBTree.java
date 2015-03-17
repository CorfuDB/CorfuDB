package org.corfudb.runtime.collections;

import org.corfudb.runtime.AbstractRuntime;
import org.corfudb.runtime.CorfuDBObject;
import org.corfudb.runtime.StreamFactory;

public abstract class CDBAbstractBTree<K extends Comparable<K>, V> extends CorfuDBObject {

    StreamFactory sf;

    /**
     * ctor
     * @param tTR
     * @param tsf
     * @param toid
     */
    public CDBAbstractBTree(
            AbstractRuntime tTR,
            StreamFactory tsf,
            long toid
        )
    {
        super(tTR, toid);
        sf = tsf;
    }

    public abstract String print();
    public abstract int size();
    public abstract int height();
    public abstract V get(K key);
    public abstract void put(K key, V value);
}


