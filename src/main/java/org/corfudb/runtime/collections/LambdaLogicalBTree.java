package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.*;
import org.corfudb.runtime.smr.legacy.AbstractRuntime;
import org.corfudb.runtime.smr.legacy.CorfuDBObjectCommand;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class LambdaLogicalBTree<K extends Comparable<K>, V>
        implements ICorfuDBObject<BTree<K,V>>, IBTree<K,V> {

    transient ISMREngine<BTree<K,V>> smr;
    UUID streamID;

    @SuppressWarnings("unchecked")
    public LambdaLogicalBTree(IStream stream, Class<? extends ISMREngine> smrClass)
    {
        try {
            streamID = stream.getStreamID();
            smr = instantiateSMREngine(stream, smrClass);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public LambdaLogicalBTree(IStream stream)
    {
        this(stream, SimpleSMREngine.class);
    }

    /**
     * Get the type of the underlying object
     */
    @Override
    public Class<?> getUnderlyingType() {
        return BTree.class;
    }

    /**
     * Get the UUID of the underlying stream
     */
    @Override
    public UUID getStreamID() {
        return streamID;
    }

    /**
     * Get underlying SMR engine
     *
     * @return The SMR engine this object was instantiated under.
     */
    @Override
    public ISMREngine<BTree<K,V>> getUnderlyingSMREngine() {
        return smr;
    }

    /**
     * Set underlying SMR engine
     *
     * @param engine
     */
    @Override
    @SuppressWarnings("unchecked")
    public void setUnderlyingSMREngine(ISMREngine engine) {
        this.smr = engine;
    }

    /**
     * Set the stream ID
     *
     * @param streamID The stream ID to set.
     */
    @Override
    public void setStreamID(UUID streamID) {
        this.streamID = streamID;
    }

    /**
     * return the size
     * @return
     */
    public int size() {
        return accessorHelper((map, opts) -> map.size());
    }

    /**
     * return the height
     * @return
     */
    public int height() {
        return accessorHelper((map, opts) -> map.height());
    }

    /**
     * get the value at the given key
     * @param key
     * @return
     */
    public V get(K key) {
        return accessorHelper((map, opts) -> map.get(key));
    }

    /**
     * put the value at the given key
     * @param key
     * @param value
     */
    public V put(K key, V value) {
        return mutatorAccessorHelper((map, opts) -> map.put(key, value));
    }

    /**
     * apply a remove command by marking the
     * entry deleted (if found)
     * @param key
     * @return
     */
    @Override
    public V remove(K key) {
        return mutatorAccessorHelper((map, opts) -> map.remove(key));
    }

    /**
     * update the value at the given key
     * @param key
     * @param value
     */
    public boolean update(K key, V value) {
        return mutatorAccessorHelper((map, opts) -> map.update(key, value));
    }

    /**
     * clear the tree
     */
    public void clear() {
        mutatorHelper((map, opts) -> {
            map.clear();
            return null;
        });
    }

    /**
     * print the current view (consistent or otherwise)
     * @return
     */
    public String printview() { return print(); }

    /**
     * print the b-tree
     * @return
     */
    public String print() {
        return accessorHelper((map, opts) -> map.print());
    }

}


