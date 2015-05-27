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
        implements ICorfuDBObject<LambdaLogicalBTree<K,V>>,
        IBTree<K,V> {

    transient ISMREngine<BTree> smr;
    ITransaction tx;
    UUID streamID;

    public LambdaLogicalBTree(LambdaLogicalBTree<K,V> map, ITransaction tx)
    {
        this.streamID = map.streamID;
        this.tx = tx;
    }

    @SuppressWarnings("unchecked")
    public LambdaLogicalBTree(IStream stream, Class<? extends ISMREngine> smrClass)
    {
        try {
            streamID = stream.getStreamID();
            smr = smrClass.getConstructor(IStream.class, Class.class).newInstance(stream, BTree.class);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public LambdaLogicalBTree(IStream stream)
    {
        streamID = stream.getStreamID();
        smr = new SimpleSMREngine<BTree>(stream, BTree.class);
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
    public ISMREngine getUnderlyingSMREngine() {
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
     * Get the underlying transaction
     *
     * @return The transaction this object is currently participating in.
     */
    @Override
    public ITransaction getUnderlyingTransaction() {
        return tx;
    }

    /**
     * return the size
     * @return
     */
    public int size() {
        return (int) accessorHelper((ISMREngineCommand<BTree>) (map, opts) -> {
            opts.getReturnResult().complete(map.size());
        });
    }

    /**
     * return the height
     * @return
     */
    public int height() {
        return (int) accessorHelper((ISMREngineCommand<BTree>) (map, opts) -> {
            opts.getReturnResult().complete(map.height());
        });
    }

    /**
     * get the value at the given key
     * @param key
     * @return
     */
    public V get(K key) {
        return (V) accessorHelper((ISMREngineCommand<BTree>) (map, opts) -> {
            opts.getReturnResult().complete(map.get(key));
        });
    }

    /**
     * put the value at the given key
     * @param key
     * @param value
     */
    public void put(K key, V value) {
        mutatorHelper((ISMREngineCommand<BTree>) (map, opts) -> {
            map.put(key, value);
        });
    }

    /**
     * apply a remove command by marking the
     * entry deleted (if found)
     * @param key
     * @return
     */
    @Override
    public V remove(K key) {
        return (V) mutatorAccessorHelper(
                (ISMREngineCommand<BTree>) (map, opts) -> {
                    opts.getReturnResult().complete(map.remove(key));
                }
        );
    }

    /**
     * update the value at the given key
     * @param key
     * @param value
     */
    public boolean update(K key, V value) {
        return (boolean) mutatorAccessorHelper(
                (ISMREngineCommand<BTree>) (map, opts) -> {
                    opts.getReturnResult().complete(map.update(key, value));
                }
        );
    }

    /**
     * clear the tree
     */
    public void clear() {
        mutatorHelper((ISMREngineCommand<BTree>) (map, opts) -> {
            map.clear();
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
        return (String) accessorHelper((ISMREngineCommand<BTree>) (map, opts) -> {
            opts.getReturnResult().complete(map.print());
        });
    }

}


