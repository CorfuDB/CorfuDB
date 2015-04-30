package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.AbstractRuntime;
import org.corfudb.runtime.smr.CorfuDBObject;
import org.corfudb.runtime.smr.CorfuDBObjectCommand;
import org.corfudb.runtime.smr.IStreamFactory;

import java.util.ArrayList;
import java.util.Collection;

public abstract class CDBAbstractBTree<K extends Comparable<K>, V> extends CorfuDBObject {

    // true if we want to collect per request latency
    // statistics. Best left out for end-to-end performance
    // measurements, since the volume of data is non-trivial
    // s_completed is a list of all commands that actually complete
    // through the applyToObject upcall, which means they are the subset
    // for which we have valid latency from helper function to apply.
    public static boolean s_collectLatencyStats = false;
    public static ArrayList<CorfuDBObjectCommand> s_completed = new ArrayList();
    public static int M = 4;

    // set to true from the client *iff* the client process knows it's
    // single-threaded. When this can be assumed, we know that exceptions taken
    // mid-transaction cannot be the result of opacity violations and are therefore
    // not worth retrying. Apparently Infinite loops of try/catch/retry are a very common
    // scenario when executing under transactions, and distinguishing when they result
    // from user code vs. runtime behavior is a challenge.
    public static boolean s_singleThreadOptimization = false;

    IStreamFactory sf;

    /**
     * ctor
     * @param tTR
     * @param tsf
     * @param toid
     */
    public CDBAbstractBTree(
            AbstractRuntime tTR,
            IStreamFactory tsf,
            long toid
        )
    {
        super(tTR, toid);
        sf = tsf;
    }

    public abstract String print();
    public abstract String printview();
    public abstract int size();
    public abstract int height();
    public abstract V get(K key);
    public abstract V remove(K key);
    public abstract void put(K key, V value);
    public abstract boolean update(K key, V value);
    public abstract void clear();
    protected abstract void startRequestImpl(CorfuDBObjectCommand op);
    protected abstract boolean completeRequestImpl(CorfuDBObjectCommand op);
    protected abstract String getLatencyStatsImpl(Collection<CorfuDBObjectCommand> ops);

    protected boolean eq(Comparable a, Comparable b) { return a.compareTo(b) == 0; }
    protected boolean lt(Comparable a, Comparable b) { return a.compareTo(b) < 0; }
    protected void startRequest(CorfuDBObjectCommand op) { if(!s_collectLatencyStats) return; startRequestImpl(op); }
    protected void completeRequest(CorfuDBObjectCommand op) { if(!s_collectLatencyStats) return; if(completeRequestImpl(op)) s_completed.add(op); }
    public String getLatencyStats() { if(!s_collectLatencyStats) return null; return getLatencyStatsImpl(s_completed); }

}


