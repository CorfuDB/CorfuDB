package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.CorfuDBObjectCommand;

class MapCommand<K,V> extends CorfuDBObjectCommand
{
    int cmdtype;
    static final int CMD_PUT = 0;
    static final int CMD_PREPUT = 1;
    static final int CMD_REMOVE = 2;
    static final int CMD_CLEAR = 3;
    //accessors
    static final int CMD_GET = 4;
    static final int CMD_ISEMPTY = 5;
    static final int CMD_CONTAINSKEY = 6;
    static final int CMD_CONTAINSVALUE = 7;
    static final int CMD_SIZE = 8;
    static final int CMD_GET_KEY_RANGE = 9;
    static final int CMD_GET_RANGE = 10;
    String desc[] = {"CMD_PUT", "CMD_PREPUT", "CMD_REMOVE", "CMD_CLEAR", "CMD_GET", "CMD_ISEMPTY",
            "CMD_CONTAINSKEY", "CMD_CONTAINSVALUE", "CMD_SIZE", "CMD_GET_KEY_RANGE", "CMD_GET_RANGE"};

    K key;
    V val;
    int count;
    public K getKey()
    {
        return key;
    }
    public V getVal()
    {
        return val;
    }
    public int getCount() { return count; }
    public int getCmdType()
    {
        return cmdtype;
    }
    public MapCommand(int tcmdtype)
    {
        this(tcmdtype, null, null);
    }
    public MapCommand(int tcmdtype, K tkey) { this(tcmdtype, tkey, null); }
    public MapCommand(int tcmdtype, K tkey, V tval) { this(tcmdtype, tkey, tval, 0); }

    public MapCommand(int tcmdtype, K tkey, V tval, int _count)
    {
        cmdtype = tcmdtype;
        key = tkey;
        val = tval;
        count = _count;
    }
    public String toString()
    {
        return desc[cmdtype] + "(" + key + "," + val + "):" + super.toString();
    }
}
