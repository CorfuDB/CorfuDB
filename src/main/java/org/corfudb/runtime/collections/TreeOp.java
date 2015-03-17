package org.corfudb.runtime.collections;

import org.corfudb.runtime.CorfuDBObjectCommand;

class TreeOp<K extends Comparable<K>, V> extends CorfuDBObjectCommand {

    static final int CMD_SIZE = 1;
    static final int CMD_HEIGHT = 2;
    static final int CMD_GET = 3;
    static final int CMD_PUT = 4;

    public int m_cmd;
    public long m_oid;
    public K m_key;
    public V m_value;
    public int cmd() { return m_cmd; }
    public K key() { return m_key; }
    public V value() { return m_value; }
    public long oid() { return m_oid; }

    public
    TreeOp(
        int _cmd,
        long _oid,
        K key,
        V value
    )
    {
        m_cmd = _cmd;
        m_oid = _oid;
        m_key = key;
        m_value = value;
    }

}