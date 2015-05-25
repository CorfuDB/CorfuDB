package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.legacy.CorfuDBObjectCommand;

import java.util.UUID;

class TreeOp<K extends Comparable<K>, V> extends CorfuDBObjectCommand {

    static final int CMD_SIZE = 1;
    static final int CMD_HEIGHT = 2;
    static final int CMD_GET = 3;
    static final int CMD_PUT = 4;
    static final int CMD_REMOVE = 5;
    static final int CMD_CLEAR = 6;
    static final int CMD_UPDATE = 7;
    public static String cmdstr(int i) {
        switch(i) {
            case CMD_SIZE:
                return "CMD_SIZE";
            case CMD_HEIGHT:
                return "CMD_HEIGHT";
            case CMD_GET:
                return "CMD_GET";
            case CMD_PUT:
                return "CMD_PUT";
            case CMD_REMOVE:
                return "CMD_REMOVE";
            case CMD_CLEAR:
                return "CMD_CLEAR";
            case CMD_UPDATE:
                return "CMD_UPDATE";
            default:
                return "CMD_INVALID";
        }
    }

    public long m_reqstart;
    public long m_reqcomplete;
    public int m_cmd;
    public UUID m_oid;
    public K m_key;
    public V m_value;
    public int cmd() { return m_cmd; }
    public K key() { return m_key; }
    public V value() { return m_value; }
    public UUID oid() { return m_oid; }
    public long latency() { if(m_reqstart != 0 && m_reqcomplete != 0) return m_reqcomplete - m_reqstart; return 0; }
    public void start() { m_reqstart = System.currentTimeMillis(); }
    public void complete() { m_reqcomplete = System.currentTimeMillis(); }

    public
    TreeOp(
        int _cmd,
        UUID _oid,
        K key,
        V value
        )
    {
        m_cmd = _cmd;
        m_oid = _oid;
        m_key = key;
        m_value = value;
        m_reqcomplete = 0;
        start();
    }

}