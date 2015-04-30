package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.CorfuDBObject;
import org.corfudb.runtime.smr.CorfuDBObjectCommand;

class NodeOp<E> extends CorfuDBObjectCommand {

    static final int CMD_READ_VALUE = 4;
    static final int CMD_READ_NEXT = 5;
    static final int CMD_READ_PREV = 6;
    static final int CMD_WRITE_VALUE = 7;
    static final int CMD_WRITE_NEXT = 8;
    static final int CMD_WRITE_PREV = 9;
    static final int CMD_READ_HEAD = 6;
    static final int CMD_WRITE_HEAD = 7;
    static final int CMD_READ_TAIL = 8;
    static final int CMD_WRITE_TAIL = 9;

    public int m_cmd;
    public long m_nodeid;
    public long m_oidparam;
    public E m_elemparam;
    public int cmd() { return m_cmd; }
    public E e() { return m_elemparam; }
    public long oidparam() { return m_oidparam; }
    public long nodeid() { return m_nodeid; }

    public NodeOp(int _cmd,
                  long _oid,
                  long _oidparam) {
        m_cmd = _cmd;
        m_nodeid = _oid;
        m_oidparam = _oidparam;
    }

    public NodeOp(int _cmd,
                  long _oidparam) {
        m_cmd = _cmd;
        m_nodeid = CorfuDBObject.oidnull;
        m_oidparam = _oidparam;
    }

    public NodeOp(int _cmd,
                  long _oid,
                  E _eparam) {
        m_cmd = _cmd;
        m_nodeid = _oid;
        m_oidparam = CorfuDBObject.oidnull;
        m_elemparam = _eparam;
    }

}