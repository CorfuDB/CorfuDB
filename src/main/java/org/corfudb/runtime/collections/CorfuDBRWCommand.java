package org.corfudb.runtime.collections;

import org.corfudb.runtime.CorfuDBObject;
import org.corfudb.runtime.CorfuDBObjectCommand;

import java.util.Collection;
import java.util.List;

class RWCommand<E> extends CorfuDBObjectCommand {

    static final int CMD_READ = 0;
    static final int CMD_WRITE_VALUE = 1;
    static final int CMD_READ_NEXT = 2;
    static final int CMD_WRITE_HEAD = 3;
    static final int CMD_WRITE_NEXT = 4;
    public E m_elem;
    public int m_cmd;
    public long m_oid;
    public long m_next = -1;
    public CDBListNode<E> m_node = null;
    public int cmd() {
        return m_cmd;
    }
    public E e() {
        return m_elem;
    }
    public long oid() { return m_oid; }
    public long next() { return m_next; }
    public RWCommand(int cmd, E elem, long oid) {
        m_cmd = cmd;
        m_elem = elem;
        m_oid = oid;
        m_next = -1;
    }
    public RWCommand(int cmd, CDBListNode<E> node, long oid) {
        m_cmd = cmd;
        m_elem = null;
        m_oid = oid;
        m_next = -1;
        m_node = node;
    }
    public RWCommand(int cmd, E elem, long oid, long next) {
        m_cmd = cmd;
        m_elem = elem;
        m_oid = oid;
        m_next = next;
    }
    public RWCommand(int cmd, long oid) {
        m_cmd = cmd;
        m_oid = oid;
    }
}