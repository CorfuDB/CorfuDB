package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.CorfuDBObjectCommand;
import java.util.Collection;
import java.util.List;

class ListCommand<E> extends CorfuDBObjectCommand {

    static final int CMD_ADD = 0;
    static final int CMD_ADD_AT = 1;
    static final int CMD_ADD_ALL = 2;
    static final int CMD_CLEAR = 3;
    static final int CMD_CONTAINS = 4;
    static final int CMD_CONTAINS_ALL = 5;
    static final int CMD_EQUALS = 6;
    static final int CMD_GET = 7;
    static final int CMD_HASH_CODE = 8; // an interesting corner case...support it?
    static final int CMD_INDEX_OF = 9;
    static final int CMD_LAST_INDEX_OF = 10;
    static final int CMD_REMOVE_OBJ = 11;
    static final int CMD_REMOVE_AT = 12;
    static final int CMD_REMOVE_ALL = 13;
    static final int CMD_REPLACE_ALL = 14;
    static final int CMD_RETAIN_ALL = 15;
    static final int CMD_SET = 16;
    static final int CMD_SIZE = 17;
    static final int CMD_SORT = 18;         // should not support at command layer...
    static final int CMD_SPLITERATOR = 19;  // should not support at command layer...
    static final int CMD_SUBLIST = 20;      // should not support at command layer...
    static final int CMD_TO_ARRAY = 21;     // should not support at command layer...
    static final int CMD_REMOVE_IF = 22;    // should not support at command layer...
    static final int CMD_IS_EMPTY = 23;
    static final int CMD_PRE_ADD = 24;
    static final int CMD_POST_ADD = 25;

    E m_elem;
    int m_index = 0;
    List<E> m_parm;
    int m_cmd;
    long m_oid = -1;

    public void setID(long oid) { m_oid = oid;}
    public long oid() { return m_oid; }

    public int getcmd() {
        return m_cmd;
    }

    public E e() {
        return m_elem;
    }

    public int index() {
        return m_index;
    }

    public List<E> lparm() {
        return m_parm;
    }

    public ListCommand(int cmd) {
        this(cmd, null, -1, null);
    }

    public ListCommand(int cmd, int idx) {
        this(cmd, null, idx, null);
    }

    public ListCommand(int cmd, E e) {
        this(cmd, e, -1, null);
    }

    public ListCommand(int cmd, E e, int idx) {
        this(cmd, e, idx, null);
    }

    public ListCommand(int cmd, List<E> lparm) {
        this(cmd, null, -1, lparm);
    }

    public ListCommand(int cmd, Collection<?> lparm) {
        m_cmd = cmd;
        m_index = -1;
        m_elem = null;
        m_parm = (List<E>) lparm;
    }

    public ListCommand(int cmd, E e, int index, List<E> lparm) {
        m_cmd = cmd;
        m_index = index;
        m_elem = e;
        m_parm = lparm;
    }
}