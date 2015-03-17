package org.corfudb.runtime.collections;

import org.corfudb.runtime.*;

import java.util.HashMap;

public abstract class CDBPhysicalBTree<K extends Comparable<K>, V> extends CDBAbstractBTree<K, V> {

    public static final int M = 4;

    private long m_root;
    private int m_height;
    private int m_size;
    private HashMap<Long, Entry> m_entries;
    private HashMap<Long, Node> m_nodes;

    private static class NodeOp<K extends Comparable<K>, V> extends CorfuDBObjectCommand {

        static final int CMD_READ_N_CHILDREN = 1;
        static final int CMD_WRITE_N_CHILDREN = 2;
        static final int CMD_READ_V_CHILDREN = 3;
        static final int CMD_WRITE_V_CHILDREN = 4;

        public int m_cmd;
        public long m_oid;
        public int m_index;
        public int m_num;
        public long m_oidparam;
        public int cmd() { return m_cmd; }
        public long oid() { return m_oid; }
        public int index() { return m_index; }
        public int num() { return m_num; }
        public long oidparam() { return m_oidparam; }

        public
        NodeOp(
                int _cmd,
                long _oid,
                int _index,
                int _num,
                long _oidparam
            )
        {
            m_cmd = _cmd;
            m_oid = _oid;
            m_index = _index;
            m_num = _num;
            m_oidparam = _oidparam;
        }

        public
        NodeOp(
                int _cmd,
                long _oid,
                int _num
            )
        {
            m_cmd = _cmd;
            m_oid = _oid;
            m_num = _num;
        }
    }

    private static class Node<K extends Comparable<K>, V> extends CorfuDBObject {

        private int m_nChildren;
        private long[] m_vChildren;

        private Node(
            AbstractRuntime tr,
            long toid,
            int nChildren
            )
        {
            super(tr, toid);
            m_vChildren = new long[M];
            m_nChildren = nChildren;
        }

        /**
         * corfu runtime upcall
         * @param bs
         * @param timestamp
         */
        public void
        applyToObject(Object bs, long timestamp) {

            NodeOp<K,V> cc = (NodeOp<K,V>) bs;
            switch (cc.cmd()) {
                case NodeOp.CMD_READ_N_CHILDREN: cc.setReturnValue(applyReadNChildren()); break;
                case NodeOp.CMD_WRITE_N_CHILDREN: applyWriteNChildren(cc.num()); break;
                case NodeOp.CMD_READ_V_CHILDREN: cc.setReturnValue(applyReadVChildren(cc.index())); break;
                case NodeOp.CMD_WRITE_V_CHILDREN: applyWriteVChildren(cc.index(), cc.oidparam()); break;
            }
        }

        public int applyReadNChildren() {
            rlock();
            try {
                return m_nChildren;
            } finally {
                runlock();
            }
        }

        public void applyWriteNChildren(int n) {
            wlock();
            try {
                m_nChildren = n;
            } finally {
                wunlock();
            }
        }

        public long applyReadVChildren(int index) {
            rlock();
            try {
                return m_vChildren[index];
            } finally {
                runlock();
            }
        }

        public void applyWriteVChildren(int n, long oid) {
            wlock();
            try {
                m_vChildren[n] = oid;
            } finally {
                wunlock();
            }
        }
    }

    private static class EntryOp<K extends Comparable<K>, V> extends CorfuDBObjectCommand {

        static final int CMD_READ_KEY = 1;
        static final int CMD_WRITE_KEY = 2;
        static final int CMD_READ_VALUE = 3;
        static final int CMD_WRITE_VALUE = 4;
        static final int CMD_READ_NEXT = 5;
        static final int CMD_WRITE_NEXT = 6;

        public int m_cmd;
        public long m_oid;
        public K m_key;
        public V m_value;
        public long m_oidnext;
        public int cmd() { return m_cmd; }
        public K key() { return m_key; }
        public V value() { return m_value; }
        public long oid() { return m_oid; }
        public long oidnext() { return m_oidnext; }

        public
        EntryOp(
                int _cmd,
                long _oid,
                K key,
                V value,
                long oidnext
            )
        {
            m_cmd = _cmd;
            m_oid = _oid;
            m_key = key;
            m_value = value;
            m_oidnext = oidnext;
        }
    }

    private static class Entry<K extends Comparable<K>, V> extends CorfuDBObject {

        private Comparable key;
        private V value;
        private long oidnext;
        public Entry(
                AbstractRuntime tr,
                long toid,
                K _key,
                V _value,
                long _next
            )
        {
            super(tr, toid);
            key = _key;
            value = _value;
            oidnext = _next;
        }

        /**
         * corfu runtime upcall
         * @param bs
         * @param timestamp
         */
        public void
        applyToObject(Object bs, long timestamp) {

            EntryOp<K,V> cc = (EntryOp<K,V>) bs;
            switch (cc.cmd()) {
                case EntryOp.CMD_READ_KEY: cc.setReturnValue(applyReadKey()); break;
                case EntryOp.CMD_WRITE_KEY: cc.setReturnValue(applyWriteKey(cc.key())); break;
                case EntryOp.CMD_READ_VALUE: cc.setReturnValue(applyReadValue()); break;
                case EntryOp.CMD_WRITE_VALUE: cc.setReturnValue(applyWriteValue(cc.value())); break;
                case EntryOp.CMD_READ_NEXT: cc.setReturnValue(applyReadNext()); break;
                case EntryOp.CMD_WRITE_NEXT: cc.setReturnValue(applyWriteNext(cc.oidnext())); break;
            }
        }

        public K applyReadKey() {
            rlock();
            try {
                return (K)key;
            } finally {
                runlock();
            }
        }

        public K applyWriteKey(K k) {
            wlock();
            try {
                key = k;
            } finally {
                wunlock();
            }
            return k;
        }

        public V applyReadValue() {
            rlock();
            try {
                return value;
            } finally {
                runlock();
            }
        }

        public V applyWriteValue(V v) {
            wlock();
            try {
                value = v;
            } finally {
                wunlock();
            }
            return v;
        }

        public long applyReadNext() {
            rlock();
            try {
                return oidnext;
            } finally {
                runlock();
            }
        }

        public long applyWriteNext(long _next) {
            wlock();
            try {
                oidnext = _next;
            } finally {
                wunlock();
            }
            return oidnext;
        }
    }

    /**
     * ctor
     * @param tTR
     * @param tsf
     * @param toid
     */
    public CDBPhysicalBTree(
            AbstractRuntime tTR,
            StreamFactory tsf,
            long toid
        )
    {
        super(tTR, tsf, toid);
        m_entries = new HashMap<Long, Entry>();
        m_nodes = new HashMap<Long, Node>();
    }

    /**
     * find the given node
     * @param noid
     * @return
     */
    protected Node<K,V>
    nodeById(long noid) {
        rlock();
        try {
            return m_nodes.getOrDefault(noid, null);
        } finally {
            runlock();
        }
    }

    /**
     * find the given node
     * @param noid
     * @return
     */
    protected Entry<K,V>
    entryById(long noid) {
        rlock();
        try {
            return m_entries.getOrDefault(noid, null);
        } finally {
            runlock();
        }
    }

    /**
     * allocate a new node.
     * @param nChildren
     * @return
     */
    private Node<K, V>
    allocNode(int nChildren) {
        Node<K, V> newnode = new Node<K, V>(TR, DirectoryService.getUniqueID(sf), nChildren);
        wlock();
        try {
            m_nodes.put(newnode.oid, newnode);
        } finally {
            wunlock();
        }
        TR.update_helper(newnode, new NodeOp<K,V>(NodeOp.CMD_WRITE_N_CHILDREN, newnode.oid, nChildren));
        return newnode;
    }

    /**
     * allocate a new entry
     * @param k
     * @param v
     * @return
     */
    private Entry<K, V>
    allocEntry(K k, V v) {
        Entry<K, V> newentry = new Entry<K, V>(TR, DirectoryService.getUniqueID(sf), k, v, oidnull);
        wlock();
        try {
            m_entries.put(newentry.oid, newentry);
        } finally {
            wunlock();
        }

        TR.update_helper(newentry, new EntryOp<K,V>(EntryOp.CMD_WRITE_KEY, newentry.oid, k, null, oidnull));
        TR.update_helper(newentry, new EntryOp<K,V>(EntryOp.CMD_WRITE_VALUE, newentry.oid, k, null, oidnull));
        TR.update_helper(newentry, new EntryOp<K,V>(EntryOp.CMD_WRITE_NEXT, newentry.oid, k, null, oidnull));
        return newentry;
    }

    /**
     * read the root of the tree
     * Note, this has the effect of inserting the tree container
     * object into the read set, but does not put the actual node
     * there. If query_helper returns false, it means we've already
     * read the tree root in the current transaction, so we're forced to
     * return the most recently observed value.
     * @return
     */
    protected Node<K, V>
    readroot() {
        PTreeOp<K, V> cmd = new PTreeOp<>(PTreeOp.CMD_READ_ROOT, oid);
        if (TR.query_helper(this, null, cmd))
            return nodeById((long)cmd.getReturnValue());
        rlock();
        try {
            return nodeById(m_root);
        } finally {
            runlock();
        }
    }

    /**
     * return the size based on the most recent view
     * @return
     */
    public int applyReadSize() {
        rlock();
        try {
            return m_size;
        } finally {
            runlock();
        }
    }

    /**
     * return the height based on the current view
     * @return
     */
    public int applyReadHeight() {
        rlock();
        try {
            return m_height;
        } finally {
            runlock();
        }
    }

    /**
     * return the size based on the most recent view
     * @return
     */
    public void applyWriteSize(int size) {
        wlock();
        try {
            m_size = size;
        } finally {
            runlock();
        }
    }

    /**
     * return the height based on the current view
     * @return
     */
    public void applyWriteHeight(int height) {
        wlock();
        try {
            m_height = height;
        } finally {
            wunlock();
        }
    }

    /**
     * return the size based on the most recent view
     * @return
     */
    public long applyReadRoot() {
        rlock();
        try {
            return m_size;
        } finally {
            runlock();
        }
    }

    public void applyWriteRoot(long _oid) {
        wlock();
        try {
            m_root = _oid;
        } finally {
            wunlock();
        }
    }

    /**
     * TODO: implement this right!
     * @return
     */
    public String print() {
        return toString();
    }

    public abstract int size();
    public abstract int height();
    public abstract V get();
    public abstract void put(K key, V value);

    class PTreeOp<K extends Comparable<K>, V> extends CorfuDBObjectCommand {

        static final int CMD_SIZE = 1;
        static final int CMD_HEIGHT = 2;
        static final int CMD_READ_ROOT = 3;
        static final int CMD_WRITE_ROOT = 4;

        public int m_cmd;
        public long m_oid;
        public int cmd() { return m_cmd; }
        public long oid() { return m_oid; }

        public
        PTreeOp(
                int _cmd,
                long _oid
            )
        {
            m_cmd = _cmd;
            m_oid = _oid;
        }

    }
}


