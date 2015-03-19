package org.corfudb.runtime.collections;

import org.corfudb.runtime.*;

import java.util.HashMap;

public class CDBPhysicalBTree<K extends Comparable<K>, V> extends CDBAbstractBTree<K, V> {

    public static final int M = 4;

    private long m_root;
    private int m_height;
    private int m_size;
    private HashMap<Long, Entry> m_entries;
    private HashMap<Long, Node> m_nodes;

    private static class NodeOp extends CorfuDBObjectCommand {

        static final int CMD_READ_CHILD_COUNT = 1;
        static final int CMD_WRITE_CHILD_COUNT = 2;
        static final int CMD_READ_CHILDREN = 3;
        static final int CMD_READ_CHILD = 5;
        static final int CMD_WRITE_CHILD = 6;

        public int m_cmd;
        public long m_oidnode;
        public int m_childindex;
        public int m_childcount;
        public long m_oidparam;
        public int cmd() { return m_cmd; }
        public long oidnode() { return m_oidnode; }
        public int childindex() { return m_childindex; }
        public int childcount() { return m_childcount; }
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
            m_oidnode = _oid;
            m_childindex = _index;
            m_childcount = _num;
            m_oidparam = _oidparam;
        }

        public static NodeOp readChildCountCmd(long _oidnode) { return new NodeOp(CMD_READ_CHILD_COUNT, _oidnode, 0, 0, 0); }
        public static NodeOp readChildrenCmd(long _oidnode) { return new NodeOp(CMD_READ_CHILDREN, _oidnode, 0, 0, 0); }
        public static NodeOp readChildCmd(long _oidnode, int _index) { return new NodeOp(CMD_READ_CHILD, _oidnode, _index, 0, 0); }
        public static NodeOp writeChildCountCmd(long _oidnode, int _count) { return new NodeOp(CMD_WRITE_CHILD_COUNT, _oidnode, 0, _count, 0);}
        public static NodeOp writeChildCmd(long _oidnode, int _index, long _oidparam) { return new NodeOp(CMD_WRITE_CHILD, _oidnode, _index, 0, _oidparam); }
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
        applyToObject(Object bs, ITimestamp timestamp) {

            NodeOp cc = (NodeOp) bs;
            switch (cc.cmd()) {
                case NodeOp.CMD_READ_CHILD_COUNT: cc.setReturnValue(applyReadChildCount()); break;
                case NodeOp.CMD_WRITE_CHILD_COUNT: applyWriteChildCount(cc.childcount()); break;
                case NodeOp.CMD_READ_CHILDREN: cc.setReturnValue(applyReadChildren()); break;
                case NodeOp.CMD_READ_CHILD: cc.setReturnValue(applyReadChild(cc.childindex())); break;
                case NodeOp.CMD_WRITE_CHILD: applyWriteChild(cc.childindex(), cc.oidparam()); break;
            }
        }

        /**
         * read the child count
         * @return the number of children in the given node
         */
        public int
        applyReadChildCount() {
            rlock();
            try {
                return m_nChildren;
            } finally {
                runlock();
            }
        }

        /**
         * write the child count
         * @param n
         */
        public void
        applyWriteChildCount(int n) {
            wlock();
            try {
                m_nChildren = n;
            } finally {
                wunlock();
            }
        }

        /**
         * apply an indexed read child operation
         * @param index
         * @return
         */
        public long
        applyReadChild(int index) {
            rlock();
            try {
                return m_vChildren[index];
            } finally {
                runlock();
            }
        }

        /**
         * apply a write child operation
         * @param n
         * @param oid
         */
        public void
        applyWriteChild(int n, long oid) {
            wlock();
            try {
                m_vChildren[n] = oid;
            } finally {
                wunlock();
            }
        }

        /**
         * apply a read children operation
         * @return
         */
        public long[]
        applyReadChildren() {
            rlock();
            try {
                return m_vChildren;
            } finally {
                runlock();
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
        static final int CMD_READ_DELETED = 7;
        static final int CMD_WRITE_DELETED = 8;

        public int m_cmd;
        public long m_oid;
        public K m_key;
        public V m_value;
        public long m_oidnext;
        public boolean m_deleted;
        public int cmd() { return m_cmd; }
        public K key() { return m_key; }
        public V value() { return m_value; }
        public long oid() { return m_oid; }
        public long oidnext() { return m_oidnext; }
        public boolean deleted() { return m_deleted; }

        /**
         * ctor
         * @param _cmd
         * @param _oid
         * @param key
         * @param value
         * @param oidnext
         * @param deleted
         */
        private
        EntryOp(
                int _cmd,
                long _oid,
                K key,
                V value,
                long oidnext,
                boolean deleted
            )
        {
            m_cmd = _cmd;
            m_oid = _oid;
            m_key = key;
            m_value = value;
            m_oidnext = oidnext;
            m_deleted = deleted;
        }

        public static <K extends Comparable<K>, V> EntryOp<K, V> readKeyCmd(long _oid) { return new EntryOp<K, V>(CMD_READ_KEY, _oid, null, null, oidnull, false); }
        public static <K extends Comparable<K>, V> EntryOp<K, V> readValueCmd(long _oid) { return new EntryOp<K, V>(CMD_READ_VALUE, _oid, null, null, oidnull, false); }
        public static <K extends Comparable<K>, V> EntryOp<K, V> readNextCmd(long _oid) { return new EntryOp<K, V>(CMD_READ_NEXT, _oid, null, null, oidnull, false); }
        public static <K extends Comparable<K>, V> EntryOp<K, V> readDeletedCmd(long _oid) { return new EntryOp<K, V>(CMD_READ_DELETED, _oid, null, null, oidnull, false); }
        public static <K extends Comparable<K>, V> EntryOp<K, V> writeKeyCmd(long _oid, K _key) { return new EntryOp<K, V>(CMD_WRITE_KEY, _oid, _key, null, oidnull, false); }
        public static <K extends Comparable<K>, V> EntryOp<K, V> writeValueCmd(long _oid, V _value) { return new EntryOp<K, V>(CMD_WRITE_VALUE, _oid, null, _value, oidnull, false); }
        public static <K extends Comparable<K>, V> EntryOp<K, V> writeNextCmd(long _oid, long _oidnext) { return new EntryOp<K, V>(CMD_WRITE_NEXT, _oid, null, null, _oidnext, false); }
        public static <K extends Comparable<K>, V> EntryOp<K, V> writeDeletedCmd(long _oid, boolean b) { return new EntryOp<K, V>(CMD_WRITE_DELETED, _oid, null, null, oidnull, b); }
    }

    private static class Entry<K extends Comparable<K>, V> extends CorfuDBObject {

        private Comparable key;
        private V value;
        private long oidnext;
        private boolean deleted;
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
            deleted = false;
        }

        /**
         * corfu runtime upcall
         * @param bs
         * @param timestamp
         */
        public void
        applyToObject(Object bs, ITimestamp timestamp) {

            EntryOp<K,V> cc = (EntryOp<K,V>) bs;
            switch (cc.cmd()) {
                case EntryOp.CMD_READ_KEY: cc.setReturnValue(applyReadKey()); break;
                case EntryOp.CMD_WRITE_KEY: cc.setReturnValue(applyWriteKey(cc.key())); break;
                case EntryOp.CMD_READ_VALUE: cc.setReturnValue(applyReadValue()); break;
                case EntryOp.CMD_WRITE_VALUE: cc.setReturnValue(applyWriteValue(cc.value())); break;
                case EntryOp.CMD_READ_NEXT: cc.setReturnValue(applyReadNext()); break;
                case EntryOp.CMD_WRITE_NEXT: cc.setReturnValue(applyWriteNext(cc.oidnext())); break;
                case EntryOp.CMD_READ_DELETED: cc.setReturnValue(applyReadDeleted()); break;
                case EntryOp.CMD_WRITE_DELETED: cc.setReturnValue(applyWriteDeleted(cc.deleted())); break;
            }
        }

        /**
         * read the deleted flag on this entry
         * @return
         */
        public Boolean applyReadDeleted() {
            rlock();
            try {
                return deleted;
            } finally {
                runlock();
            }
        }

        /**
         * apply a delete command
         * @param b
         * @return
         */
        public Boolean applyWriteDeleted(boolean b) {
            wlock();
            try {
                Boolean oval = deleted;
                deleted = b;
                return oval;
            } finally {
                wunlock();
            }
        }

        /**
         * read the key
         * @return
         */
        public K applyReadKey() {
            rlock();
            try {
                return (K)key;
            } finally {
                runlock();
            }
        }

        /**
         * write the key
         * @param k
         * @return
         */
        public K applyWriteKey(K k) {
            wlock();
            try {
                key = k;
            } finally {
                wunlock();
            }
            return k;
        }

        /**
         * read the value
         * @return
         */
        public V applyReadValue() {
            rlock();
            try {
                return value;
            } finally {
                runlock();
            }
        }

        /**
         * write the value
         * @param v
         * @return
         */
        public V applyWriteValue(V v) {
            wlock();
            try {
                value = v;
            } finally {
                wunlock();
            }
            return v;
        }

        /**
         * read the next pointer
         * @return
         */
        public long applyReadNext() {
            rlock();
            try {
                return oidnext;
            } finally {
                runlock();
            }
        }

        /**
         * write the next pointer
         * @param _next
         * @return
         */
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

    public static class BTreeOp extends CorfuDBObjectCommand {

        static final int CMD_READ_SIZE = 1;
        static final int CMD_READ_HEIGHT = 2;
        static final int CMD_READ_ROOT = 3;
        static final int CMD_WRITE_SIZE = 4;
        static final int CMD_WRITE_HEIGHT = 5;
        static final int CMD_WRITE_ROOT = 6;

        public int m_cmd;
        public long m_oid;
        public int m_iparam;
        public long m_oidparam;
        public int cmd() { return m_cmd; }
        public long oid() { return m_oid; }
        public int iparam() { return m_iparam; }
        public long oidparam() { return m_oidparam; }

        /**
         * ctor
         * @param _cmd
         * @param _oid
         * @param _iparam
         * @param _oidparam
         */
        private
        BTreeOp(
                int _cmd,
                long _oid,
                int _iparam,
                long _oidparam
            )
        {
            m_cmd = _cmd;
            m_oid = _oid;
            m_iparam = _iparam;
            m_oidparam = _oidparam;
        }

        public static BTreeOp readSizeCmd(long _oid) { return new BTreeOp(CMD_READ_SIZE, _oid, 0, oidnull); }
        public static BTreeOp readHeightCmd(long _oid) { return new BTreeOp(CMD_READ_HEIGHT, _oid, 0, oidnull); }
        public static BTreeOp readRootCmd(long _oid) { return new BTreeOp(CMD_READ_ROOT, _oid, 0, oidnull); }
        public static BTreeOp writeSizeCmd(long _oid, int i) { return new BTreeOp(CMD_WRITE_SIZE, _oid, i, oidnull); }
        public static BTreeOp writeHeightCmd(long _oid, int i) { return new BTreeOp(CMD_WRITE_HEIGHT, _oid, i, oidnull); }
        public static BTreeOp writeRootCmd(long _oid, long _param) { return new BTreeOp(CMD_WRITE_ROOT, _oid, 0, _param); }
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
     * corfu runtime upcall
     * @param bs
     * @param timestamp
     */
    public void
    applyToObject(Object bs, ITimestamp timestamp) {

        BTreeOp cc = (BTreeOp) bs;
        switch (cc.cmd()) {
            case BTreeOp.CMD_READ_HEIGHT: cc.setReturnValue(applyReadHeight()); break;
            case BTreeOp.CMD_WRITE_HEIGHT: applyWriteHeight(cc.iparam()); break;
            case BTreeOp.CMD_READ_ROOT: cc.setReturnValue(applyReadRoot()); break;
            case BTreeOp.CMD_WRITE_ROOT:  applyWriteRoot(cc.oidparam()); break;
            case BTreeOp.CMD_READ_SIZE: cc.setReturnValue(applyReadSize()); break;
            case BTreeOp.CMD_WRITE_SIZE: applyWriteSize(cc.iparam()); break;
        }
    }

    /**
     * print the b-tree
     * @return
     */
    public String print() {
        return print(readroot(), readheight(), "") + "\n";
    }

    /**
     * print helper function
     * @param node
     * @param height
     * @param indent
     * @return
     */
    private String
    print(Node<K, V> node, int height, String indent) {
        StringBuilder sb = new StringBuilder();
        long[] children = readchildren(node);
        int nChildren = readchildcount(node);
        if(height == 0) {
            for(int i=0; i<nChildren; i++) {
                Entry child = entryById(children[i]);
                sb.append(indent);
                sb.append(readkey(child));
                sb.append(" ");
                sb.append(readvalue(child));
                sb.append("\n");
            }
        } else {
            for(int i=0; i<nChildren; i++) {
                if(i>0) {
                    sb.append(indent);
                    sb.append("(");
                    sb.append(readkey(children[i]));
                    sb.append(")\n");
                }
                Node next = nodeById(readnext(children[i]));
                sb.append(print(next, height-1, indent + "    "));
            }
        }
        return sb.toString();
    }

    /**
     * return the size of the tree
     * @return
     */
    @Override
    public int size() {
        return readsize();
    }

    /**
     * return the height of the btree.
     * pray for a correct answer.
     * @return
     */
    public int height() {
        return readheight();
    }

    /**
     * maps. they rock.
     * @param key
     * @return
     */
    public V get(K key) {
        long root = readrootoid();
        int height = readheight();
        return search(root, key, height);
    }

    /**
     *
     * @param key
     * @return
     */
    public V remove(K key) {
        V result = null;
        long root = readrootoid();
        int height = readheight();
        Entry entry = searchEntry(root, key, height);
        if(entry != null) {
            boolean deleted = readdeleted(entry);
            if(!deleted) {
                result = (V) readvalue(entry);
                writedeleted(entry, true);
                int size = readsize();
                writesize(size-1);
                return result;
            }
        }
        return result;
    }

    /**
     * clear the tree
     */
    public void clear() {
        long root = readrootoid();
        writeroot(oidnull);
        writesize(0);
        writeheight(0);
    }


    /**
     * maps. was their rocking-ness mentioned?
     * @param key
     * @param value
     */
    public void
    put(K key, V value) {
        long root = readrootoid();
        int height = readheight();
        int size = readsize();
        long unodeoid = insert(root, key, value, height);
        writesize(size+1);
        if(unodeoid != oidnull) {
            // split required
            Node t = allocNode(2);
            long[] rootchildren = readchildren(nodeById(root));
            long[] uchildren = readchildren(nodeById(unodeoid));
            Comparable r0key = readkey(entryById(rootchildren[0]));
            Comparable u0key = readkey(entryById(uchildren[0]));

            Entry tc0 = allocEntry((K) r0key, null);
            Entry tc1 = allocEntry((K) u0key, null);
            writenext(tc0, root);
            writenext(tc1, unodeoid);
            writeroot(t.oid);
            writeheight(height+1);
        }
    }

    /**
     * search for a key starting at the given
     * node and height
     * @param oidnode
     * @param key
     * @param height
     * @return
     */
    private V
    search(
        long oidnode,
        K key,
        int height
        )
    {
        Node<K,V> node = nodeById(oidnode);
        long[] children = readchildren(node);
        int nChildren = readchildcount(node);

        if(height == 0) {
            // external node
            for(int i=0; i<nChildren; i++) {
                long oidchild = children[i];
                Entry child = entryById(oidchild);
                Comparable ckey = readkey(child);
                if(eq(key, ckey))
                    return (V) readvalue(child);
            }
        } else {
            // internal node
            for(int i=0; i<nChildren; i++) {
                if(i+1 == nChildren || lt(key, readkey(children[i + 1]))) {
                    return search(readnext(children[i]), key, height-1);
                }
            }
        }
        return null;
    }

    /**
     * search for a key starting at the given
     * node and height
     * @param oidnode
     * @param key
     * @param height
     * @return
     */
    private Entry
    searchEntry(
            long oidnode,
            K key,
            int height
    )
    {
        Node<K,V> node = nodeById(oidnode);
        long[] children = readchildren(node);
        int nChildren = readchildcount(node);

        if(height == 0) {
            // external node
            for(int i=0; i<nChildren; i++) {
                long oidchild = children[i];
                Entry child = entryById(oidchild);
                Comparable ckey = readkey(child);
                if(eq(key, ckey))
                    return child;
            }
        } else {
            // internal node
            for(int i=0; i<nChildren; i++) {
                if(i+1 == nChildren || lt(key, readkey(children[i + 1]))) {
                    return searchEntry(readnext(children[i]), key, height-1);
                }
            }
        }
        return null;
    }

    /**
     * insert a node starting at the given parent
     * @param oidnode
     * @param key
     * @param value
     * @param height
     * @return oid of a node to be split, if needed
     */
    private long
    insert(
        long oidnode,
        K key,
        V value,
        int height
        )
    {
        int idx = 0;
        Node<K,V> node = nodeById(oidnode);
        long[] children = readchildren(node);
        int nChildren = readchildcount(node);
        Entry entry = allocEntry(key, value);

        if(height == 0) {
            for(idx=0; idx<nChildren; idx++)
                if(lt(key, readkey(children[idx])))
                    break;
        } else {
            // internal node
            for(idx=0; idx<nChildren; idx++) {
                if(idx+1==nChildren || lt(key, readkey(children[idx+1]))) {
                    long oidunode = insert(readnext(children[idx++]), key, value, height-1);
                    if(oidunode == oidnull)
                        break;
                    Node<K, V> unode = nodeById(oidunode);
                    long[] uchildren = readchildren(unode);
                    Entry<K, V> uentry0 = entryById(uchildren[0]);
                    Comparable ukey = readkey(uentry0);
                    writekey(entry, ukey);
                    writenext(entry, oidunode);
                    break;
                }
            }
        }

        for(int i=nChildren; i>idx; i--)
            writechild(node, i, children[i-1]);
        writechild(node, idx, entry.oid);
        writechildcount(node, nChildren+1);
        if(nChildren+1 < M)
            return oidnull;
        return split(node);
    }

    /**
     * split a full node
     * @param node
     * @return
     */
    private long
    split(
        Node node
        )
    {
        Node t = allocNode(M/2);
        long[] children = readchildren(node);
        writechildcount(node, M/2);
        for(int i=0; i<M/2; i++)
            writechild(t, i, children[M/2+i]);
        return t.oid;
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

        long noid = newnode.oid;
        TR.update_helper(newnode, NodeOp.writeChildCountCmd(noid, nChildren));
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

        long noid = newentry.oid;
        TR.update_helper(newentry, EntryOp.writeKeyCmd(noid, k));
        TR.update_helper(newentry, EntryOp.writeValueCmd(noid, v));
        TR.update_helper(newentry, EntryOp.writeNextCmd(noid, oidnull));
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
        BTreeOp cmd = BTreeOp.readRootCmd(oid);
        if (TR.query_helper(this, null, cmd))
            return nodeById((long) cmd.getReturnValue());
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
     * read the root oid
     * @return
     */
    private long readrootoid() {
        BTreeOp cmd = BTreeOp.readRootCmd(oid);
        if(!TR.query_helper(this, null, cmd))
            return applyReadRoot();
        return (long) cmd.getReturnValue();
    }

    /**
     * read the size
     * @return
     */
    private int readsize() {
        BTreeOp cmd = BTreeOp.readSizeCmd(oid);
        if(!TR.query_helper(this, null, cmd))
            return applyReadSize();
        return (int) cmd.getReturnValue();
    }

    /**
     * read the size
     * @return
     */
    private int readheight() {
        BTreeOp cmd = BTreeOp.readHeightCmd(oid);
        if(!TR.query_helper(this, null, cmd))
            return applyReadHeight();
        return (int) cmd.getReturnValue();
    }

    /**
     * write the child oid at the given index in the node
     * @param node
     * @param index
     * @param oidchild
     */
    private void
    writechild(
        Node<K,V> node,
        int index,
        long oidchild
        )
    {
        NodeOp cmd = NodeOp.writeChildCmd(node.oid, index, oidchild);
        TR.update_helper(node, cmd);
    }

    /**
     * write the node's child count
     * @param node
     * @param count
     */
    private void
    writechildcount(
        Node<K,V> node,
        int count
        )
    {
        NodeOp cmd = NodeOp.writeChildCountCmd(node.oid, count);
        TR.update_helper(node, cmd);
    }

    /**
     * write the entry key
     * @param entry
     * @param ckey
     */
    private void
    writekey(
        Entry<K, V> entry,
        Comparable ckey
        )
    {
        EntryOp<K,V> cmd = EntryOp.writeKeyCmd(entry.oid, (K) ckey);
        TR.update_helper(entry, cmd);
    }

    /**
     * write the entry value
     * @param entry
     * @param value
     */
    private void
    writevalue(
        Entry<K, V> entry,
        V value
        )
    {
        EntryOp<K,V> cmd = EntryOp.writeValueCmd(entry.oid, value);
        TR.update_helper(entry, cmd);
    }

    /**
     * write the entry's next pointer
     * @param entry
     * @param next
     */
    private void
    writenext(
        Entry<K, V> entry,
        long next
        )
    {
        EntryOp<K,V> cmd = EntryOp.writeNextCmd(entry.oid, next);
        TR.update_helper(entry, cmd);
    }

    /**
     * write the deleted flag for the entry
     * @param entry
     * @param deleted
     */
    private void
    writedeleted(
        Entry<K, V> entry,
        boolean deleted
        )
    {
        EntryOp<K,V> cmd = EntryOp.writeDeletedCmd(entry.oid, deleted);
        TR.update_helper(entry, cmd);
    }

    /**
     * read the key of the given entry
     * @param entry
     * @return
     */
    private Comparable
    readkey(
        Entry<K, V> entry
        )
    {
        EntryOp<K, V> cmd = EntryOp.readKeyCmd(entry.oid);
        if(!TR.query_helper(entry, null, cmd))
            return entry.applyReadKey();
        return (Comparable) cmd.getReturnValue();
    }

    /**
     * read the key of the given entry
     * @param entry
     * @return
     */
    private boolean
    readdeleted(
        Entry<K, V> entry
        )
    {
        EntryOp<K, V> cmd = EntryOp.readDeletedCmd(entry.oid);
        if(!TR.query_helper(entry, null, cmd))
            return entry.applyReadDeleted();
        return (boolean) cmd.getReturnValue();
    }

    /**
     * read the key of the given entry
     * @param entryoid
     * @return
     */
    private Comparable
    readkey(
        long entryoid
        )
    {
        EntryOp<K, V> cmd = EntryOp.readKeyCmd(entryoid);
        Entry<K, V> entry = entryById(entryoid);
        if(!TR.query_helper(entry, null, cmd))
            return entry.applyReadKey();
        return (Comparable) cmd.getReturnValue();
    }

    /**
     * read the value field of the given node
     * @param entry
     * @return
     */
    private V
    readvalue(
        Entry<K, V> entry
        )
    {
        EntryOp<K,V> cmd = EntryOp.readValueCmd(entry.oid);
        if(!TR.query_helper(entry, null, cmd))
            return entry.applyReadValue();
        return (V) cmd.getReturnValue();
    }

    /**
     * read the next pointer of the entry
     * @param entry
     * @return
     */
    private long
    readnext(
        Entry<K, V> entry
        )
    {
        EntryOp<K,V> cmd = EntryOp.readNextCmd(entry.oid);
        if(!TR.query_helper(entry, null, cmd))
            return entry.applyReadNext();
        return (long) cmd.getReturnValue();
    }

    /**
     * read the next pointer of the entry
     * @param entryoid
     * @return
     */
    private long
    readnext(
        long entryoid
        )
    {
        Entry<K, V> entry = entryById(entryoid);
        EntryOp<K,V> cmd = EntryOp.readNextCmd(entry.oid);
        if(!TR.query_helper(entry, null, cmd))
            return entry.applyReadNext();
        return (long) cmd.getReturnValue();
    }


    /**
     * get the children array for the given node
     * @param node
     * @return
     */
    private long[]
    readchildren(
        Node<K,V> node
        )
    {
        NodeOp cmd = NodeOp.readChildrenCmd(node.oid);
        if(!TR.query_helper(node, null, cmd)) {
            return node.applyReadChildren();
        }
        return (long[]) cmd.getReturnValue();
    }

    /**
     * read the number of valid child pointers
     * in the given node.
     * @param node
     * @return
     */
    private int
    readchildcount(
        Node<K,V> node
        )
    {
        NodeOp cmd = NodeOp.readChildCountCmd(node.oid);
        if(!TR.query_helper(node, null, cmd))
            return node.applyReadChildCount();
        return (int) cmd.getReturnValue();
    }

    /**
     * write the size field of the btree
     * @param size
     */
    private void writesize(int size) {
        BTreeOp cmd = BTreeOp.writeSizeCmd(oid, size);
        TR.update_helper(this, cmd);
    }

    /**
     * write the height field
     * @param height
     */
    private void writeheight(int height) {
        BTreeOp cmd = BTreeOp.writeHeightCmd(oid, height);
        TR.update_helper(this, cmd);
    }

    /**
     * write the root member
     * @param root
     */
    private void writeroot(long root) {
        BTreeOp cmd = BTreeOp.writeRootCmd(oid, root);
        TR.update_helper(this, cmd);
    }


}


