package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.AbstractRuntime;
import org.corfudb.runtime.smr.CorfuDBObjectCommand;
import org.corfudb.runtime.smr.IStreamFactory;
import org.corfudb.client.ITimestamp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

public class CDBLogicalBTree<K extends Comparable<K>, V> extends CDBAbstractBTree<K, V> {

    private Node m_root;
    private int m_height;
    private int m_size;

    private static class Node<K extends Comparable<K>, V> {
        private int m_nChildren;
        private Entry<K, V>[] m_vChildren;
        private Node(int nChildren) {
            m_vChildren = new Entry[M];
            m_nChildren = nChildren;
        }
    }

    private static class Entry<K extends Comparable<K>, V> {
        private Comparable key;
        private V value;
        private Node next;
        private boolean deleted;
        public Entry(K _key, V _value, Node _next) {
            key = _key;
            value = _value;
            next = _next;
            deleted = false;
        }
    }

    /**
     * if we are collecting latency
     * statistics, note the start time for this
     * request
     * @param _op
     */
    @Override
    protected void startRequestImpl(CorfuDBObjectCommand _op) {
        TreeOp<K, V> op = (TreeOp<K, V>)_op;
        if(op == null) return;
        op.start();
    }

    /**
     * log completion time. return true if
     * we actually completed this request through the
     * normal channels...
     * @param _op
     * @return
     */
    @Override
    protected boolean completeRequestImpl(CorfuDBObjectCommand _op) {
        TreeOp<K, V> op = (TreeOp<K, V>)_op;
        if(op == null) return false;
        op.complete();
        return op.latency() != 0;
    }

    /**
     * get the latency stats as a string
     * @param ops
     * @return
     */
    @Override
    protected String
    getLatencyStatsImpl(
            Collection<CorfuDBObjectCommand> ops
        ) {
        HashMap<Integer, ArrayList<TreeOp<K, V>>> map = new HashMap();
        for(CorfuDBObjectCommand _op : ops) {
            TreeOp<K, V> op = (TreeOp<K, V>)_op;
            if(op == null) continue;
            ArrayList<TreeOp<K,V>> list = map.getOrDefault(new Integer(op.cmd()), new ArrayList());
            list.add(op);
            map.put(op.cmd(), list);
        }
        StringBuilder sb = new StringBuilder();
        for(Integer cmd : map.keySet()) {
            sb.append("FSCMDLAT, ");
            sb.append(TreeOp.cmdstr(cmd));
            ArrayList<TreeOp<K,V>> list = map.get(cmd);
            for(TreeOp<K, V> op : list) {
                sb.append(", ");
                sb.append(op.latency());
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * ctor
     * @param tTR
     * @param tsf
     * @param toid
     */
    public
    CDBLogicalBTree(
        AbstractRuntime tTR,
        IStreamFactory tsf,
        long toid
        )
    {
        super(tTR, tsf, toid);
        m_root = new Node(0);
        m_height = 0;
        m_size = 0;
    }

    /**
     * corfu runtime upcall
     * @param bs
     * @param timestamp
     */
    public void
    applyToObject(Object bs, ITimestamp timestamp) {

        TreeOp<K,V> cc = (TreeOp<K,V>) bs;
        switch (cc.cmd()) {
            case TreeOp.CMD_GET: cc.setReturnValue(applyGet(cc.key())); break;
            case TreeOp.CMD_PUT: applyPut(cc.key(), cc.value()); break;
            case TreeOp.CMD_SIZE: cc.setReturnValue(applySize()); break;
            case TreeOp.CMD_HEIGHT:  cc.setReturnValue(applyHeight()); break;
            case TreeOp.CMD_CLEAR: applyClear(); break;
            case TreeOp.CMD_REMOVE: cc.setReturnValue(applyRemove(cc.key())); break;
            case TreeOp.CMD_UPDATE: cc.setReturnValue(applyUpdate(cc.key(), cc.value())); break;
        }
        cc.complete();
    }

    /**
     * apply a clear operation
     */
    public void applyClear() {
        wlock();
        try {
            m_root = new Node(0);
            m_size = 0;
            m_height = 0;
        } finally {
            wunlock();
        }
    }

    /**
     * apply a remove command by marking the
     * entry deleted (if found)
     * @param key
     * @return
     */
    public V applyRemove(K key) {
        wlock();
        try {
            Entry entry = searchEntry(m_root, key, m_height);
            if(entry != null) {
                V result = entry.deleted ? null : (V) entry.value;
                m_size -= entry.deleted ? 0 : 1;
                entry.deleted = true;
                return result;
            }
            return null;
        } finally {
            wunlock();
        }
    }


    /**
     * apply a get command
     * @param key
     * @return
     */
    public V applyGet(K key) {
        rlock();
        try {
            return search(m_root, key, m_height);
        } finally {
            runlock();
        }
    }

    /**
     * apply a put command
     * @param key
     * @param value
     */
    public void applyPut(K key, V value) {
        wlock();
        try {
            Node unode = insert(m_root, key, value, m_height);
            m_size++;
            if(unode != null) {
                // split required
                Node t = new Node(2);
                t.m_vChildren[0] = new Entry(m_root.m_vChildren[0].key, null, m_root);
                t.m_vChildren[1] = new Entry(unode.m_vChildren[0].key, null, unode);
                m_root = t;
                m_height++;
            }
        } finally {
            wunlock();
        }
    }

    /**
     * apply an update command
     * @param key
     * @param value
     */
    public Boolean applyUpdate(K key, V value) {
        wlock();
        try {
            Entry entry = searchEntry(m_root, key, m_height);
            if(entry != null && !entry.deleted) {
                entry.value = value;
                return true;
            }
            return false;
        } finally {
            wunlock();
        }
    }


    /**
     * return the size based on the most recent view
     * @return
     */
    public int applySize() {
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
    public int applyHeight() {
        rlock();
        try {
            return m_height;
        } finally {
            runlock();
        }
    }

    /**
     * return the size
     * @return
     */
    public int size() {
        TreeOp sizecmd = new TreeOp(TreeOp.CMD_SIZE, oid, null, null);
        if(!TR.query_helper(this, null, sizecmd)) {
            return applySize();
        }
        return (int) sizecmd.getReturnValue();
    }

    /**
     * return the height
     * @return
     */
    public int height() {
        TreeOp cmd = new TreeOp(TreeOp.CMD_HEIGHT, oid, null, null);
        if(!TR.query_helper(this, null, cmd)) {
            return applyHeight();
        }
        return (int) cmd.getReturnValue();
    }

    /**
     * get the value at the given key
     * @param key
     * @return
     */
    public V get(K key) {
        TreeOp cmd = new TreeOp(TreeOp.CMD_GET, oid, key, null);
        if(!TR.query_helper(this, null, cmd)) {
            return applyGet(key);
        }
        return (V) cmd.getReturnValue();

    }

    /**
     * put the value at the given key
     * @param key
     * @param value
     */
    public void put(K key, V value) {
        TreeOp cmd = new TreeOp(TreeOp.CMD_PUT, oid, key, value);
        TR.update_helper(this, cmd, oid);
    }

    /**
     * update the value at the given key
     * @param key
     * @param value
     */
    public boolean update(K key, V value) {
        TreeOp cmd = new TreeOp(TreeOp.CMD_UPDATE, oid, key, value);
        TR.update_helper(this, cmd, oid);
        return true;
    }

    /**
     * clear the tree
     */
    public void clear() {
        TreeOp cmd = new TreeOp(TreeOp.CMD_CLEAR, oid, null, null);
        TR.update_helper(this, cmd);
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
        return print(m_root, m_height, "") + "\n";
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

        if(node == null)
            return new String("");

        StringBuilder sb = new StringBuilder();
        Entry[] children = node.m_vChildren;
        int nChildren = node.m_nChildren;
        if(height == 0) {
            for(int i=0; i<nChildren; i++) {
                Entry child = children[i];
                sb.append(indent);
                if(child.deleted)
                    sb.append("DEL: ");
                sb.append(child.key);
                sb.append(" ");
                sb.append(child.value);
                sb.append("\n");
            }
        } else {
            for(int i=0; i<nChildren; i++) {
                if(i>0) {
                    sb.append(indent);
                    sb.append("(");
                    sb.append(children[i].key);
                    sb.append(")\n");
                }
                Node next = children[i].next;
                sb.append(print(next, height-1, indent + "    "));
            }
        }
        return sb.toString();
    }

    /**
     *
     * @param key
     * @return
     */
    public V remove(K key) {
        TreeOp cmd = new TreeOp(TreeOp.CMD_REMOVE, oid, key, null);
        TR.update_helper(this, cmd);
        rlock();
        try {
            return get(key);
        } finally {
            runlock();
        }
    }

    /**
     * search for a key starting at the given
     * node and height
     * @param node
     * @param key
     * @param height
     * @return
     */
    private V
    search(
        Node node,
        K key,
        int height
        )
    {
        Entry entry = searchEntry(node, key, height);
        if (entry == null) return null;
        if (entry.deleted) return null;
        return (V) entry.value;
    }

    /**
     * search for the entry with given key
     * starting at the given node and tree depth
     * @param node
     * @param key
     * @param height
     * @return
     */
    private Entry
    searchEntry(
        Node node,
        K key,
        int height
        )
    {
        Entry[] children = node.m_vChildren;

        if(height == 0) {
            // external node
            for(int i=0; i<node.m_nChildren; i++) {
                Entry child = node.m_vChildren[i];
                Comparable ckey = child.key;
                if(eq(key, ckey))
                    return child;
            }
        } else {
            // internal node
            for(int i=0; i<node.m_nChildren; i++) {
                if(i+1==node.m_nChildren || lt(key, children[i+1].key)) {
                    return searchEntry(children[i].next, key, height - 1);
                }
            }
        }
        return null;
    }

    /**
     * insert a node starting at the given parent
     * @param node
     * @param key
     * @param value
     * @param height
     * @return
     */
    private Node
    insert(
        Node node,
        K key,
        V value,
        int height
        )
    {
        int idx = 0;
        Entry entry = new Entry(key, value, null);
        if(height == 0) {
            // external node
            for(idx=0; idx<node.m_nChildren; idx++)
                if(lt(key, node.m_vChildren[idx].key))
                    break;
        } else {
            // internal node
            for(idx=0; idx<node.m_nChildren; idx++) {
                if(idx+1==node.m_nChildren || lt(key, node.m_vChildren[idx+1].key)) {
                    Node unode = insert(node.m_vChildren[idx++].next, key, value, height-1);
                    if(unode == null)
                        return null;
                    entry.key = unode.m_vChildren[0].key;
                    entry.next = unode;
                    break;
                }
            }
        }

        for(int i=node.m_nChildren; i>idx; i--)
            node.m_vChildren[i] = node.m_vChildren[i-1];
        node.m_vChildren[idx] = entry;
        node.m_nChildren++;
        if(node.m_nChildren < M)
            return null;
        return split(node);
    }

    /**
     * split a full node
     * @param node
     * @return
     */
    private Node
    split(
        Node node
        )
    {
        Node t = new Node(M/2);
        node.m_nChildren = M/2;
        for(int i=0; i<M/2; i++)
            t.m_vChildren[i] = node.m_vChildren[M/2+i];
        return t;
    }

}


