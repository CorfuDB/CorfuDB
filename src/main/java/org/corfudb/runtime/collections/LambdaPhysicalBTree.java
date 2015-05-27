package org.corfudb.runtime.collections;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.smr.*;
import org.corfudb.runtime.smr.legacy.*;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.ITimestamp;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LambdaPhysicalBTree<K extends Comparable<K>, V>
        implements ICorfuDBObject<LambdaPhysicalBTree<K,V>>,
        IBTree<K,V> {

    public static final int DEFAULT_B = 4;
    protected static class BTreeRoot {
        UUID m_root;
        int m_size;
        int m_height;
        int B;
        HashMap<UUID, Entry> m_entries;
        HashMap<UUID, Node> m_nodes;
        public BTreeRoot(UUID root) {
            m_root = root;
            m_size = 0;
            m_height = 0;
            B = DEFAULT_B;
            m_entries = new HashMap();
            m_nodes = new HashMap();
        }
        Node nodeById(UUID noid) {
            return m_nodes.getOrDefault(noid, null);
        }
        Entry entryById(UUID noid) {
            return m_entries.getOrDefault(noid, null);
        }
    }

    transient ISMREngine<BTreeRoot> smr;
    transient CorfuDBRuntime cdr;
    ITransaction tx;
    UUID streamID;

    public static boolean extremeDebug = false;

//    public LambdaPhysicalBTree(LambdaPhysicalBTree<K,V> map, ITransaction tx)
//    {
//        this.streamID = map.streamID;
//        this.tx = tx;
//    }

    @SuppressWarnings("unchecked")
    public LambdaPhysicalBTree(IStream stream, Class<? extends ISMREngine> smrClass)
    {
        try {
            cdr = stream.getRuntime();
            streamID = stream.getStreamID();
            smr = smrClass.getConstructor(IStream.class, Class.class).newInstance(stream, BTreeRoot.class);
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public LambdaPhysicalBTree(IStream stream)
    {
        cdr = stream.getRuntime();
        streamID = stream.getStreamID();
        smr = new SimpleSMREngine<BTreeRoot>(stream, BTreeRoot.class);
    }

    /**
     * Get the type of the underlying object
     */
    @Override
    public Class<?> getUnderlyingType() {
        return BTreeRoot.class;
    }

    /**
     * Get the UUID of the underlying stream
     */
    @Override
    public UUID getStreamID() {
        return streamID;
    }

    /**
     * Get underlying SMR engine
     *
     * @return The SMR engine this object was instantiated under.
     */
    @Override
    public ISMREngine getUnderlyingSMREngine() {
        return smr;
    }

    /**
     * Set underlying SMR engine
     *
     * @param engine
     */
    @Override
    @SuppressWarnings("unchecked")
    public void setUnderlyingSMREngine(ISMREngine engine) {
        this.smr = engine;
    }

    /**
     * Get the underlying transaction
     *
     * @return The transaction this object is currently participating in.
     */
    @Override
    public ITransaction getUnderlyingTransaction() {
        return tx;
    }

    /**
     * console logging for verbose mode.
     * @param strFormat
     * @param args
     */
    protected static void
    inform(
            String strFormat,
            Object... args
        )
    {
        if(extremeDebug)
            System.out.format(strFormat, args);
    }

    private IStream newStream() {
        UUID newID = UUID.randomUUID();
        return cdr.getLocalInstance().openStream(newID);
    }

    private static class _Node<K extends Comparable<K>, V> {
        int m_nChildren;
        UUID[] m_vChildren;
        _Node() {
            m_nChildren = 0;
            m_vChildren = new UUID[DEFAULT_B];
        }
        _Node(int nChildren, int nArity) {
            m_nChildren = nChildren;
            m_vChildren = new UUID[nArity];
        }
    }

    private static class Node<K extends Comparable<K>, V> implements ICorfuDBObject<Node<K,V>> {

        transient ISMREngine<_Node> smr;
        ITransaction tx;
        UUID streamID;
        public Node(Node<K,V> map, ITransaction tx) {
            this.streamID = map.streamID;
            this.tx = tx;
        }

        @SuppressWarnings("unchecked")
        public Node(IStream stream, Class<? extends ISMREngine> smrClass, int nChildren, int nArity) {
            try {
                streamID = stream.getStreamID();
                Object[] args = new Object[2];
                args[0] = new Integer(nChildren);
                args[1] = new Integer(nArity);
                smr = smrClass.getConstructor(IStream.class, Class.class).newInstance(stream, _Node.class, args);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public Node(IStream stream, int nChildren, int nArity) {
            streamID = stream.getStreamID();
            Object[] args = new Object[2];
            args[0] = new Integer(nChildren);
            args[1] = new Integer(nArity);
            smr = new SimpleSMREngine<_Node>(stream, _Node.class, args);
        }

        @Override
        public Class<?> getUnderlyingType() { return _Node.class; }
        @Override
        public UUID getStreamID() { return streamID; }
        @Override
        public ISMREngine getUnderlyingSMREngine() {  return smr; }
        @Override
        @SuppressWarnings("unchecked")
        public void setUnderlyingSMREngine(ISMREngine engine) { this.smr = engine; }
        @Override
        public ITransaction getUnderlyingTransaction() { return tx; }


        /**
         * read the child count
         * @return the number of children in the given node
         */
        public int
        readChildCount() {
            return (int) accessorHelper((ISMREngineCommand<_Node>) (node, opts) -> {
                opts.getReturnResult().complete(node.m_nChildren);
            });
        }

        /**
         * write the child count
         * @param n
         */
        public void
        writeChildCount(int n) {
            mutatorHelper((ISMREngineCommand<_Node>) (node, opts) -> {
                node.m_nChildren = n;
            });
        }

        /**
         * getChild
         * @param index
         * @return
         */
        protected UUID getChild(int index) {
            return (UUID) accessorHelper((ISMREngineCommand<_Node>) (node, opts) -> {
                UUID result = CorfuDBObject.oidnull;
                if (index >= 0 && index < node.m_nChildren)
                    result = node.m_vChildren[index];
                opts.getReturnResult().complete(result);
            });
        }

        /**
         * apply an indexed read child operation
         * @param index
         * @return
         */
        public UUID
        readChild(int index) {
            return getChild(index);
        }

        /**
         * apply a write child operation
         * @param n
         * @param _oid
         */
        public void
        writeChild(int n, UUID _oid) {
            mutatorHelper((ISMREngineCommand<_Node>) (node, opts) -> {
                if(n>=0 && n<node.m_vChildren.length)
                    node.m_vChildren[n] = _oid;
            });
        }

        /**
         * toString
         * @return
         */
        @Override
        public String toString() {
            return (String) accessorHelper((ISMREngineCommand<_Node>) (node, opts) -> {
                StringBuilder sb = new StringBuilder();
                sb.append("N");
                sb.append(streamID);
                boolean first = true;
                for (int i = 0; i < node.m_nChildren; i++) {
                    boolean last = i == node.m_nChildren - 1;
                    if (first) {
                        sb.append("[");
                    } else {
                        sb.append(", ");
                    }
                    sb.append("c");
                    sb.append(i);
                    sb.append("=");
                    sb.append(getChild(i));
                    if (last) sb.append("]");
                    first = false;
                }
                opts.getReturnResult().complete(sb.toString());
            });
        }

    }



    private static class _Entry<K extends Comparable<K>, V> {

        private Comparable key;
        private V value;
        private UUID oidnext;
        private boolean deleted;
        public _Entry(K _key, V _value, UUID _next) {
            key = _key;
            value = _value;
            oidnext = _next;
        }
    }

    private static class Entry<K extends Comparable<K>, V> implements ICorfuDBObject<Entry<K,V>> {


        transient ISMREngine<_Entry> smr;
        ITransaction tx;
        UUID streamID;

        public Entry(Entry<K, V> entry, ITransaction tx) {
            this.streamID = entry.streamID;
            this.tx = tx;
        }

        @SuppressWarnings("unchecked")
        public Entry(IStream stream, Class<? extends ISMREngine> smrClass, K _key, V _value, UUID _next) {
            try {
                streamID = stream.getStreamID();
                Object[] args = new Object[3];
                args[0] = _key;
                args[1] = _value;
                args[2] = _next;
                smr = smrClass.getConstructor(IStream.class, Class.class).newInstance(stream, _Entry.class, args);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public Entry(IStream stream, K _key, V _value, UUID _next) {
            streamID = stream.getStreamID();
            Object[] args = new Object[3];
            args[0] = _key;
            args[1] = _value;
            args[2] = _next;
            smr = new SimpleSMREngine<_Entry>(stream, _Entry.class, args);
        }

        @Override
        public Class<?> getUnderlyingType() {
            return _Entry.class;
        }

        @Override
        public UUID getStreamID() {
            return streamID;
        }

        @Override
        public ISMREngine getUnderlyingSMREngine() {
            return smr;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void setUnderlyingSMREngine(ISMREngine engine) {
            this.smr = engine;
        }

        @Override
        public ITransaction getUnderlyingTransaction() {
            return tx;
        }

        /**
         * read the deleted flag on this entry
         *
         * @return
         */
        public Boolean readDeleted() {
            return (Boolean) accessorHelper((ISMREngineCommand<_Entry>) (entry, opts) -> {
                opts.getReturnResult().complete(entry.deleted);
            });
        }

        /**
         * apply a delete command
         *
         * @param b
         * @return
         */
        public Boolean writeDeleted(boolean b) {
            return (Boolean) mutatorAccessorHelper((ISMREngineCommand<_Entry>) (entry, opts) -> {
                entry.deleted = b;
                opts.getReturnResult().complete(b);
            });
        }

        /**
         * read the key
         *
         * @return
         */
        public K readKey() {
            return (K) accessorHelper((ISMREngineCommand<_Entry>) (entry, opts) -> {
                opts.getReturnResult().complete(entry.key);
            });
        }

        /**
         * write the key
         *
         * @param k
         * @return
         */
        public K writeKey(K k) {
            return (K) mutatorAccessorHelper((ISMREngineCommand<_Entry>) (entry, opts) -> {
                entry.key = k;
                opts.getReturnResult().complete(k);
            });
        }

        /**
         * read the value
         *
         * @return
         */
        public V readValue() {
            return (V) accessorHelper((ISMREngineCommand<_Entry>) (entry, opts) -> {
                opts.getReturnResult().complete(entry.value);
            });
        }

        /**
         * write the value
         *
         * @param v
         * @return
         */
        public V writeValue(V v) {
            return (V) mutatorAccessorHelper((ISMREngineCommand<_Entry>) (entry, opts) -> {
                entry.value = v;
                opts.getReturnResult().complete(v);
            });
        }

        /**
         * read the next pointer
         *
         * @return
         */
        public UUID readNext() {
            return (UUID) accessorHelper((ISMREngineCommand<_Entry>) (entry, opts) -> {
                opts.getReturnResult().complete(entry.oidnext);
            });
        }

        /**
         * write the next pointer
         *
         * @param _next
         * @return
         */
        public UUID writeNext(UUID _next) {
            return (UUID) mutatorAccessorHelper((ISMREngineCommand<_Entry>) (entry, opts) -> {
                entry.oidnext = _next;
                opts.getReturnResult().complete(_next);
            });
        }

        /**
         * toString
         *
         * @return
         */
        @Override
        public String toString() {
            return (String) accessorHelper((ISMREngineCommand<_Entry>) (entry, opts) -> {
                StringBuilder sb = new StringBuilder();
                if (entry.deleted)
                    sb.append("DEL: ");
                sb.append("E");
                sb.append(streamID);
                sb.append(":[k=");
                sb.append(entry.key);
                sb.append(", v=");
                sb.append(entry.value);
                sb.append(", n=");
                sb.append(entry.oidnext);
                sb.append("]");
                opts.getReturnResult().complete(sb.toString());
            });
        }

    }

    /**
     * print the current view (consistent or otherwise)
     * @return
     */
    public String printview() {
        return (String) accessorHelper((ISMREngineCommand<BTreeRoot>) (tree, opts) -> {
            opts.getReturnResult().complete(printview(tree, tree.nodeById(tree.m_root), tree.m_height, "") + "\n");
        });
    }

    /**
     * printview helper function
     * @param node
     * @param height
     * @param indent
     * @return
     */
    private String
    printview(BTreeRoot tree, Node<K, V> node, int height, String indent) {
        if(node == null) return "";
        StringBuilder sb = new StringBuilder();
        int nChildren = node.readChildCount();
        if(height == 0) {
            for(int i=0; i<nChildren; i++) {
                Entry child = tree.entryById(node.getChild(i));
                if(child == null) {
                    sb.append("OIDNULL");
                } else {
                    if(child.readDeleted())
                        sb.append("DEL: ");
                    sb.append(indent);
                    sb.append(child.readKey());
                    sb.append(" ");
                    sb.append(child.readValue());
                    sb.append("\n");
                }
            }
        } else {
            for(int i=0; i<nChildren; i++) {
                if(i>0) {
                    sb.append(indent);
                    sb.append("(");
                    sb.append(tree.entryById(node.getChild(i)).readKey());
                    sb.append(")\n");
                }
                Entry<K,V> echild = tree.entryById(node.getChild(i));
                if(echild == null) {
                    sb.append("null-child-entry");
                } else {
                    Node next = tree.nodeById(echild.readNext());
                    if (next == null) {
                        sb.append("null-child-next");
                    } else {
                        sb.append(printview(tree, next, height - 1, indent + "    "));
                    }
                }
            }
        }
        return sb.toString();
    }

    /**
     * print the b-tree
     * @return
     */
    public String print() {

        return (String) accessorHelper((ISMREngineCommand<BTreeRoot>) (tree, opts) -> {
            String result = print(tree, tree.nodeById(tree.m_root), tree.m_height, "") + "\n";
            opts.getReturnResult().complete(result);
        });
    }

    /**
     * print helper function
     * @param node
     * @param height
     * @param indent
     * @return
     */
    private String
    print(BTreeRoot tree, Node<K, V> node, int height, String indent) {
        if(node == null) return "";
        StringBuilder sb = new StringBuilder();
        int nChildren = readchildcount(node);
        if(height == 0) {
            for(int i=0; i<nChildren; i++) {
                Entry child = tree.entryById(readchild(node, i));
                boolean deleted = readdeleted(child);
                if(deleted)
                    sb.append("DEL: ");
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
                    sb.append(readkey(readchild(node, i)));
                    sb.append(")\n");
                }
                Node next = tree.nodeById(readnext(readchild(node, i)));
                sb.append(print(tree, next, height - 1, indent + "    "));
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
        return (V) accessorHelper((ISMREngineCommand<BTreeRoot>) (tree, opts) -> {
            V result = null;
            if (key != null) {
                UUID root = readrootoid();
                int height = readheight();
                Entry entry = searchEntry(tree, root, key, height);
                if (entry != null) {
                    boolean deleted = readdeleted(entry);
                    if (!deleted) {
                        result = (V) readvalue(entry);
                    }
                }
            }
            opts.getReturnResult().complete(result);
        });
    }

    /**
     *
     * @param key
     * @return
     */
    public V remove(K key) {
        return (V) mutatorAccessorHelper((ISMREngineCommand<BTreeRoot>) (tree, opts) -> {
            V result = null;
            if (key != null) {
                UUID root = readrootoid();
                int height = readheight();
                Entry entry = searchEntry(tree, root, key, height);
                if (entry != null) {
                    boolean deleted = readdeleted(entry);
                    if (!deleted) {
                        result = (V) readvalue(entry);
                        writedeleted(entry, true);
                        int size = readsize();
                        writesize(size - 1);
                    }
                }
                opts.getReturnResult().complete(result);
            }
        });
    }

    /**
     * update the value at the given key
     * @param key
     * @param value
     * @return
     */
    public boolean update(K key, V value) {
        return (boolean) mutatorAccessorHelper((ISMREngineCommand<BTreeRoot>) (tree, opts) -> {
            boolean result = false;
            if (key != null) {
                UUID root = readrootoid();
                int height = readheight();
                Entry entry = searchEntry(tree, root, key, height);
                if (entry != null) {
                    boolean deleted = readdeleted(entry);
                    if (!deleted) {
                        V oval = (V) readvalue(entry);
                        writevalue(entry, value);
                        result = true;
                    }
                }
                opts.getReturnResult().complete(result);
            }
        });
    }


    /**
     * clear the tree
     */
    public void clear() {
        mutatorHelper((ISMREngineCommand<BTreeRoot>) (tree, opts) -> {
            UUID root = readrootoid();
            writeroot(CorfuDBObject.oidnull);
            writesize(0);
            writeheight(0);
        });
    }


    /**
     * maps. was their rocking-ness mentioned?
     * @param key
     * @param value
     */
    public V
    put(K key, V value) {
        return (V) mutatorAccessorHelper((ISMREngineCommand<BTreeRoot>) (tree, opts) -> {
            V result = null;
            UUID root = readrootoid();
            int height = readheight();
            int size = readsize();
            Entry e = searchEntry(tree, root, key, height);
            if(e != null) {
                result = (V)e.readValue();
                e.writeValue(value);
            } else {
                UUID unodeoid = insert(tree, root, key, value, height);
                writesize(size + 1);
                if (unodeoid != CorfuDBObject.oidnull) {
                    // split required
                    Node t = allocNode(2);
                    UUID rootchild0 = readchild(tree.nodeById(root), 0);
                    UUID uchild0 = readchild(tree.nodeById(unodeoid), 0);
                    Comparable r0key = readkey(tree.entryById(rootchild0));
                    Comparable u0key = readkey(tree.entryById(uchild0));
                    Entry tc0 = allocEntry((K) r0key, null);
                    Entry tc1 = allocEntry((K) u0key, null);
                    writechild(t, 0, tc0.getStreamID());
                    writechild(t, 1, tc1.getStreamID());
                    writenext(tc0, root);
                    writenext(tc1, unodeoid);
                    writeroot(t.getStreamID());
                    writeheight(height + 1);
                }
            }
            opts.getReturnResult().complete(result);
        });
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
        BTreeRoot tree,
        UUID oidnode,
        K key,
        int height
        )
    {
        Node<K,V> node = tree.nodeById(oidnode);
        int nChildren = readchildcount(node);

        if(height == 0) {
            // external node
            for(int i=0; i<nChildren; i++) {
                UUID oidchild = readchild(node, i);
                Entry child = tree.entryById(oidchild);
                Comparable ckey = readkey(child);
                if(eq(key, ckey))
                    return (V) readvalue(child);
            }
        } else {
            // internal node
            for(int i=0; i<nChildren; i++) {
                if(i+1 == nChildren || lt(key, readkey(readchild(node, i+1)))) {
                    return search(tree, readnext(readchild(node, i)), key, height - 1);
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
        BTreeRoot tree,
        UUID oidnode,
        K key,
        int height
        ) {

        if(oidnode == CorfuDBObject.oidnull)
            return null;

        Node<K, V> node = tree.nodeById(oidnode);
        int nChildren = readchildcount(node);

        if (height == 0) {
            // external node
            for (int i = 0; i < nChildren; i++) {
                UUID oidchild = readchild(node, i);
                Entry child = tree.entryById(oidchild);
                Comparable ckey = readkey(child);
                if (eq(key, ckey))
                    return child;
            }
        } else {
            // internal node
            for (int i = 0; i < nChildren; i++) {
                if (i + 1 == nChildren || lt(key, readkey(readchild(node, i + 1)))) {
                    return searchEntry(tree, readnext(readchild(node, i)), key, height - 1);
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
    private UUID
    insert(
        BTreeRoot tree,
        UUID oidnode,
        K key,
        V value,
        int height
        )
    {
        int idx = 0;
        Node<K,V> node = tree.nodeById(oidnode);
        int nChildren = readchildcount(node);
        Entry entry = allocEntry(key, value);

        if(height == 0) {
            for(idx=0; idx<nChildren; idx++)
                if(lt(key, readkey(readchild(node, idx))))
                    break;
        } else {
            // internal node
            for(idx=0; idx<nChildren; idx++) {
                if(idx+1==nChildren || lt(key, readkey(readchild(node, idx+1)))) {
                    UUID oidunode = insert(tree, readnext(readchild(node, idx++)), key, value, height-1);
                    if(oidunode == CorfuDBObject.oidnull)
                        return CorfuDBObject.oidnull;
                    Node<K, V> unode = tree.nodeById(oidunode);
                    UUID uchild0 = readchild(unode, 0);
                    Entry<K, V> uentry0 = tree.entryById(uchild0);
                    Comparable ukey = readkey(uentry0);
                    writekey(entry, ukey);
                    writenext(entry, oidunode);
                    break;
                }
            }
        }

        for(int i=nChildren; i>idx; i--)
            writechild(node, i, readchild(node, i-1));
        writechild(node, idx, entry.getStreamID());
        writechildcount(node, nChildren+1);
        if(nChildren+1 < tree.B)
            return CorfuDBObject.oidnull;
        return split(tree, node);
    }

    /**
     * split a full node
     * @param node
     * @return
     */
    private UUID
    split(
        BTreeRoot tree,
        Node node
        )
    {
        int B = tree.B;
        Node t = allocNode(B/2);
        writechildcount(node, B / 2);
        for(int i=0; i<B/2; i++)
            writechild(t, i, readchild(node, B/2+i));
        return t.getStreamID();
    }

    /**
     * allocate a new node.
     * @param nChildren
     * @return
     */
    private Node<K, V>
    allocNode(int nChildren) {
        return (Node<K, V>) mutatorAccessorHelper((ISMREngineCommand<BTreeRoot>) (tree, opts) -> {
            Node<K, V> newnode = new Node<K, V>(newStream(), nChildren, tree.B);
            tree.m_nodes.put(newnode.getStreamID(), newnode);
            newnode.writeChildCount(nChildren);
            for (int i = 0; i < tree.B; i++)
                newnode.writeChild(i, CorfuDBObject.oidnull);
            opts.getReturnResult().complete(newnode);
        });
    }

    /**
     * allocate a new entry
     * @param k
     * @param v
     * @return
     */
    private Entry<K, V>
    allocEntry(K k, V v) {
        return (Entry<K, V>) mutatorAccessorHelper((ISMREngineCommand<BTreeRoot>) (tree, opts) -> {
            Entry<K, V> newentry = new Entry<K, V>(newStream(), k, v, CorfuDBObject.oidnull);
            tree.m_entries.put(newentry.getStreamID(), newentry);
            UUID noid = newentry.getStreamID();
            opts.getReturnResult().complete(newentry);
        });
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
        return (Node<K, V>) accessorHelper((ISMREngineCommand<BTreeRoot>) (tree, opts) -> {
            opts.getReturnResult().complete(tree.nodeById(tree.m_root));
        });
    }

    /**
     * return the size based on the most recent view
     * @return
     */
    public int readsize() {
        return (int) accessorHelper((ISMREngineCommand<BTreeRoot>) (tree, opts) -> {
            opts.getReturnResult().complete(tree.m_size);
        });
    }

    /**
     * return the height based on the current view
     * @return
     */
    public int readheight() {
        return (int) accessorHelper((ISMREngineCommand<BTreeRoot>) (tree, opts) -> {
            opts.getReturnResult().complete(tree.m_height);
        });
    }

    /**
     * return the size based on the most recent view
     * @return
     */
    public void writesize(int size) {
        mutatorHelper((ISMREngineCommand<BTreeRoot>) (tree, opts) -> {
            tree.m_size = size;
        });
    }

    /**
     * return the height based on the current view
     * @return
     */
    public void writeheight(int height) {
        mutatorHelper((ISMREngineCommand<BTreeRoot>) (tree, opts) -> {
            tree.m_height = height;
        });
    }

    /**
     * return the size based on the most recent view
     * @return
     */
    public UUID readrootoid() {
        return (UUID) accessorHelper((ISMREngineCommand<BTreeRoot>) (tree, opts) -> {
            opts.getReturnResult().complete(tree.m_root);
        });
    }

    public void writeroot(UUID _oid) {
        mutatorHelper((ISMREngineCommand<BTreeRoot>) (tree, opts) -> {
            tree.m_root = _oid;
        });
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
        UUID oidchild
        )
    {
        node.writeChild(index, oidchild);
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
        ) {
        node.writeChildCount(count);
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
        ) {
        entry.writeKey((K) ckey);
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
        ) {
        entry.writeValue(value);
    }

    /**
     * write the entry's next pointer
     * @param entry
     * @param next
     */
    private void
    writenext(
        Entry<K, V> entry,
        UUID next
        ) {
        entry.writeNext(next);
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
        entry.writeDeleted(deleted);
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
        return entry.readKey();
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
        return entry.readDeleted();
    }

    /**
     * read the key of the given entry
     * @param entryoid
     * @return
     */
    private Comparable
    readkey(
        UUID entryoid
        )
    {
        return (Comparable) accessorHelper((ISMREngineCommand<BTreeRoot>) (tree, opts) -> {
            Entry<K, V> entry = tree.entryById(entryoid);
            opts.getReturnResult().complete(entry.readKey());
        });
    }

    /**
     * read the value field of the given node
     * @param entry
     * @return
     */
    private V
    readvalue(
        Entry<K, V> entry
        ) {
        return entry.readValue();
    }

    /**
     * read the next pointer of the entry
     * @param entry
     * @return
     */
    private UUID
    readnext(
        Entry<K, V> entry
        )
    {
        return entry.readNext();
    }

    /**
     * read the next pointer of the entry
     * @param entryoid
     * @return
     */
    private UUID
    readnext(
            UUID entryoid
        ) {
        return (UUID) accessorHelper((ISMREngineCommand<BTreeRoot>) (tree, opts) -> {
            Entry<K, V> entry = tree.entryById(entryoid);
            opts.getReturnResult().complete(entry.readNext());
        });
    }


    /**
     * get the children array for the given node
     * @param node
     * @return
     */
    private UUID
    readchild(
        Node<K,V> node,
        int idx
        ) {
        return node.readChild(idx);
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
        ) {
        return node.readChildCount();
    }


}


