package org.corfudb.runtime.collections;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.smr.*;
import org.corfudb.runtime.smr.legacy.*;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.view.ICorfuDBInstance;
import org.corfudb.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class LPBTree<K extends Comparable<K>, V>
        extends AbstractLambdaBTree<K, V> {

    private static final Logger log = LoggerFactory.getLogger(LPBTree.class);

    public static final int DEFAULT_B = 4;
    public transient HashMap<UUID, LPBTEntry> m_entries;
    public transient HashMap<UUID, LPBTNode> m_nodes;
    transient ISMREngine<TreeContainer> smr;
    transient CorfuDBRuntime cdr;
    ITransaction tx;
    UUID streamID;

    public static boolean extremeDebug = false;

    private LPBTNode nodeById(UUID noid) {
        if (noid.compareTo(CorfuDBObject.oidnull) == 0)
            return null;
        LPBTNode n = m_nodes == null ? null : m_nodes.getOrDefault(noid, null);
        if (n == null) {
            if(cdr == null)
                log.error("WHAT THE FREAKING ERG?");
            ICorfuDBInstance instance = cdr.getLocalInstance();
            IStream stream = instance.openStream(noid);
            n = new LPBTNode(stream);
            m_nodes.put(noid, n);
        }
        return n;
    }

    private LPBTEntry entryById(UUID noid) {
        if (noid.compareTo(CorfuDBObject.oidnull) == 0)
            return null;
        LPBTEntry e = m_entries == null ? null : m_entries.getOrDefault(noid, null);
        if (e == null) {
            e = new LPBTEntry(cdr.getLocalInstance().openStream(noid));
            m_entries.put(noid, e);
        }
        return e;
    }


    public LPBTree(LPBTree<K,V> map, ITransaction tx) {
        this.streamID = map.streamID;
        this.tx = tx;
        if(tx != null)
            cdr = tx.getRuntime();
        m_nodes = new HashMap();
        m_entries = new HashMap();
    }

    @SuppressWarnings("unchecked")
    public LPBTree(IStream stream, Class<? extends ISMREngine> smrClass) {
        m_nodes = new HashMap();
        m_entries = new HashMap();
        try {
            cdr = stream.getRuntime();
            streamID = stream.getStreamID();
            smr = smrClass.getConstructor(IStream.class, Class.class).newInstance(stream, TreeContainer.class);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public LPBTree() {
        throw new RuntimeException("GIVE ME A FREAKING BREAK!");
    }


    public LPBTree(IStream stream)
    {
        m_nodes = new HashMap();
        m_entries = new HashMap();
        cdr = stream.getRuntime();
        streamID = stream.getStreamID();
        smr = new SimpleSMREngine<TreeContainer>(stream, TreeContainer.class);
    }

    @Override
    public LPBTree clone() {
        throw new RuntimeException("GIVE ME A BREAK!");
    }

    @Override
    public void init() {
        mutatorHelper((ISMREngineCommand<TreeContainer>) (tree, opts) -> {
            UUID nodeUUID = Utils.nextDeterministicUUID(getStreamID(), ++tree.m_idseed);
            IStream nodeStream = opts.getRuntime().getLocalInstance().openStream(nodeUUID);
            LPBTNode<K, V> e = new LPBTNode(nodeStream);
            e.writeChildCount(0);
            tree.m_root = e.getStreamID();
            tree.m_height = 0;
            tree.m_size = 0;
        });
    }


    /**
     * Get the type of the underlying object
     */
    @Override
    public Class<?> getUnderlyingType() {
        return TreeContainer.class;
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

    /**
     * print the current view (consistent or otherwise)
     * @return
     */
    public String printview() {
        return (String) accessorHelper((ISMREngineCommand<TreeContainer>) (tree, opts) -> {
            opts.getReturnResult().complete(printview(tree, nodeById(tree.m_root), tree.m_height, "") + "\n");
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
    printview(TreeContainer tree, LPBTNode<K, V> node, int height, String indent) {
        if(node == null) return "";
        StringBuilder sb = new StringBuilder();
        int nChildren = node.readChildCount();
        if(height == 0) {
            for(int i=0; i<nChildren; i++) {
                LPBTEntry child = entryById(node.getChild(i));
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
                    sb.append(entryById(node.getChild(i)).readKey());
                    sb.append(")\n");
                }
                LPBTEntry<K,V> echild = entryById(node.getChild(i));
                if(echild == null) {
                    sb.append("null-child-entry");
                } else {
                    LPBTNode next = nodeById(echild.readNext());
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

        return (String) accessorHelper((ISMREngineCommand<TreeContainer>) (tree, opts) -> {
            String result = print(tree, nodeById(tree.m_root), tree.m_height, "") + "\n";
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
    print(TreeContainer tree, LPBTNode<K, V> node, int height, String indent) {
        if(node == null) return "";
        StringBuilder sb = new StringBuilder();
        int nChildren = readchildcount(node);
        if(height == 0) {
            for(int i=0; i<nChildren; i++) {
                LPBTEntry child = entryById(readchild(node, i));
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
                LPBTNode next = nodeById(readnext(readchild(node, i)));
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
        return (V) accessorHelper((ISMREngineCommand<TreeContainer>) (tree, opts) -> {
            V result = null;
            if (key != null) {
                UUID root = readrootoid();
                int height = readheight();
                LPBTEntry entry = searchEntry(root, key, height);
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
        return (V) mutatorAccessorHelper((ISMREngineCommand<TreeContainer>) (tree, opts) -> {
            V result = null;
            if (key != null) {
                UUID root = readrootoid();
                int height = readheight();
                LPBTEntry entry = searchEntry(root, key, height);
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
        return (boolean) mutatorAccessorHelper((ISMREngineCommand<TreeContainer>) (tree, opts) -> {
            boolean result = false;
            if (key != null) {
                UUID root = readrootoid();
                int height = readheight();
                LPBTEntry entry = searchEntry(root, key, height);
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
        mutatorAccessorHelper((ISMREngineCommand<TreeContainer>) (tree, opts) -> {
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
        return (V) mutatorAccessorHelper((ISMREngineCommand<TreeContainer>) (tree, opts) -> {
            V result = null;
            UUID root = tree.m_root;
            int height = tree.m_height;
            int size = tree.m_size;
            LPBTEntry e = searchEntry(root, key, height);
            if(e != null) {
                if(!e.readDeleted())
                    result = (V)e.readValue();
                e.writeValue(value);
                e.writeDeleted(false);
            } else {
                UUID unodeoid = insert(opts, tree, root, key, value, height);
                writesize(size + 1);
                if (unodeoid != CorfuDBObject.oidnull) {
                    // split required
                    UUID tUUID = Utils.nextDeterministicUUID(getStreamID(), ++tree.m_idseed);
                    IStream tstream = opts.getRuntime().getLocalInstance().openStream(tUUID);
                    LPBTNode t = new LPBTNode(tstream);
                    t.writeChildCount(2);
                    UUID rootchild0 = readchild(nodeById(root), 0);
                    UUID uchild0 = readchild(nodeById(unodeoid), 0);
                    Comparable r0key = readkey(entryById(rootchild0));
                    Comparable u0key = readkey(entryById(uchild0));
                    UUID tc0UUID = Utils.nextDeterministicUUID(getStreamID(), ++tree.m_idseed);
                    UUID tc1UUID = Utils.nextDeterministicUUID(getStreamID(), ++tree.m_idseed);
                    IStream tc0Stream = opts.getRuntime().getLocalInstance().openStream(tc0UUID);
                    IStream tc1Stream = opts.getRuntime().getLocalInstance().openStream(tc1UUID);
                    LPBTEntry tc0 = new LPBTEntry(tc0Stream);
                    LPBTEntry tc1 = new LPBTEntry(tc1Stream);
                    tc0.writeKey((K) r0key);
                    tc1.writeKey((K) u0key);
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
        UUID oidnode,
        K key,
        int height
        )
    {
        LPBTNode<K,V> node = nodeById(oidnode);
        int nChildren = readchildcount(node);

        if(height == 0) {
            // external node
            for(int i=0; i<nChildren; i++) {
                UUID oidchild = readchild(node, i);
                LPBTEntry child = entryById(oidchild);
                Comparable ckey = readkey(child);
                if(eq(key, ckey))
                    return (V) readvalue(child);
            }
        } else {
            // internal node
            for(int i=0; i<nChildren; i++) {
                if(i+1 == nChildren || lt(key, readkey(readchild(node, i+1)))) {
                    return search(readnext(readchild(node, i)), key, height - 1);
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
    private LPBTEntry
    searchEntry(
        UUID oidnode,
        K key,
        int height
        ) {

        if(oidnode == CorfuDBObject.oidnull)
            return null;

        LPBTNode<K, V> node = nodeById(oidnode);
        int nChildren = readchildcount(node);

        if (height == 0) {
            // external node
            for (int i = 0; i < nChildren; i++) {
                UUID oidchild = readchild(node, i);
                LPBTEntry child = entryById(oidchild);
                Comparable ckey = readkey(child);
                if (eq(key, ckey))
                    return child;
            }
        } else {
            // internal node
            for (int i = 0; i < nChildren; i++) {
                if (i + 1 == nChildren || lt(key, readkey(readchild(node, i + 1)))) {
                    return searchEntry(readnext(readchild(node, i)), key, height - 1);
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
        ISMREngine.ISMREngineOptions opts,
        TreeContainer tree,
        UUID oidnode,
        K key,
        V value,
        int height
        )
    {
        int idx = 0;
        LPBTNode<K,V> node = nodeById(oidnode);
        int nChildren = readchildcount(node);
        UUID eUUID = Utils.nextDeterministicUUID(getStreamID(), ++tree.m_idseed);
        IStream eStream = opts.getRuntime().getLocalInstance().openStream(eUUID);
        LPBTEntry entry = new LPBTEntry(eStream);
        entry.writeKey(key);
        entry.writeValue(value);

        if(height == 0) {
            for(idx=0; idx<nChildren; idx++)
                if(lt(key, readkey(readchild(node, idx))))
                    break;
        } else {
            // internal node
            for(idx=0; idx<nChildren; idx++) {
                if(idx+1==nChildren || lt(key, readkey(readchild(node, idx+1)))) {
                    UUID oidunode = insert(opts, tree, readnext(readchild(node, idx++)), key, value, height-1);
                    if(oidunode == CorfuDBObject.oidnull)
                        return CorfuDBObject.oidnull;
                    LPBTNode<K, V> unode = nodeById(oidunode);
                    UUID uchild0 = readchild(unode, 0);
                    LPBTEntry<K, V> uentry0 = entryById(uchild0);
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
        return split(opts, tree, node);
    }

    /**
     * split a full node
     * @param node
     * @return
     */
    private UUID
    split(
        ISMREngine.ISMREngineOptions opts,
        TreeContainer tree,
        LPBTNode node
        )
    {
        int B = tree.B;
        UUID tUUID = Utils.nextDeterministicUUID(getStreamID(), ++tree.m_idseed);
        IStream tStream = opts.getRuntime().getLocalInstance().openStream(tUUID);
        LPBTNode t = new LPBTNode(tStream);
        t.writeChildCount(B / 2);
        writechildcount(node, B / 2);
        for(int i=0; i<B/2; i++)
            writechild(t, i, readchild(node, B/2+i));
        return t.getStreamID();
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
    protected LPBTNode<K, V>
    readroot() {
        return (LPBTNode<K, V>) accessorHelper((ISMREngineCommand<TreeContainer>) (tree, opts) -> {
            opts.getReturnResult().complete(nodeById(tree.m_root));
        });
    }

    /**
     * return the size based on the most recent view
     * @return
     */
    public int readsize() {
        return (int) accessorHelper((ISMREngineCommand<TreeContainer>) (tree, opts) -> {
            opts.getReturnResult().complete(tree.m_size);
        });
    }

    /**
     * return the height based on the current view
     * @return
     */
    public int readheight() {
        return (int) accessorHelper((ISMREngineCommand<TreeContainer>) (tree, opts) -> {
            opts.getReturnResult().complete(tree.m_height);
        });
    }

    /**
     * return the size based on the most recent view
     * @return
     */
    public void writesize(int size) {
        mutatorHelper((ISMREngineCommand<TreeContainer>) (tree, opts) -> {
            tree.m_size = size;
        });
    }

    /**
     * return the height based on the current view
     * @return
     */
    public void writeheight(int height) {
        mutatorHelper((ISMREngineCommand<TreeContainer>) (tree, opts) -> {
            tree.m_height = height;
        });
    }

    /**
     * return the size based on the most recent view
     * @return
     */
    public UUID readrootoid() {
        return (UUID) accessorHelper((ISMREngineCommand<TreeContainer>) (tree, opts) -> {
            opts.getReturnResult().complete(tree.m_root);
        });
    }

    public void writeroot(UUID _oid) {
        mutatorHelper((ISMREngineCommand<TreeContainer>) (tree, opts) -> {
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
        LPBTNode<K,V> node,
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
        LPBTNode<K,V> node,
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
        LPBTEntry<K, V> entry,
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
        LPBTEntry<K, V> entry,
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
        LPBTEntry<K, V> entry,
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
        LPBTEntry<K, V> entry,
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
        LPBTEntry<K, V> entry
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
        LPBTEntry<K, V> entry
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
        return (Comparable) accessorHelper((ISMREngineCommand<TreeContainer>) (tree, opts) -> {
            LPBTEntry<K, V> entry = entryById(entryoid);
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
        LPBTEntry<K, V> entry
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
        LPBTEntry<K, V> entry
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
        return (UUID) accessorHelper((ISMREngineCommand<TreeContainer>) (tree, opts) -> {
            LPBTEntry<K, V> entry = entryById(entryoid);
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
        LPBTNode<K,V> node,
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
        LPBTNode<K,V> node
        ) {
        return node.readChildCount();
    }

    /**
     * Gets a transactional context for this object.
     * @return              A transactional context to be used during a transaction.
     */
    @Override
    public LPBTree<K,V> getTransactionalContext(ITransaction tx) {
        tx.registerStream(getSMREngine().getStreamID());
        return new LPBTree<K, V>(this, tx);
    }

}


