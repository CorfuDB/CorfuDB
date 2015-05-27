package org.corfudb.runtime.collections;

public class BTree<K extends Comparable<K>, V> implements IBTree<K,V> {

    public static int DEFAULT_B = 4;

    private Node m_root;
    private int m_height;
    private int m_size;
    private int B;

    private static class Node<K extends Comparable<K>, V> {
        private int m_nChildren;
        private Entry<K, V>[] m_vChildren;
        private Node(int nChildren, int M) {
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
     * ctor
     * @param _B
     */
    public BTree(int _B) {
        if(_B <=0 || _B % 2 != 0)
            throw new RuntimeException("BTree implementation requires even-numbered B > 0.");
        B = _B;
        m_root = new Node(0, B);
        m_height = 0;
        m_size = 0;
    }

    /**
     * apply a clear operation
     */
    @Override
    public void clear() {
        m_root = new Node(0, B);
        m_size = 0;
        m_height = 0;
    }

    /**
     * apply a remove command by marking the
     * entry deleted (if found)
     * @param key
     * @return
     */
    @Override
    public V remove(K key) {
        Entry entry = searchEntry(m_root, key, m_height);
        if (entry != null) {
            V result = entry.deleted ? null : (V) entry.value;
            m_size -= entry.deleted ? 0 : 1;
            entry.deleted = true;
            return result;
        }
        return null;
    }


    /**
     * apply a get command
     * @param key
     * @return
     */
    @Override
    public V get(K key) {
        return search(m_root, key, m_height);
    }

    /**
     * apply a put command
     * @param key
     * @param value
     */
    @Override
    public void put(K key, V value) {
        Node unode = insert(m_root, key, value, m_height);
        m_size++;
        if (unode != null) {
            // split required
            Node t = new Node(2, B);
            t.m_vChildren[0] = new Entry(m_root.m_vChildren[0].key, null, m_root);
            t.m_vChildren[1] = new Entry(unode.m_vChildren[0].key, null, unode);
            m_root = t;
            m_height++;
        }
    }

    /**
     * apply an update command
     * @param key
     * @param value
     */
    @Override
    public boolean update(K key, V value) {
        Entry entry = searchEntry(m_root, key, m_height);
        if (entry != null && !entry.deleted) {
            entry.value = value;
            return true;
        }
        return false;
    }


    /**
     * return the size based on the most recent view
     * @return
     */
    @Override
    public int size() {
        return m_size;
    }

    /**
     * return the height based on the current view
     * @return
     */
    @Override
    public int height() {
        return m_height;
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
        if(node.m_nChildren < B)
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
        Node t = new Node(B /2, B);
        node.m_nChildren = B /2;
        for(int i=0; i< B /2; i++)
            t.m_vChildren[i] = node.m_vChildren[B /2+i];
        return t;
    }

}


