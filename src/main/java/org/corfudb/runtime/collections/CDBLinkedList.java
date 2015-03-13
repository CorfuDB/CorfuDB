package org.corfudb.runtime.collections;

import org.corfudb.runtime.AbstractRuntime;
import org.corfudb.runtime.DirectoryService;
import org.corfudb.runtime.StreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.function.UnaryOperator;

/**
 *
 */
public class CDBLinkedList<E> extends CDBAbstractList<E> {

    static Logger dbglog = LoggerFactory.getLogger(CDBLinkedList.class);
    static protected final HashMap<Long, CDBLinkedList> s_lists = new HashMap<>();

    static public CDBLinkedList findList(long loid) {
        synchronized (s_lists) {
            if (s_lists.containsKey(loid))
                return s_lists.get(loid);
            return null;
        }
    }

    public long m_head;
    public HashMap<Long, CDBLinkedListNode<E>> m_nodes;
    public StreamFactory sf;
    public long oid;

    public void applyToObject(Object bs, long timestamp) {

        dbglog.debug("CDBLinkedList received upcall");
        NodeOp<E> cc = (NodeOp<E>) bs;
        switch (cc.cmd()) {
            case NodeOp.CMD_READ_HEAD: applyReadHead(cc); break;
            case NodeOp.CMD_WRITE_HEAD: applyWriteHead(cc); break;
            case NodeOp.CMD_WRITE_TAIL: throw new RuntimeException("invalid operation (write tail on singly linked list)");
            case NodeOp.CMD_READ_TAIL: throw new RuntimeException("invalid operation (read tail on singly linked list)");
        }
    }

    protected long applyReadHead(NodeOp<E> cc) {
        rlock();
        try {
            cc.setReturnValue(m_head);
        } finally {
            runlock();
        }
        return (long) cc.getReturnValue();
    }

    protected void applyWriteHead(NodeOp<E> cc) {
        wlock();
        try {
            m_head = cc.oidparam();
        } finally {
            wunlock();
        }
    }

    public CDBLinkedList(AbstractRuntime tTR, StreamFactory tsf, long toid) {
        super(tTR, tsf, toid);
        m_head = oidnull;
        sf = tsf;
        m_nodes = new HashMap<>();
        synchronized (s_lists) {
            assert(!s_lists.containsKey(oid));
            s_lists.put(oid, this);
        }
    }

    protected CDBLinkedListNode<E> nodeById_nolock(long noid) {
        assert(lockheld());
        return m_nodes.getOrDefault(noid, null);
    }

    protected CDBLinkedListNode<E> nodeById(long noid) {
        rlock();
        try {
            return m_nodes.getOrDefault(noid, null);
        } finally {
            runlock();
        }
    }

    protected long readhead() {
        NodeOp<E> cmd = new NodeOp<>(NodeOp.CMD_READ_HEAD, oid, oid);
        if (TR.query_helper(this, null, cmd))
            return (long) cmd.getReturnValue();
        rlock();
        try {
            return m_head;
        } finally {
            runlock();
        }
    }

    protected long readnext(CDBLinkedListNode<E> node) {

        NodeOp<E> cmd = new NodeOp<>(NodeOp.CMD_READ_NEXT, node.oid, node.oid);
        if (TR.query_helper(node, null, cmd))
            return (long) cmd.getReturnValue();
        node.rlock();
        try {
            return node.oidnext;
        } finally {
            node.runlock();
        }
    }

    protected E readvalue(CDBLinkedListNode<E> node) {

        NodeOp<E> cmd = new NodeOp<>(NodeOp.CMD_READ_VALUE, node.oid, node.oid);
        if (TR.query_helper(node, null, cmd))
            return (E) cmd.getReturnValue();
        node.rlock();
        try {
            return node.value;
        } finally {
            node.runlock();
        }
    }

    @Override
    public int size() {

        int size = 0;
        long nodeoid = readhead();
        while(nodeoid != oidnull) {
            size++;
            nodeoid = readnext(nodeById(nodeoid));
        }
        return size;
    }

    @Override
    public int indexOf(Object o) {

        E value;
        int index = 0;
        if(!isTypeE(o)) return -1;
        long oidnode = readhead();

        while(oidnode != oidnull) {
            CDBLinkedListNode<E> node = nodeById(oidnode);
            value = readvalue(node);
            if(value.equals(o))
                return index;
            oidnode = readnext(node);
            index++;
        }
        return -1;
    }

    @Override
    public int sizeview() {

        rlock();
        try {
            int size = 0;
            long oidnode = m_head;
            while (oidnode != oidnull) {
                size++;
                CDBLinkedListNode<E> node = nodeById_nolock(oidnode);
                assert(node != null);
                node.rlock();
                try {
                    oidnode = node.oidnext;
                } finally {
                    node.runlock();
                }
            }
            return size;
        } finally {
            runlock();
        }
    }

    @Override
    public int lastIndexOf(Object o) {

        if(!isTypeE(o)) return -1;
        int size = size();
        int index = 0;
        int lastIndex = -1;

        E value;
        long oidnode = readhead();
        while(oidnode != oidnull) {
            CDBLinkedListNode<E> node = nodeById(oidnode);
            value = readvalue(node);
            if(value.equals(o))
                lastIndex = index;
            oidnode = readnext(node);
            index++;
        }
        return lastIndex;
    }

    @Override
    public boolean isEmpty() {
        long head = readhead();
        return head == oidnull;
    }

    @Override
    public boolean contains(Object o) {
        return indexOf(o) != -1;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if(!contains(o)) return false;
        }
        return true;
    }

    @Override
    public E get(int index) {

        int cindex=0;
        long nodeoid = readhead();
        while(nodeoid != oidnull) {
            CDBLinkedListNode<E> node = nodeById(nodeoid);
            if(index == cindex)
                return readvalue(node);
            nodeoid = readnext(node);
            cindex++;
        }
        return null;
    }

    @Override
    public E remove(int index) {

        int cindex=0;
        boolean found = false;
        long nodeoid = readhead();
        CDBLinkedListNode<E> node = null;
        CDBLinkedListNode<E> prev = null;

        while(nodeoid != oidnull) {
            node = nodeById(nodeoid);
            if(index == cindex) {
                found = true;
                break;
            }
            nodeoid = readnext(node);
            prev = node;
            cindex++;
        }

        if(!found)
            return null;

        E result = readvalue(node);
        long oidnext = readnext(node);
        long oidprev = prev == null ? oidnull : prev.oid;

        if(oidprev == oidnull) {
            // remove at head from (potentially) singleton from list. equivalent:
            // head = next;
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_HEAD, oidnext));
        }

        else  {
            // remove in middle or end of list. equivalent:
            // prev.next = next;
            TR.update_helper(nodeById(oidprev), new NodeOp(NodeOp.CMD_WRITE_NEXT, oidnext));
        }

        return result;
    }

    @Override
    public boolean remove(Object o) {
        int idx = indexOf(o);
        if (idx==-1) return false;
        remove(idx);
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean res = true;
        for (Object o : c) {
            res &= remove(o);
        }
        return res;
    }

    @Override
    public void replaceAll(UnaryOperator<E> op) {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public void clear() {
        TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_HEAD, oidnull));
        TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_TAIL, oidnull));
    }

    @Override
    public E set(int index, E element) {

        NodeOp<E> cmd;
        int cindex=0;
        CDBLinkedListNode<E> node;
        long nodeoid = readhead();

        while(nodeoid != oidnull) {
            node = nodeById(nodeoid);
            if(index == cindex) {
                cmd = new NodeOp<>(NodeOp.CMD_WRITE_VALUE, node.oid, element);
                TR.update_helper(node, cmd);
                return element;
            }
            nodeoid = readnext(node);
            cindex++;
        }
        return null;
    }

    @Override
    public void sort(Comparator<? super E> c) {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public Spliterator<E> spliterator() {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public Object[] toArray() {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public <E> E[] toArray(E[] a) {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public ListIterator<E> listIterator() {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public Iterator<E> iterator() {
        throw new RuntimeException("unimplemented");
    }

    private CDBLinkedListNode<E> allocNode(E e) {
        CDBLinkedListNode<E> newnode = new CDBLinkedListNode<>(TR, sf, e, DirectoryService.getUniqueID(sf), this);
        wlock();
        try {
            m_nodes.put(newnode.oid, newnode);
            TR.update_helper(newnode, new NodeOp<>(NodeOp.CMD_WRITE_VALUE, newnode.oid, e));
            TR.update_helper(newnode, new NodeOp(NodeOp.CMD_WRITE_NEXT, newnode.oid, oidnull));
            return newnode;
        } finally {
            wunlock();
        }
    }

    protected CDBLinkedListNode<E> readtail() {

        int size = 0;
        long nodeoid = readhead();
        if(nodeoid == oidnull)
            return null;
        CDBLinkedListNode<E> node = nodeById(nodeoid);
        while(node.oidnext != oidnull) {
            nodeoid = readnext(nodeById(nodeoid));
            node = nodeById(nodeoid);
        }
        return node;
    }

    @Override
    public boolean add(E e) {

        CDBLinkedListNode<E> tail = readtail();
        CDBLinkedListNode<E> newnode = allocNode(e);
        long oidtail = tail == null ? oidnull : tail.oid;

        if(oidtail == oidnull) {

            // add to an empty list. It suffices to update the
            // head and tail pointers to point to the new node.
            assert(m_head == oidnull);
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_HEAD, newnode.oid, newnode.oid));

        } else {

            // add to a non-empty list. must point old tail.next to
            // to the new node, and update the tail pointer of the list.
            TR.update_helper(tail, new NodeOp(NodeOp.CMD_WRITE_NEXT, tail.oid, newnode.oid));
        }
        return true;
    }

    @Override
    public void add(int index, E e) {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean res = true;
        for(E o : c){
            res &= add(o);
        }
        return res;
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        throw new RuntimeException("unimplemented");
    }

}


