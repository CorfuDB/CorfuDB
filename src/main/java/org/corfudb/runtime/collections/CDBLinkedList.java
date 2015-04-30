package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.AbstractRuntime;
import org.corfudb.runtime.smr.DirectoryService;
import org.corfudb.runtime.smr.IStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.function.UnaryOperator;
import org.corfudb.client.ITimestamp;

/**
 *
 */
public class CDBLinkedList<E> extends CDBAbstractList<E> {

    static Logger dbglog = LoggerFactory.getLogger(CDBLinkedList.class);
    static protected final HashMap<Long, CDBLinkedList> s_lists = new HashMap<>();

    public long m_head;                                 // oid of head node
    public HashMap<Long, CDBLinkedListNode<E>> m_nodes; // map of all nodes. TODO: prune on delete
    public IStreamFactory sf;                            // stream factory--simplifies new node creation

    /**
     * maintain a view of all known lists.
     * TODO: remove this once it is no longer useful. It
     * ensures nothing will ever get garbage collected!
     * @param loid
     * @return
     */
    static public CDBLinkedList
    findList(long loid) {
        synchronized (s_lists) {
            if (s_lists.containsKey(loid))
                return s_lists.get(loid);
            return null;
        }
    }


    /**
     * required by corfu programming model.
     * handles only R/W ops on head node.
     * @param bs
     * @param timestamp
     */
    public void
    applyToObject(Object bs, ITimestamp timestamp) {

        dbglog.debug("CDBLinkedList received upcall");
        NodeOp<E> cc = (NodeOp<E>) bs;
        switch (cc.cmd()) {
            case NodeOp.CMD_READ_HEAD: applyReadHead(cc); break;
            case NodeOp.CMD_WRITE_HEAD: applyWriteHead(cc); break;
            case NodeOp.CMD_WRITE_TAIL: throw new RuntimeException("invalid operation (write tail on singly linked list)");
            case NodeOp.CMD_READ_TAIL: throw new RuntimeException("invalid operation (read tail on singly linked list)");
        }
    }

    /**
     * apply a read command by returning
     * the current value of the head field.
     * @param cc
     * @return
     */
    protected long
    applyReadHead(NodeOp<E> cc) {
        rlock();
        try {
            cc.setReturnValue(m_head);
        } finally {
            runlock();
        }
        return (long) cc.getReturnValue();
    }

    /**
     * apply a write command by setting the head field.
     * @param cc
     */
    protected void
    applyWriteHead(NodeOp<E> cc) {
        wlock();
        try {
            m_head = cc.oidparam();
        } finally {
            wunlock();
        }
    }

    /**
     * ctor
     * @param tTR
     * @param tsf
     * @param toid
     */
    public
    CDBLinkedList(
        AbstractRuntime tTR,
        IStreamFactory tsf,
        long toid
        )
    {
        super(tTR, tsf, toid);
        m_head = oidnull;
        sf = tsf;
        m_nodes = new HashMap<>();
        synchronized (s_lists) {
            assert(!s_lists.containsKey(oid));
            s_lists.put(oid, this);
        }
    }

    /**
     * find the node with the given id.
     * lock assumed (and asserted to be held).
     * @param noid
     * @return
     */
    protected CDBLinkedListNode<E>
    nodeById_nolock(long noid) {
        assert(lockheld());
        return m_nodes.getOrDefault(noid, null);
    }

    /**
     * find the given list node
     * @param noid
     * @return
     */
    protected CDBLinkedListNode<E>
    nodeById(long noid) {
        rlock();
        try {
            return m_nodes.getOrDefault(noid, null);
        } finally {
            runlock();
        }
    }

    /**
     * read the head of the list.
     * Note, this has the effect of inserting the list container
     * object into the read set, but does not put the actual node
     * there. If query_helper returns false, it means we've already
     * read the head in the current transaction, so we're forced to
     * return the most recently observed value.
     * @return
     */
    protected long
    readhead() {
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

    /**
     * read the next pointer of the given node.
     * If query_helper returns false, it means we've already
     * read the node in the current transaction, so we're forced to
     * return the most recently observed value.
     * @param node
     * @return
     */
    protected long
    readnext(CDBLinkedListNode<E> node) {

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

    /**
     * read the value field of the given node.
     * If query_helper returns false, it means we've already
     * read the node in the current transaction, so we're forced to
     * return the most recently observed value.
     * @param node
     * @return
     */
    protected E
    readvalue(CDBLinkedListNode<E> node) {

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

    /**
     * return the size of the
     * list by traversing it until we find the tail.
     * @return
     */
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

    /**
     * return the index of the given object
     * @param o
     * @return
     */
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

    /**
     * return the size of the list with no
     * guarantee of correctness.
     * @return
     */
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

    /**
     * return the last index of the given object.
     * @param o
     * @return
     */
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

    /**
     * return true if the list is empty
     * @return
     */
    @Override
    public boolean isEmpty() {
        long head = readhead();
        return head == oidnull;
    }

    /**
     * return true if the list contains the
     * given node. builds on indexOf
     * @param o
     * @return
     */
    @Override
    public boolean contains(Object o) {
        return indexOf(o) != -1;
    }

    /**
     * return true if the list contains all
     * the given elements.
     * @param c
     * @return
     */
    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if(!contains(o)) return false;
        }
        return true;
    }

    /**
     * return the element at the given index
     * @param index
     * @return
     */
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

    /**
     * remove the element at the given index
     * @param index
     * @return
     */
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

        if(prev == null) {
            // remove at head from (potentially) singleton from list. equivalent:
            // head = next;
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_HEAD, oidnext));
        }

        else  {
            // remove in middle or end of list. equivalent:
            // prev.next = next;
            TR.update_helper(prev, new NodeOp(NodeOp.CMD_WRITE_NEXT, oidnext));
        }

        return result;
    }

    /**
     * remove the given object.
     * TODO: this implementation is convenient but
     * super inefficient. no need to start from the beginning
     * of the list for every found match.
     * @param o
     * @return
     */
    @Override
    public boolean remove(Object o) {
        int idx;
        boolean removed = false;
        do {
            idx = indexOf(o);
            if (idx!=-1) {
                remove(idx);
                removed = true;
            }
        } while(idx != -1);
        return removed;
    }

    /**
     * remove all matching objects
     * @param c
     * @return
     */
    @Override
    public boolean removeAll(Collection<?> c) {
        boolean res = true;
        for (Object o : c) {
            res &= remove(o);
        }
        return res;
    }

    /**
     * not implemented
     * @param op
     */
    @Override
    public void replaceAll(UnaryOperator<E> op) {
        throw new RuntimeException("unimplemented");
    }

    /**
     * not implemented
     * @param op
     */
    @Override
    public boolean retainAll(Collection<?> c) {
        throw new RuntimeException("unimplemented");
    }

    /**
     * clear the list.
     * read the head explicitly to make sure it's in the RW set.
     */
    @Override
    public void clear() {
        readhead();
        TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_HEAD, oidnull));
    }

    /**
     * set the element at the given index
     * @param index
     * @param element
     * @return
     */
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

    /**
      * not implemented
      * @param c
     */
    @Override
    public void sort(Comparator<? super E> c) {
        throw new RuntimeException("unimplemented");
    }

    /**
     * not implemented
     * @return
     */
    @Override
    public Spliterator<E> spliterator() {
        throw new RuntimeException("unimplemented");
    }

    /**
     * not implemented
     * @return
     */
    @Override
    public Object[] toArray() {
        throw new RuntimeException("unimplemented");
    }

    /**
     * not implemented
     * @return
     */
    @Override
    public <E> E[] toArray(E[] a) {
        throw new RuntimeException("unimplemented");
    }

    /**
     * not implemented
     * @param fromIndex
     * @param toIndex
     * @return
     */
    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        throw new RuntimeException("unimplemented");
    }

    /**
     * not implemented
     * @param index
     * @return
     */
    @Override
    public ListIterator<E> listIterator(int index) {
        throw new RuntimeException("unimplemented");
    }

    /**
     * not implemented
     * @return
     */
    @Override
    public ListIterator<E> listIterator() {
        throw new RuntimeException("unimplemented");
    }

    /**
     * not implemented
     * @return
     */
    @Override
    public Iterator<E> iterator() {
        throw new RuntimeException("unimplemented");
    }

    /**
     * allocate a new node.
     * @param e
     * @return
     */
    private CDBLinkedListNode<E>
    allocNode(E e) {
        CDBLinkedListNode<E> newnode = new CDBLinkedListNode<>(TR, sf, e, DirectoryService.getUniqueID(sf), this);
        wlock();
        try {
            m_nodes.put(newnode.oid, newnode);
        } finally {
            wunlock();
        }
        TR.update_helper(newnode, new NodeOp<>(NodeOp.CMD_WRITE_VALUE, newnode.oid, e));
        TR.update_helper(newnode, new NodeOp(NodeOp.CMD_WRITE_NEXT, newnode.oid, oidnull));
        return newnode;
    }

    /**
     * return the tail node
     * @return
     */
    protected CDBLinkedListNode<E>
    readtail() {

        int size = 0;
        long oidnext = readhead();
        if(oidnext == oidnull) return null;
        CDBLinkedListNode<E> node = null;

        do {
            node = nodeById(oidnext);
            oidnext = readnext(node);
        } while(oidnext != oidnull);
        return node;
    }

    /**
     * add a new element by appending to the tail.
     * @param e
     * @return
     */
    @Override
    public boolean add(E e) {

        CDBLinkedListNode<E> newnode = allocNode(e);
        CDBLinkedListNode<E> tail = readtail();

        if(tail == null) {

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

    /**
     * add an element at the given index
     * @param index
     * @param e
     */
    @Override
    public void add(int index, E e) {

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

        if(!found && cindex != index)
            throw new IndexOutOfBoundsException();

        CDBLinkedListNode<E> newnode = allocNode(e);
        long oidnext = readnext(node);
        long oidprev = prev == null ? oidnull : prev.oid;

        if(prev == null) {
            // add at head from (potentially) singleton from list. equivalent:
            // head = new node;
            // new node.next = old head
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_HEAD, newnode.oid));
            TR.update_helper(newnode, new NodeOp(NodeOp.CMD_WRITE_NEXT, node.oid, node.oid));
        }

        else  {
            // insert at middle or end of list. equivalent:
            // prev.next = new node
            // new node.next = old prev
            TR.update_helper(prev, new NodeOp(NodeOp.CMD_WRITE_NEXT, newnode.oid));
            TR.update_helper(newnode, new NodeOp(NodeOp.CMD_WRITE_NEXT, node.oid, node.oid));
        }
    }

    /**
     * add 'em all!
     * @param c
     * @return
     */
    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean res = true;
        for(E o : c){
            res &= add(o);
        }
        return res;
    }

    /**
     * add 'em all!
     * @param index
     * @param c
     * @return
     */
    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        throw new RuntimeException("unimplemented");
    }

    /**
     * print the current list
     * @return
     */
    @Override
    public String print() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first = true;
        long nodeoid = readhead();
        while(nodeoid != oidnull) {
            CDBLinkedListNode<E> node = nodeById(nodeoid);
            E value = readvalue(node);
            nodeoid = readnext(node);
            if(!first) sb.append(", ");
            first = false;
            sb.append(value);
        }
        sb.append("]");
        return sb.toString();
    }
}


