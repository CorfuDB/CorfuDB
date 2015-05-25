package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.legacy.AbstractRuntime;
import org.corfudb.runtime.smr.legacy.DirectoryService;
import org.corfudb.runtime.smr.IStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.function.UnaryOperator;
import org.corfudb.runtime.stream.ITimestamp;

/**
 *
 */
public class CDBDoublyLinkedList<E> extends CDBAbstractList<E> {

    static Logger dbglog = LoggerFactory.getLogger(CDBDoublyLinkedList.class);
    static protected final HashMap<UUID, CDBDoublyLinkedList> s_lists = new HashMap<>();
    public UUID m_head;
    public UUID m_tail;
    public HashMap<UUID, CDBDoublyLinkedListNode<E>> m_nodes;
    public IStreamFactory sf;

    /**
     * track all instances
     * @param loid
     * @return
     */
    static public
    CDBDoublyLinkedList findList(UUID loid) {
        synchronized (s_lists) {
            if (s_lists.containsKey(loid))
                return s_lists.get(loid);
            return null;
        }
    }

    /**
     * ctor
     * @param tTR
     * @param tsf
     * @param toid
     */
    public
    CDBDoublyLinkedList(
        AbstractRuntime tTR,
        IStreamFactory tsf,
        UUID toid
        )
    {
        super(tTR, tsf, toid);
        m_head = oidnull;
        m_tail = oidnull;
        sf = tsf;
        m_nodes = new HashMap<>();
        synchronized (s_lists) {
            assert(!s_lists.containsKey(oid));
            s_lists.put(oid, this);
        }
    }

    /**
     * required by runtime
     * @param bs
     * @param timestamp
     */
    public void
    applyToObject(Object bs, ITimestamp timestamp) {

        dbglog.debug("CDBNode received upcall");
        NodeOp<E> cc = (NodeOp<E>) bs;
        switch (cc.cmd()) {
            case NodeOp.CMD_READ_HEAD: applyReadHead(cc); break;
            case NodeOp.CMD_READ_TAIL: applyReadTail(cc); break;
            case NodeOp.CMD_WRITE_HEAD: applyWriteHead(cc); break;
            case NodeOp.CMD_WRITE_TAIL: applyWriteTail(cc); break;
        }
    }

    /**
     * read the head of the list
     * @param cc
     * @return
     */
    protected UUID
    applyReadHead(NodeOp<E> cc) {
        rlock();
        try {
            cc.setReturnValue(m_head);
        } finally {
            runlock();
        }
        return (UUID) cc.getReturnValue();
    }

    /**
     * read the tail
     * @param cc
     * @return
     */
    protected UUID
    applyReadTail(NodeOp<E> cc) {
        rlock();
        try {
            cc.setReturnValue(m_tail);
        } finally {
            runlock();
        }
        return (UUID) cc.getReturnValue();
    }

    /**
     * apply a write head command
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
     * apply a write tail command
     * @param cc
     */
    protected void
    applyWriteTail(NodeOp<E> cc) {
        wlock();
        try {
            m_tail = cc.oidparam();
        } finally {
            wunlock();
        }
    }

    /**
     * get the node with the given id. lock assumed held
     * @param noid
     * @return
     */
    protected
    CDBDoublyLinkedListNode<E>
    nodeById_nolock(
        UUID noid
        )
    {
        assert(lockheld());
        return m_nodes.getOrDefault(noid, null);
    }

    /**
     * get the node with the given oid
     * @param noid
     * @return
     */
    protected CDBDoublyLinkedListNode<E>
    nodeById(
        UUID noid
        )
    {
        rlock();
        try {
            return m_nodes.getOrDefault(noid, null);
        } finally {
            runlock();
        }
    }

    /**
     * read the head field
     * @return
     */
    protected UUID
    readhead() {
        NodeOp<E> cmd = new NodeOp<>(NodeOp.CMD_READ_HEAD, oid, oid);
        if (TR.query_helper(this, null, cmd))
            return (UUID) cmd.getReturnValue();
        rlock();
        try {
            return m_head;
        } finally {
            runlock();
        }
    }

    /**
     * read the tail field
     * @return
     */
    protected UUID
    readtail() {
        NodeOp<E> cmd = new NodeOp<>(NodeOp.CMD_READ_TAIL, oid, oid);
        if(TR.query_helper(this, null, cmd))
            return (UUID) cmd.getReturnValue();
        rlock();
        try {
            return m_tail;
        } finally {
            runlock();
        }
    }

    /**
     * read the next pointer of a given node
     * @param node
     * @return
     */
    protected UUID
    readnext(CDBDoublyLinkedListNode<E> node) {

        NodeOp<E> cmd = new NodeOp<>(NodeOp.CMD_READ_NEXT, node.oid, node.oid);
        if (TR.query_helper(node, null, cmd))
            return (UUID) cmd.getReturnValue();
        node.rlock();
        try {
            return node.oidnext;
        } finally {
            node.runlock();
        }
    }

    /**
     * read the prev pointer for a given node
     * @param node
     * @return
     */
    protected UUID
    readprev(CDBDoublyLinkedListNode<E> node) {

        NodeOp<E> cmd = new NodeOp<>(NodeOp.CMD_READ_PREV, node.oid, node.oid);
        if (TR.query_helper(node, null, cmd))
            return (UUID) cmd.getReturnValue();
        node.rlock();
        try {
            return node.oidprev;
        } finally {
            node.runlock();
        }
    }

    /**
     * read the value of a given node
     * @param node
     * @return
     */
    protected E
    readvalue(CDBDoublyLinkedListNode<E> node) {

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
     * return the size of the list
     * @return
     */
    @Override
    public int size() {

        int size = 0;
        UUID nodeoid = readhead();
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
        UUID oidnode = readhead();

        while(oidnode != oidnull) {
            CDBDoublyLinkedListNode<E> node = nodeById(oidnode);
            value = readvalue(node);
            if(value.equals(o))
                return index;
            oidnode = readnext(node);
            index++;
        }
        return -1;
    }

    /**
     * return the size based on the most recent view
     * @return
     */
    @Override
    public int sizeview() {

        rlock();
        try {
            int size = 0;
            UUID oidnode = m_head;
            while (oidnode != oidnull) {
                size++;
                CDBDoublyLinkedListNode<E> node = nodeById_nolock(oidnode);
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
     * return the last index of the given object
     * @param o
     * @return
     */
    @Override
    public int lastIndexOf(Object o) {

        if(!isTypeE(o)) return -1;
        int size = size();
        int index = size-1;

        E value;
        UUID oidnode = readtail();
        while(oidnode != oidnull) {
            CDBDoublyLinkedListNode<E> node = nodeById(oidnode);
            value = readvalue(node);
            if(value.equals(o))
                return index;
            oidnode = readprev(node);
            index--;
        }
        return -1;
    }

    /**
     * return true if the list is empty
     * @return
     */
    @Override
    public boolean isEmpty() {
        UUID head = readhead();
        return head == oidnull;
    }

    /**
     * return true if the list contains the given object
     * @param o
     * @return
     */
    @Override
    public boolean contains(Object o) {
        return indexOf(o) != -1;
    }

    /**
     * return true if the list contains all the given objects
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
     * get the value at the given index
     * @param index
     * @return
     */
    @Override
    public E get(int index) {

        int cindex=0;
        UUID nodeoid = readhead();
        while(nodeoid != oidnull) {
            CDBDoublyLinkedListNode<E> node = nodeById(nodeoid);
            if(index == cindex)
                return readvalue(node);
            nodeoid = readnext(node);
            cindex++;
        }
        return null;
    }

    /**
     * return the value at the given index
     * @param index
     * @return
     */
    @Override
    public E remove(int index) {

        int cindex=0;
        boolean found = false;
        UUID nodeoid = readhead();
        CDBDoublyLinkedListNode<E> node = null;

        while(nodeoid != oidnull) {
            node = nodeById(nodeoid);
            if(index == cindex) {
                found = true;
                break;
            }
            nodeoid = readnext(node);
            cindex++;
        }

        if(!found)
            return null;

        E result = readvalue(node);
        UUID oidnext = readnext(node);
        UUID oidprev = readprev(node);

        if(oidnext == oidnull && oidprev == oidnull) {
            // remove singleton from list. equivalent:
            // head = null;
            // tail = null;
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_HEAD, oidnull));
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_TAIL, oidnull));
        }

        else if(oidnext != oidnull && oidprev != oidnull) {
            // remove in middle of list. equivalent:
            // prev.next = next;
            // next.prev = prev;
            TR.update_helper(nodeById(oidnext), new NodeOp(NodeOp.CMD_WRITE_PREV, oidnext, oidprev));
            TR.update_helper(nodeById(oidprev), new NodeOp(NodeOp.CMD_WRITE_NEXT, oidprev, oidnext));
        }

        else if(oidprev == oidnull) {
            // remove at head of list. equivalent:
            // next.prev = null;
            // head = next;
            TR.update_helper(nodeById(oidnext), new NodeOp(NodeOp.CMD_WRITE_PREV, oidnext, oidnull));
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_HEAD, oidnext, oidnext));
        }

        else {
            // remove at tail of list. equivalent:
            // prev.next = null;
            // tail = prev;
            TR.update_helper(nodeById(oidprev), new NodeOp(NodeOp.CMD_WRITE_NEXT, oidprev, oidnull));
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_TAIL, oidprev, oidprev));
        }

        return result;
    }

    /**
     * remove the given object
     * @param o
     * @return
     */
    @Override
    public boolean remove(Object o) {
        boolean removed = false;
        while(contains(o)) {
            int idx = indexOf(o);
            remove(idx);
            removed = true;
        }
        return removed;
    }

    /**
     * remove all the objects
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
     * replace all
     * @param op
     */
    @Override
    public void replaceAll(UnaryOperator<E> op) {
        throw new RuntimeException("unimplemented");
    }

    /**
     * filter the list
     * @param c
     * @return
     */
    @Override
    public boolean retainAll(Collection<?> c) {
        throw new RuntimeException("unimplemented");
    }

    /**
     * clear the list
     */
    @Override
    public void clear() {
        readhead();
        TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_HEAD, oidnull));
        TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_TAIL, oidnull));
    }

    /**
     * set the value at the given index
     * @param index
     * @param element
     * @return
     */
    @Override
    public E set(int index, E element) {

        NodeOp<E> cmd;
        int cindex=0;
        CDBDoublyLinkedListNode<E> node;
        UUID nodeoid = readhead();

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
     * sort
     * @param c
     */
    @Override
    public void sort(Comparator<? super E> c) {
        throw new RuntimeException("unimplemented");
    }

    /**
     * unimplemented
     * @return
     */
    @Override
    public Spliterator<E> spliterator() {
        throw new RuntimeException("unimplemented");
    }

    /**
     * unimplemented
     * @return
     */
    @Override
    public Object[] toArray() {
        throw new RuntimeException("unimplemented");
    }

    /**
     * unimplemented
     * @return
     */
    @Override
    public <E> E[] toArray(E[] a) {
        throw new RuntimeException("unimplemented");
    }

    /**
     * unimplemented
     * @return
     */
    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        throw new RuntimeException("unimplemented");
    }

    /**
     * unimplemented
     * @return
     */
    @Override
    public ListIterator<E> listIterator(int index) {
        throw new RuntimeException("unimplemented");
    }

    /**
     * unimplemented
     * @return
     */
    @Override
    public ListIterator<E> listIterator() {
        throw new RuntimeException("unimplemented");
    }

    /**
     * unimplemented
     * @return
     */
    @Override
    public Iterator<E> iterator() {
        throw new RuntimeException("unimplemented");
    }

    /**
     * allocate a new node
     * @param e
     * @param oidtail
     * @return
     */
    private CDBDoublyLinkedListNode<E>
    allocNode(
        E e,
        UUID oidtail
        )
    {
        CDBDoublyLinkedListNode<E> newnode =
                new CDBDoublyLinkedListNode<>(TR, sf, e, DirectoryService.getUniqueID(sf), this);

        wlock();
        try {
            m_nodes.put(newnode.oid, newnode);
        } finally {
            wunlock();
        }

        TR.update_helper(newnode, new NodeOp<>(NodeOp.CMD_WRITE_VALUE, newnode.oid, e));
        TR.update_helper(newnode, new NodeOp(NodeOp.CMD_WRITE_PREV, newnode.oid, oidtail));
        TR.update_helper(newnode, new NodeOp(NodeOp.CMD_WRITE_NEXT, newnode.oid, oidnull));
        return newnode;
    }

    /**
     * add the given element
     * @param e
     * @return
     */
    @Override
    public boolean add(E e) {

        UUID oidtail = readtail();
        CDBDoublyLinkedListNode<E> newnode = allocNode(e, oidtail);

        if(oidtail == oidnull) {

            // add to an empty list. It suffices to update the
            // head and tail pointers to point to the new node.
            assert(m_head == oidnull);
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_HEAD, newnode.oid, newnode.oid));
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_TAIL, newnode.oid, newnode.oid));

        } else {

            // add to a non-empty list. must point old tail.next to
            // to the new node, and update the tail pointer of the list.
            CDBDoublyLinkedListNode<E> tail = nodeById(oidtail);
            TR.update_helper(tail, new NodeOp(NodeOp.CMD_WRITE_NEXT, tail.oid, newnode.oid));
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_TAIL, newnode.oid, newnode.oid));
        }
        return true;
    }

    /**
     * unimplemented
     * @param index
     * @param e
     */
    @Override
    public void add(int index, E e) {
        throw new RuntimeException("unimplemented");
    }

    /**
     * unimplemented
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
     * print the current state of the list
     * @return
     */
    @Override
    public String print() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first = true;
        UUID nodeoid = readhead();
        while(nodeoid != oidnull) {
            CDBDoublyLinkedListNode<E> node = nodeById(nodeoid);
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


