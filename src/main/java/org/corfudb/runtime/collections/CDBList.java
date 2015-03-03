package org.corfudb.runtime.collections;

import org.corfudb.runtime.AbstractRuntime;
import org.corfudb.runtime.CorfuDBObject;
import org.corfudb.runtime.DirectoryService;
import org.corfudb.runtime.StreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.function.UnaryOperator;

/**
 *
 */
public class CDBList<E> extends CorfuDBList<E>  {

    static Logger dbglog = LoggerFactory.getLogger(CDBList.class);
    static protected HashMap<Long, CDBList> s_lists = new HashMap<Long, CDBList>();
    static public CDBList findList(long loid) {
        synchronized (s_lists) {
            if (s_lists.containsKey(loid))
                return s_lists.get(loid);
            return null;
        }
    }

    public CDBNode<E> m_head;
    public CDBNode<E> m_tail;
    public HashMap<Long, CDBNode<E>> m_nodes;
    public StreamFactory sf;
    public long oid;

    public void applyToObject(Object bs) {

        dbglog.debug("CDBNode received upcall");
        CDBNode<E> node = null;
        CDBNode<E> oprev = null;
        CDBNode<E> onext = null;
        long ooidnext = CDBNode.oidnull;
        long ooidprev = CDBNode.oidnull;
        NodeOp<E> cc = (NodeOp<E>) bs;

        switch (cc.cmd()) {
            case NodeOp.CMD_READ_HEAD: applyReadHead(cc); break;
            case NodeOp.CMD_READ_TAIL: applyReadTail(cc); break;
            case NodeOp.CMD_WRITE_HEAD: applyWriteHead(cc); break;
            case NodeOp.CMD_WRITE_TAIL: applyWriteTail(cc); break;
        }
    }

    protected long applyReadHead(NodeOp<E> cc) {
        rlock();
        try {
            cc.setReturnValue(m_head == null ? CDBNode.oidnull : m_head.oid);
        } finally {
            runlock();
        }
        return (long) cc.getReturnValue();
    }

    protected long applyReadTail(NodeOp<E> cc) {
        rlock();
        try {
            cc.setReturnValue(m_tail == null ? CDBNode.oidnull : m_tail.oid);
        } finally {
            runlock();
        }
        return (long) cc.getReturnValue();
    }

    protected void applyWriteHead(NodeOp<E> cc) {
        wlock();
        try {
            long hoid = cc.oidparam();
            m_head = hoid == CDBNode.oidnull ? null : findNode(hoid, null, false);
        } finally {
            wunlock();
        }
    }

    protected void applyWriteTail(NodeOp<E> cc) {
        wlock();
        try {
            long toid = cc.oidparam();
            m_tail = toid == CDBNode.oidnull ? null : findNode(toid, null, false);
        } finally {
            wunlock();
        }
    }

    public CDBList(AbstractRuntime tTR, StreamFactory tsf, long toid) {
        super(tTR, tsf, toid);
        m_head = null;
        m_tail = null;
        sf = tsf;
        m_nodes = new HashMap<Long, CDBNode<E>>();
        synchronized (s_lists) {
            assert(!s_lists.containsKey(oid));
            s_lists.put(oid, this);
        }
    }

    public CDBNode<E> findNode(long toid, E e, boolean createifabsent) {
        rlock();
        try {
            if(m_nodes.containsKey(toid))
                return m_nodes.get(toid);
            if(createifabsent) {
                wlock();
                try {
                    CDBNode<E> node = new CDBNode<E>(TR, sf, e, toid, this);
                    m_nodes.put(toid, node);
                    return node;
                } finally {
                    wunlock();
                }
            }
            return null;
        } finally {
            runlock();
        }
    }

    public void updateTail(long startoid) {
        assert(false); // obsolete!
        CDBNode<E> node = findNode(startoid, null, false);
        if(node == null) throw new RuntimeException("cannot find node "+startoid);
        while(node.next() != null) {
            node = node.next();
        }
        m_tail = node;
    }

    public void updateHead(long startoid) {
        assert(false); // obsolete!
        CDBNode<E> node = findNode(startoid, null, false);
        if(node == null) throw new RuntimeException("cannot find node "+startoid);
        while(node.prev() != null) {
            node = node.prev();
        }
        m_head = node;
    }

    @Override
    public int size() {

        // add everything to the read set, and then
        // speculate that the list hasn't changed by
        // returning the current size of the list

        NodeOp<E> cmd = new NodeOp(NodeOp.CMD_READ_PARENT, oid);
        TR.query_helper(this, oid, cmd);

        int size = 0;
        CDBNode<E> node = m_head;
        while(node != null) {
            size++;
            cmd = new NodeOp(NodeOp.CMD_READ_NEXT, node.oid);
            TR.query_helper(node, oid, cmd);
            node = node.next();
        }
        return size;
    }

    @Override
    public int indexOf(Object o) {

        int index = 0;
        if(!isTypeE(o)) return -1;

        NodeOp<E> cmd = new NodeOp(NodeOp.CMD_READ_PARENT, oid);
        TR.query_helper(this, oid, cmd);

        CDBNode<E> node = m_head;
        while(node != null) {
            cmd = new NodeOp(NodeOp.CMD_READ_VALUE, node.oid);
            TR.query_helper(node, node.oid, cmd);
            if(node.value.equals(o))
                return index;
            cmd = new NodeOp(NodeOp.CMD_READ_NEXT, node.oid);
            TR.query_helper(node, node.oid, cmd);
            node = node.next();
            index++;
        }
        return -1;
    }

    @Override
    public int sizeview() {
        rlock();
        try {
            int size = 0;
            CDBNode<E> node = m_head;
            while (node != null) {
                size++;
                node = node.next();
            }
            return size;
        } finally {
            runlock();
        }
    }

    @Override
    public int lastIndexOf(Object o) {

        if(!isTypeE(o)) return -1;
        int size = sizeview();
        int index = size-1;

        NodeOp<E> cmd = new NodeOp(NodeOp.CMD_READ_PARENT, oid);
        TR.query_helper(this, oid, cmd);

        CDBNode<E> node = m_tail;
        while(node != null) {
            cmd = new NodeOp(NodeOp.CMD_READ_VALUE, node.oid);
            TR.query_helper(node, node.oid, cmd);
            cmd = new NodeOp(NodeOp.CMD_READ_PREV, node.oid);
            TR.query_helper(node, node.oid, cmd);
            if(node.value.equals(o))
                return index;
            index--;
            node = node.prev();
        }
        return -1;
    }

    @Override
    public boolean isEmpty() {
        NodeOp<E> cmd = new NodeOp(NodeOp.CMD_READ_PARENT, oid);
        TR.query_helper(null, oid, cmd);
        return m_head != null;
    }

    @Override
    public boolean contains(Object o) {
        return indexOf(o) != -1;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if(!contains((E)o)) return false;
        }
        return true;
    }

    @Override
    public E get(int index) {

        int cindex=0;
        NodeOp<E> cmd = new NodeOp(NodeOp.CMD_READ_PARENT, oid);
        TR.query_helper(this, oid, cmd);

        CDBNode<E> node = m_head;
        while(node != null) {
            cmd = new NodeOp(NodeOp.CMD_READ_NEXT, node.oid);
            TR.query_helper(node, node.oid, cmd);
            if(index == cindex) {
                cmd = new NodeOp(NodeOp.CMD_READ_VALUE, node.oid);
                TR.query_helper(node, node.oid, cmd);
                return node.value;
            }
            cindex++;
            node = node.next();
        }
        return null;
    }

    @Override
    public E remove(int index) {

        int cindex=0;
        NodeOp<E> cmd = new NodeOp(NodeOp.CMD_READ_PARENT, oid);
        TR.query_helper(this, oid, cmd);

        boolean found = false;
        CDBNode<E> node = m_head;
        while(node != null) {
            cmd = new NodeOp(NodeOp.CMD_READ_NEXT, node.oid);
            TR.query_helper(node, node.oid, cmd);
            if(index == cindex) {
                found = true;
                break;
            }
            cindex++;
            node = node.next();
        }

        if(!found)
            return null;

        E result = node.value;
        cmd = new NodeOp(NodeOp.CMD_READ_VALUE, node.oid);
        TR.query_helper(node, node.oid, cmd);
        CDBNode<E> next = node.next();
        CDBNode<E> prev = node.prev();

        if(next == null && prev == null) {
            // remove singleton from list. equivalent:
            // head = null;
            // tail = null;
            TR.update_helper(node, new NodeOp(NodeOp.CMD_WRITE_HEAD, oid, CDBNode.oidnull));
            TR.update_helper(node, new NodeOp(NodeOp.CMD_WRITE_TAIL, oid, CDBNode.oidnull));
        }

        else if(next != null && prev != null) {
            // remove in middle of list. equivalent:
            // prev.next = next;
            // next.prev = prev;
            TR.query_helper(next, next.oid, new NodeOp(NodeOp.CMD_READ_PREV, next.oid));
            TR.query_helper(prev, prev.oid, new NodeOp(NodeOp.CMD_READ_NEXT, prev.oid));
            TR.update_helper(next, new NodeOp(NodeOp.CMD_WRITE_PREV, next.oid, prev.oid));
            TR.update_helper(prev, new NodeOp(NodeOp.CMD_WRITE_NEXT, prev.oid, next.oid));
        }

        else if(prev == null) {
            // remove at head of list. equivalent:
            // next.prev = null;
            // head = next;
            TR.query_helper(next, next.oid, new NodeOp(NodeOp.CMD_READ_PREV, next.oid));
            TR.update_helper(next, new NodeOp(NodeOp.CMD_WRITE_PREV, next.oid, CDBNode.oidnull));
            TR.update_helper(node, new NodeOp(NodeOp.CMD_WRITE_HEAD, oid, next.oid));
        }

        else if(next == null) {
            // remove at tail of list. equivalent:
            // prev.next = null;
            // tail = prev;
            TR.query_helper(prev, prev.oid, new NodeOp(NodeOp.CMD_READ_NEXT, prev.oid));
            TR.update_helper(prev, new NodeOp(NodeOp.CMD_WRITE_NEXT, prev.oid, CDBNode.oidnull));
            TR.update_helper(node, new NodeOp(NodeOp.CMD_WRITE_TAIL, oid, CDBNode.oidnull));
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
        TR.update_helper(null, new NodeOp(NodeOp.CMD_WRITE_HEAD, CDBNode.oidnull), oid);
        TR.update_helper(null, new NodeOp(NodeOp.CMD_WRITE_TAIL, CDBNode.oidnull), oid);
    }

    @Override
    public E set(int index, E element) {
        int cindex=0;
        NodeOp<E> cmd = new NodeOp(NodeOp.CMD_READ_PARENT, oid);
        TR.query_helper(null, oid, cmd);

        CDBNode<E> node = m_head;
        while(node != null) {
            cmd = new NodeOp(NodeOp.CMD_READ_NEXT, node.oid);
            TR.query_helper(node, node.oid, cmd);
            if(index == cindex) {
                cmd = new NodeOp(NodeOp.CMD_WRITE_VALUE, node.oid, element);
                TR.update_helper(node, cmd, node.oid);
                return element;
            }
            cindex++;
            node = node.next();
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

    @Override
    public boolean add(E e) {

        NodeOp<E> cmd = new NodeOp(NodeOp.CMD_READ_PARENT, oid);
        System.out.println("TR="+TR+", this="+this+", oid="+oid+", cmd="+cmd);
        TR.query_helper(this, oid, cmd);

        CDBNode<E> newnode = new CDBNode<E>(TR, sf, e, DirectoryService.getUniqueID(sf), this);
        wlock();
        try {
            m_nodes.put(newnode.oid, newnode);
        } finally {
            wunlock();
        }

        cmd = new NodeOp(NodeOp.CMD_WRITE_VALUE, newnode.oid, e);
        TR.update_helper(newnode, cmd, newnode.oid);

        if(m_tail == null) {
            assert(m_head == null);
            TR.update_helper(newnode, new NodeOp(NodeOp.CMD_WRITE_PREV, CDBNode.oidnull));
            TR.update_helper(newnode, new NodeOp(NodeOp.CMD_WRITE_NEXT, CDBNode.oidnull));
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_HEAD, oid, newnode.oid));
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_TAIL, oid, newnode.oid));
        } else {
            TR.update_helper(newnode, new NodeOp(NodeOp.CMD_WRITE_PREV, m_tail.oid));
            TR.update_helper(newnode, new NodeOp(NodeOp.CMD_WRITE_NEXT, CDBNode.oidnull));
            TR.update_helper(this, new NodeOp(NodeOp.CMD_WRITE_HEAD, oid, newnode.oid));
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


