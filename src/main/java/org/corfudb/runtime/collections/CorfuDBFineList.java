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
public class CorfuDBFineList<E> implements List<E>  {

    static Logger dbglog = LoggerFactory.getLogger(CorfuDBFineList.class);
    public LinkedList<CDBListNode<E>> m_memlist;
    LinkedList<CDBListNode<E>> m_clone;
    public StreamFactory sf;
    public AbstractRuntime TR;
    public long oid;

    public CorfuDBFineList(AbstractRuntime tTR, StreamFactory tsf, long toid) {
        TR = tTR;
        oid = toid;
        m_memlist = new LinkedList<CDBListNode<E>>();
        m_clone = null;
        sf = tsf;
    }

    boolean isTypeE(Object o) {
        try {
            E e = (E) o;
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    @Override
    public int size() {
        // add everything to the read set, and then
        // speculate that the list hasn't changed by
        // returning the current size of the list
        RWCommand<E> rhcmd = new RWCommand(RWCommand.CMD_READ, oid);
        TR.query_helper(null, oid, rhcmd);
        for(CDBListNode<E> node:m_memlist) {
            RWCommand<E> rcmd = new RWCommand(RWCommand.CMD_READ, node.oid);
            TR.query_helper(node, node.oid, rcmd);
        }
        return m_memlist.size();
    }

    @Override
    public int indexOf(Object o) {
        int index = 0;
        if(!isTypeE(o)) return -1;
        RWCommand<E> rhcmd = new RWCommand(RWCommand.CMD_READ, oid);
        TR.query_helper(null, oid, rhcmd);
        for(CDBListNode<E> node:m_memlist) {
            RWCommand<E> rcmd = new RWCommand(RWCommand.CMD_READ, node.oid);
            TR.query_helper(node, node.oid, rcmd);
            if(node.equals(o))
                return index;
            index++;
        }
        return -1;
    }

    @Override
    public int lastIndexOf(Object o) {
        if (!isTypeE(o)) return -1;
        RWCommand<E> rhcmd = new RWCommand(RWCommand.CMD_READ, oid);
        TR.query_helper(null, oid, rhcmd);
        for (int i = m_memlist.size() - 1; i >= 0; i--) {
            CDBListNode<E> node = m_memlist.get(i);
            RWCommand<E> rcmd = new RWCommand(RWCommand.CMD_READ, node.oid);
            TR.query_helper(node, node.oid, rcmd);
            if (node.equals(o))
                return i;
        }
        return -1;
    }

    @Override
    public boolean isEmpty() {
        RWCommand<E> rhcmd = new RWCommand(RWCommand.CMD_READ, oid);
        TR.query_helper(null, oid, rhcmd);
        return size() == 0;
    }

    @Override
    public boolean contains(Object o) {
        if (!isTypeE(o)) return false;
        RWCommand<E> rhcmd = new RWCommand(RWCommand.CMD_READ, oid);
        TR.query_helper(null, oid, rhcmd);
        return indexOf(o) != -1;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (!isTypeE(c)) return false;
            if(!contains((E)o)) return false;
        }
        return true;
    }

    @Override
    public E get(int index) {
        int cindex = 0;
        RWCommand<E> rhcmd = new RWCommand(RWCommand.CMD_READ, oid);
        TR.query_helper(null, oid, rhcmd);
        for(CDBListNode<E> node:m_memlist) {
            RWCommand<E> rcmd = new RWCommand(RWCommand.CMD_READ, node.oid);
            TR.query_helper(node, node.oid, rcmd);
            if(cindex == index)
                return node.value;
        }
        return null;
    }

    @Override
    public E remove(int index) {
        if(index >= m_memlist.size())
            return null;
        int cindex = 0;
        RWCommand<E> rhcmd = new RWCommand(RWCommand.CMD_READ, oid);
        TR.query_helper(null, oid, rhcmd);
        for(CDBListNode<E> node:m_memlist) {
            RWCommand<E> rcmd = new RWCommand(RWCommand.CMD_READ, node.oid);
            TR.query_helper(node, node.oid, rcmd);
            if(cindex == index) {
                if(m_memlist.size() == 1) {
                    RWCommand<E> wcmd = new RWCommand(RWCommand.CMD_WRITE_HEAD, oid);
                    TR.update_helper(null, wcmd);
                } else if(index == 0) {
                    CDBListNode<E> nnode = m_memlist.get(index+1);
                    RWCommand<E> rncmd = new RWCommand(RWCommand.CMD_READ, nnode.oid);
                    RWCommand<E> wcmd = new RWCommand(RWCommand.CMD_WRITE_HEAD, nnode, oid);
                    TR.query_helper(nnode, nnode.oid, rncmd);
                    TR.update_helper(node, wcmd);
                } else if(index == m_memlist.size() - 1) {
                    RWCommand<E> wcmd = new RWCommand(RWCommand.CMD_WRITE_NEXT, node, -1);
                    TR.update_helper(node, wcmd);
                }
            }
        }
        return null;
    }

    @Override
    public boolean remove(Object o) {
        if (!isTypeE(o)) return false;
        int idx = indexOf(o);
        if (idx==-1) return false;
        remove(idx);
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean res = true;
        for (Object o : c) if (!isTypeE(c)) return false;
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
        RWCommand<E> rhcmd = new RWCommand(RWCommand.CMD_READ, oid);
        TR.query_helper(null, oid, rhcmd);
        RWCommand<E> whcmd = new RWCommand<E>(RWCommand.CMD_WRITE_HEAD, oid);
        TR.update_helper(null, whcmd, oid);
    }

    @Override
    public E set(int index, E element) {
        int cindex = 0;
        RWCommand<E> rhcmd = new RWCommand(RWCommand.CMD_READ, oid);
        TR.query_helper(null, oid, rhcmd);
        if(m_memlist.size() <= index) return null;
        if(index < 0) return null;
        for(CDBListNode<E> node:m_memlist) {
            RWCommand<E> rcmd = new RWCommand(RWCommand.CMD_READ, node.oid);
            TR.query_helper(node, node.oid, rcmd);
            if(cindex == index) {
                RWCommand<E> wcmd = new RWCommand<E>(RWCommand.CMD_WRITE_VALUE, element, node.oid);
                TR.update_helper(node, wcmd, node.oid);
            }
            cindex++;
        }
        return element;
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
        int index = 0;
        int size = m_memlist.size();
        RWCommand<E> rhcmd = new RWCommand(RWCommand.CMD_READ, oid);
        TR.query_helper(null, oid, rhcmd);
        CDBListNode<E> tail = null;
        for(CDBListNode<E> node:m_memlist) {
            RWCommand<E> rcmd = new RWCommand(RWCommand.CMD_READ, node.oid);
            TR.query_helper(node, node.oid, rcmd);
            if(++index == size)
                tail = node;
        }
        assert(size == 0 || tail != null);
        CDBListNode<E> newnode = new CDBListNode<E>(TR, e, DirectoryService.getUniqueID(sf), this);
        if(size == 0) {
            RWCommand<E> whcmd = new RWCommand<E>(RWCommand.CMD_WRITE_HEAD, e, oid);
            TR.update_helper(null, whcmd, oid);
        } else {
            RWCommand<E> wtcmd = new RWCommand<E>(RWCommand.CMD_WRITE_NEXT, null, tail.oid, newnode.oid);
            RWCommand<E> waddcmd = new RWCommand<E>(RWCommand.CMD_WRITE_HEAD, e, newnode.oid);
            TR.update_helper(tail, wtcmd, tail.oid);
            TR.update_helper(null, waddcmd, newnode.oid);
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


