package org.corfudb.runtime.collections;

import org.corfudb.runtime.AbstractRuntime;
import org.corfudb.runtime.StreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.function.UnaryOperator;

/**
 *
 */
public class CDBLogicalList<E> extends CDBAbstractList<E> {

    static Logger dbglog = LoggerFactory.getLogger(CDBLogicalList.class);
    LinkedList<E> m_memlist;

    public CDBLogicalList(AbstractRuntime tTR, StreamFactory sf, long toid) {
        super(tTR, sf, toid);
        m_memlist = new LinkedList<E>();
    }

    public void applyToObject(Object bs, long timestamp) {

        dbglog.debug("CorfuDBList received upcall");
        ListCommand<E> cc = (ListCommand<E>) bs;
        switch (cc.getcmd()) {
            case ListCommand.CMD_POST_ADD: cc.setReturnValue(applyContains(cc.e())); break;
            case ListCommand.CMD_ADD: cc.setReturnValue(new Boolean(applyAddObject(cc.e()))); break;
            case ListCommand.CMD_ADD_AT: applyAddAt(cc.index(), cc.e()); break;
            case ListCommand.CMD_ADD_ALL: applyAddAll(cc.lparm()); break;
            case ListCommand.CMD_CLEAR: applyClear(); break;
            case ListCommand.CMD_CONTAINS: cc.setReturnValue(applyContains(cc.e())); break;
            case ListCommand.CMD_CONTAINS_ALL: cc.setReturnValue(applyContainsAll(cc.lparm())); break;
            case ListCommand.CMD_EQUALS: cc.setReturnValue(applyEquals(cc.lparm())); break;
            case ListCommand.CMD_GET: cc.setReturnValue(applyGet(cc.index())); break;
            case ListCommand.CMD_HASH_CODE: cc.setReturnValue(applyHashCode()); break;
            case ListCommand.CMD_INDEX_OF: cc.setReturnValue(applyIndexOf(cc.e())); break;
            case ListCommand.CMD_LAST_INDEX_OF: cc.setReturnValue(applyLastIndexOf(cc.e())); break;
            case ListCommand.CMD_REMOVE_OBJ: cc.setReturnValue(applyRemove(cc.e())); break;
            case ListCommand.CMD_REMOVE_AT: cc.setReturnValue(applyRemove(cc.index())); break;
            case ListCommand.CMD_REMOVE_ALL: cc.setReturnValue(applyRemoveAll(cc.lparm())); break;
            case ListCommand.CMD_REPLACE_ALL: throw new RuntimeException("Unsupported");
            case ListCommand.CMD_RETAIN_ALL: throw new RuntimeException("Unsupported");
            case ListCommand.CMD_SET: cc.setReturnValue(applySet(cc.index(), cc.e())); break;
            case ListCommand.CMD_SIZE: cc.setReturnValue(applySize()); break;
            case ListCommand.CMD_SORT:
                throw new RuntimeException("Unsupported");
            case ListCommand.CMD_SPLITERATOR:
                throw new RuntimeException("Unsupported");
            case ListCommand.CMD_SUBLIST:
                throw new RuntimeException("Unsupported");
            case ListCommand.CMD_TO_ARRAY:
                throw new RuntimeException("Unsupported");
            case ListCommand.CMD_REMOVE_IF:
                throw new RuntimeException("Unsupported");
        }
        dbglog.debug("list size is {}", m_memlist.size());
    }

    protected int applySize() {
        return sizeview();
    }

    protected boolean applyEquals(Collection<?> c) {
        boolean result = false;
        rlock();
        try {
            result = this.equals(c);
        } finally {
            runlock();
        }
        return result;
    }

    protected int applyHashCode() {
        int result = -1;
        rlock();
        try {
            result = hashCode();
        } finally {
            runlock();
        }
        return result;
    }

    @Override
    public int sizeview() {
        int result = 0;
        rlock();
        try {
            result = m_memlist.size();
        } finally {
            runlock();
        }
        return result;
    }

    protected int applyIndexOf(Object o) {
        int result = -1;
        rlock();
        try {
            result = m_memlist.indexOf(o);
        } finally {
            runlock();
        }
        return result;
    }

    protected int applyLastIndexOf(Object o) {
        int result = 0;
        rlock();
        try {
            result = m_memlist.lastIndexOf(o);
        } finally {
            runlock();
        }
        return result;
    }

    protected boolean applyIsEmpty() {
        boolean result =false;
        rlock();
        try {
            result = m_memlist.isEmpty();
        } finally {
            runlock();
        }
        return result;
    }

    protected boolean applyContains(Object o) {
        boolean result = false;
        rlock();
        try {
            result = m_memlist.contains(o);
        } finally {
            runlock();
        }
        return result;
    }

    protected boolean applyContainsAll(Collection<?> c) {
        boolean result = false;
        rlock();
        try {
            result = m_memlist.containsAll(c);
        } finally {
            runlock();
        }
        return result;
    }

    protected E applyGet(int index) {
        E result = null;
        rlock();
        try {
            if(index > 0 && index < m_memlist.size())
                result = m_memlist.get(index);
        } finally {
            runlock();
        }
        return result;
    }

    protected E applyRemove(int i) {
        E result;
        wlock();
        try {
            result = m_memlist.remove(i);
        } finally {
            wunlock();
        }
        return result;
    }

    protected boolean applyRemove(Object o) {
        boolean result = false;
        wlock();
        try {
            result = m_memlist.remove(o);
        } finally {
            wunlock();
        }
        return result;
    }

    protected boolean applyRemoveAll(Collection<?> c) {
        boolean result = false;
        wlock();
        try {
            result = m_memlist.removeAll(c);
        } finally {
            wunlock();
        }
        return result;
    }

    protected void applyClear() {
        wlock();
        try {
            m_memlist.clear();
        } finally {
            wunlock();
        }
    }

    protected E applySet(int index, E element) {
        E result;
        wlock();
        try {
            result = m_memlist.set(index, element);
        } finally {
            wunlock();
        }
        return result;
    }

    protected boolean applyAddObject(E e) {
        boolean result = false;
        wlock();
        try {
            result = m_memlist.add(e);
        } finally {
            wunlock();
        }
        return result;
    }

    protected void applyAddAt(int index, E e) {
        wlock();
        try {
            m_memlist.add(index, e);
        } finally {
            wunlock();
        }
    }

    protected void applyAddAll(Collection<? extends E> c) {
        wlock();
        try {
            m_memlist.addAll(c);
        } finally {
            wunlock();
        }
    }

    @Override
    public int size() {
        ListCommand sizecmd = new ListCommand(ListCommand.CMD_SIZE);
        if(!TR.query_helper(this, null, sizecmd)) {
            return sizeview();
        }
        return (int) sizecmd.getReturnValue();
    }


    @Override
    public int indexOf(Object o) {
        if(!isTypeE(o)) return -1;
        ListCommand cmd = new ListCommand(ListCommand.CMD_INDEX_OF, (E) o);
        if(!TR.query_helper(this, oid, cmd))
            return applyIndexOf(o);
        return (Integer) cmd.getReturnValue();
    }

    @Override
    public int lastIndexOf(Object o) {
        if(!isTypeE(o)) return -1;
        ListCommand cmd = new ListCommand(ListCommand.CMD_LAST_INDEX_OF, (E) o);
        if(!TR.query_helper(this, oid, cmd))
            return applyLastIndexOf(o);
        return (Integer) cmd.getReturnValue();
    }

    @Override
    public boolean isEmpty() {
        ListCommand isemptycmd = new ListCommand(ListCommand.CMD_IS_EMPTY);
        if(!TR.query_helper(this, oid, isemptycmd))
            return applyIsEmpty();
        return (Boolean) isemptycmd.getReturnValue();
    }

    @Override
    public boolean contains(Object o) {
        if (!isTypeE(o)) return false;
        ListCommand containscmd = new ListCommand(ListCommand.CMD_CONTAINS, (E) o);
        if(!TR.query_helper(this, oid, containscmd))
            return applyContains(o);
        return (Boolean) containscmd.getReturnValue();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) if (!isTypeE(c)) return false;
        ListCommand cmd = new ListCommand(ListCommand.CMD_CONTAINS_ALL, c);
        if(!TR.query_helper(this, oid, cmd))
            return applyContainsAll(c);
        return (Boolean) cmd.getReturnValue();
    }


    @Override
    public E get(int index) {
        ListCommand getcmd = new ListCommand(ListCommand.CMD_GET, index);
        if(!TR.query_helper(this, oid, getcmd))
            return applyGet(index);
        return (E) getcmd.getReturnValue();
    }

    @Override
    public E remove(int index) {
        ListCommand cmd = new ListCommand(ListCommand.CMD_REMOVE_AT, index);
        TR.update_helper(this, cmd, oid);
        return (E) null; // TODO: this breaks the Java list API
        // throw new RuntimeException("unimplemented");
    }

    @Override
    public boolean remove(Object o) {
        if (!isTypeE(o)) return false;
        ListCommand cmd = new ListCommand(ListCommand.CMD_REMOVE_OBJ, (E) o);
        TR.update_helper(this, cmd, oid);
        // return (Boolean) cmd.getReturnValue();
        return true; // FIXME: TODO: this is a speculative result
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        for (Object o : c) if (!isTypeE(c)) return false;
        ListCommand cmd = new ListCommand(ListCommand.CMD_REMOVE_ALL, c);
        TR.update_helper(this, cmd, oid);
        // return (Boolean) cmd.getReturnValue();
        return true;
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
        TR.update_helper(this, new ListCommand<E>(ListCommand.CMD_CLEAR));
    }

    @Override
    public E set(int index, E element) {
        ListCommand<E> cmd = new ListCommand<E>(ListCommand.CMD_SET, element, index);
        TR.update_helper(this, cmd, oid);
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
        ListCommand<E> cmd =  new ListCommand<E>(ListCommand.CMD_ADD, e);
        TR.update_helper(this, cmd, oid);
        return true;
    }

    @Override
    public void add(int index, E e) {
        ListCommand<E> cmd =  new ListCommand<E>(ListCommand.CMD_ADD_AT, e, index);
        TR.update_helper(this, cmd, oid);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        ListCommand<E> cmd =  new ListCommand<E>(ListCommand.CMD_ADD_ALL, c);
        TR.update_helper(this, cmd, oid);
        // return (Boolean)cmd.getReturnValue();
        return true;
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        throw new RuntimeException("unimplemented");
    }

}


