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
public class CorfuDBCoarseList<E> extends CorfuDBList<E> {

    static Logger dbglog = LoggerFactory.getLogger(CorfuDBCoarseList.class);
    LinkedList<E> m_memlist;

    public CorfuDBCoarseList(AbstractRuntime tTR, StreamFactory sf, long toid) {
        super(tTR, sf, toid);
        m_memlist = new LinkedList<E>();
    }

    public void applyToObject(Object bs) {

        dbglog.debug("CorfuDBList received upcall");
        ListCommand<E> cc = (ListCommand<E>) bs;
        switch (cc.getcmd()) {
            case ListCommand.CMD_POST_ADD:
                lock(false);
                cc.setReturnValue(new Boolean(m_memlist.contains(cc.e())));
                unlock(false);
                break;
            case ListCommand.CMD_ADD:
                lock(true);
                cc.setReturnValue(new Boolean(m_memlist.add(cc.e())));
                unlock(true);
                break;
            case ListCommand.CMD_ADD_AT:
                lock(true);
                m_memlist.add(cc.index(), cc.e());
                unlock(true);
                break;
            case ListCommand.CMD_ADD_ALL:
                lock(true);
                cc.setReturnValue(m_memlist.addAll(cc.lparm()));
                unlock(true);
                break;
            case ListCommand.CMD_CLEAR:
                lock(true);
                m_memlist.clear();
                unlock(true);
                break;
            case ListCommand.CMD_CONTAINS:
                lock(false);
                cc.setReturnValue(new Boolean(m_memlist.contains(cc.e())));
                unlock(false);
                break;
            case ListCommand.CMD_CONTAINS_ALL:
                lock(false);
                cc.setReturnValue(m_memlist.containsAll(cc.lparm()));
                unlock(false);
                break;
            case ListCommand.CMD_EQUALS:
                lock(false);
                cc.setReturnValue(m_memlist.equals(cc.lparm()));
                unlock(false);
                break;
            case ListCommand.CMD_GET:
                lock(false);
                cc.setReturnValue(m_memlist.get(cc.index()));
                unlock(false);
                break;
            case ListCommand.CMD_HASH_CODE:
                lock(false);
                cc.setReturnValue(m_memlist.hashCode());
                unlock(false);
                break;
            case ListCommand.CMD_INDEX_OF:
                lock(false);
                cc.setReturnValue(m_memlist.indexOf(cc.e()));
                unlock(false);
                break;
            case ListCommand.CMD_LAST_INDEX_OF:
                lock(false);
                cc.setReturnValue(m_memlist.lastIndexOf(cc.e()));
                unlock(false);
                break;
            case ListCommand.CMD_REMOVE_OBJ:
                lock(true);
                cc.setReturnValue(m_memlist.remove(cc.e()));
                unlock(true);
                break;
            case ListCommand.CMD_REMOVE_AT:
                lock(true);
                cc.setReturnValue(m_memlist.remove(cc.index()));
                unlock(true);
                break;
            case ListCommand.CMD_REMOVE_ALL:
                lock(true);
                cc.setReturnValue(m_memlist.removeAll(cc.lparm()));
                unlock(true);
                break;
            case ListCommand.CMD_REPLACE_ALL:
                throw new RuntimeException("Unsupported");
            case ListCommand.CMD_RETAIN_ALL:
                throw new RuntimeException("Unsupported");
            case ListCommand.CMD_SET:
                lock(true);
                cc.setReturnValue(m_memlist.set(cc.index(), cc.e()));
                unlock(true);
                break;
            case ListCommand.CMD_SIZE:
                lock(false);
                cc.setReturnValue(m_memlist.size());
                unlock(false);
                break;
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

    void rlock() { lock(false); }
    void runlock() { unlock(false); }
    void wlock() { lock(true); }
    void wunlock() { unlock(true); }

    @Override
    public int sizeview() {
        rlock();
        int result = m_memlist.size();
        runlock();
        return result;
    }

    @Override
    public int size() {
        ListCommand sizecmd = new ListCommand(ListCommand.CMD_SIZE);
        if(!TR.query_helper(this, null, sizecmd)) {
            return sizeview();
        }
        return (int) sizecmd.getReturnValue();
    }

    public int indexOfview(Object o) {
        rlock();
        int result = m_memlist.indexOf(o);
        runlock();
        return result;
    }

    @Override
    public int indexOf(Object o) {
        if(!isTypeE(o)) return -1;
        ListCommand cmd = new ListCommand(ListCommand.CMD_INDEX_OF, (E) o);
        if(!TR.query_helper(this, oid, cmd))
            return indexOfview(o);
        return (Integer) cmd.getReturnValue();
    }

    public int lastIndexOfview(Object o) {
        rlock();
        int result = m_memlist.lastIndexOf(o);
        runlock();
        return result;
    }

    @Override
    public int lastIndexOf(Object o) {
        if(!isTypeE(o)) return -1;
        ListCommand cmd = new ListCommand(ListCommand.CMD_LAST_INDEX_OF, (E) o);
        if(!TR.query_helper(this, oid, cmd))
            return lastIndexOfview(o);
        return (Integer) cmd.getReturnValue();
    }

    public boolean isEmptyView() {
        rlock();
        boolean result = m_memlist.isEmpty();
        runlock();
        return result;
    }

    @Override
    public boolean isEmpty() {
        ListCommand isemptycmd = new ListCommand(ListCommand.CMD_IS_EMPTY);
        if(!TR.query_helper(this, oid, isemptycmd))
            return isEmptyView();
        return (Boolean) isemptycmd.getReturnValue();
    }

    public boolean containsView(Object o) {
        rlock();
        boolean result = m_memlist.contains(o);
        runlock();
        return result;
    }


    @Override
    public boolean contains(Object o) {
        if (!isTypeE(o)) return false;
        ListCommand containscmd = new ListCommand(ListCommand.CMD_CONTAINS, (E) o);
        if(!TR.query_helper(this, oid, containscmd))
            return containsView(o);
        return (Boolean) containscmd.getReturnValue();
    }

    public boolean containsAllView(Collection<?> c) {
        rlock();
        boolean result = m_memlist.containsAll(c);
        runlock();
        return result;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) if (!isTypeE(c)) return false;
        ListCommand cmd = new ListCommand(ListCommand.CMD_CONTAINS_ALL, c);
        if(!TR.query_helper(this, oid, cmd))
            return containsAllView(c);
        return (Boolean) cmd.getReturnValue();
    }

    public E getView(int index) {
        rlock();
        E result = m_memlist.get(index);
        runlock();
        return result;
    }

    @Override
    public E get(int index) {
        ListCommand getcmd = new ListCommand(ListCommand.CMD_GET, index);
        if(!TR.query_helper(this, oid, getcmd))
            return getView(index);
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
        // ACHTUNG: sadly, we cannot know whether this succeeds until we see the upcall.
        // Which will not happen until the transaction commits. Hence returning true/false
        // is technically impossible here. We can return true/false based on our current
        // view of the list, which is slightly better, but it would require us to modify the
        // data structure and the result is still speculative, so if we take this route we have
        // to clone the data structure to ensure that we can recover local state on an abort.
        // for now, explore limiting the programming model: basically, we can't do synchronous,
        // conditional updates.
        // return (Boolean)cmd.getReturnValue();
        return true; // TODO: FIXME: this will lie to the programmer sometimes!
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


