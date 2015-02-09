package org.corfudb.runtime.collections;

import org.corfudb.runtime.AbstractRuntime;
import org.corfudb.runtime.CorfuDBObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.function.UnaryOperator;

/**
 *
 */
public abstract class CorfuDBList<E> extends CorfuDBObject implements List<E> {

    static Logger dbglog = LoggerFactory.getLogger(CorfuDBCoarseList.class);
    //backing state of the map

    public CorfuDBList(AbstractRuntime tTR, long toid) {
        super(tTR, toid);
        TR = tTR;
        oid = toid;
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
        ListCommand sizecmd = new ListCommand(ListCommand.CMD_SIZE);
        TR.query_helper(this, null, sizecmd);
        return (Integer) sizecmd.getReturnValue();
    }

    @Override
    public int indexOf(Object o) {
        if(!isTypeE(o)) return -1;
        ListCommand cmd = new ListCommand(ListCommand.CMD_INDEX_OF, (E) o);
        TR.query_helper(this, null, cmd);
        return (Integer) cmd.getReturnValue();
    }

    @Override
    public int lastIndexOf(Object o) {
        if(!isTypeE(o)) return -1;
        ListCommand cmd = new ListCommand(ListCommand.CMD_LAST_INDEX_OF, (E) o);
        TR.query_helper(this, null, cmd);
        return (Integer) cmd.getReturnValue();
    }


    @Override
    public boolean isEmpty() {
        ListCommand isemptycmd = new ListCommand(ListCommand.CMD_IS_EMPTY);
        TR.query_helper(this, null, isemptycmd);
        return (Boolean) isemptycmd.getReturnValue();
    }

    @Override
    public boolean contains(Object o) {
        if (!isTypeE(o)) return false;
        ListCommand containscmd = new ListCommand(ListCommand.CMD_CONTAINS, (E) o);
        TR.query_helper(this, null, containscmd);
        return (Boolean) containscmd.getReturnValue();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) if (!isTypeE(c)) return false;
        ListCommand cmd = new ListCommand(ListCommand.CMD_CONTAINS_ALL, c);
        TR.query_helper(this, null, cmd);
        return (Boolean) cmd.getReturnValue();
    }

    @Override
    public E get(int index) {
        ListCommand getcmd = new ListCommand(ListCommand.CMD_GET, index);
        TR.query_helper(this, null, getcmd);
        return (E) getcmd.getReturnValue();
    }

    @Override
    public E remove(int index) {
        //ListCommand cmd = new ListCommand(ListCommand.CMD_REMOVE_AT, index);
        //TR.update_helper(this, cmd);
        //return (E) cmd.getReturnValue();
        throw new RuntimeException("unimplemented");
    }

    @Override
    public boolean remove(Object o) {
        if (!isTypeE(o)) return false;
        ListCommand cmd = new ListCommand(ListCommand.CMD_REMOVE_OBJ, (E) o);
        TR.update_helper(this, cmd);
        // return (Boolean) cmd.getReturnValue();
        return true; // FIXME: TODO: this is a speculative result
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        for (Object o : c) if (!isTypeE(c)) return false;
        ListCommand cmd = new ListCommand(ListCommand.CMD_REMOVE_ALL, c);
        TR.update_helper(this, cmd);
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
        TR.update_helper(this, cmd);
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
        TR.update_helper(this, cmd);
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
        TR.update_helper(this, cmd);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        ListCommand<E> cmd =  new ListCommand<E>(ListCommand.CMD_ADD_ALL, c);
        TR.update_helper(this, cmd);
        // return (Boolean)cmd.getReturnValue();
        return true;
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        throw new RuntimeException("unimplemented");
    }
}


