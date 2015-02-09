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
public class CorfuDBCoarseList<E> extends CorfuDBList<E> {

    static Logger dbglog = LoggerFactory.getLogger(CorfuDBCoarseList.class);
    //backing state of the map
    LinkedList<E> m_memlist;
    LinkedList<E> m_clone;

    public CorfuDBCoarseList(AbstractRuntime tTR, long toid) {
        super(tTR, toid);
        m_memlist = new LinkedList<E>();
        m_clone = null;
        TR = tTR;
        oid = toid;
        TR.registerObject(this);
    }

    public void apply(Object bs) {

        dbglog.debug("CorfuDBList received upcall");
        ListCommand<E> cc = (ListCommand<E>) bs;
        switch (cc.getcmd()) {
            case ListCommand.CMD_POST_ADD:
                System.out.println("got post upcall...");
                cc.setReturnValue(new Boolean(m_memlist.contains(cc.e())));
                break;
            case ListCommand.CMD_ADD:
                System.out.println("got add upcall...");
                cc.setReturnValue(new Boolean(m_memlist.add(cc.e())));
                break;
            case ListCommand.CMD_ADD_AT:
                m_memlist.add(cc.index(), cc.e());
                break;
            case ListCommand.CMD_ADD_ALL:
                cc.setReturnValue(m_memlist.addAll(cc.lparm()));
                break;
            case ListCommand.CMD_CLEAR:
                m_memlist.clear();
                break;
            case ListCommand.CMD_CONTAINS:
                System.out.println("got contains upcall...");
                cc.setReturnValue(new Boolean(m_memlist.contains(cc.e())));
                break;
            case ListCommand.CMD_CONTAINS_ALL:
                cc.setReturnValue(m_memlist.containsAll(cc.lparm()));
                break;
            case ListCommand.CMD_EQUALS:
                cc.setReturnValue(m_memlist.equals(cc.lparm()));
                break;
            case ListCommand.CMD_GET:
                cc.setReturnValue(m_memlist.get(cc.index()));
                break;
            case ListCommand.CMD_HASH_CODE:
                cc.setReturnValue(m_memlist.hashCode());
                break;
            case ListCommand.CMD_INDEX_OF:
                cc.setReturnValue(m_memlist.indexOf(cc.e()));
                break;
            case ListCommand.CMD_LAST_INDEX_OF:
                cc.setReturnValue(m_memlist.lastIndexOf(cc.e()));
                break;
            case ListCommand.CMD_REMOVE_OBJ:
                cc.setReturnValue(m_memlist.remove(cc.e()));
                break;
            case ListCommand.CMD_REMOVE_AT:
                cc.setReturnValue(m_memlist.remove(cc.index()));
                break;
            case ListCommand.CMD_REMOVE_ALL:
                cc.setReturnValue(m_memlist.removeAll(cc.lparm()));
                break;
            case ListCommand.CMD_REPLACE_ALL:
                throw new RuntimeException("Unsupported");
            case ListCommand.CMD_RETAIN_ALL:
                throw new RuntimeException("Unsupported");
            case ListCommand.CMD_SET:
                cc.setReturnValue(m_memlist.set(cc.index(), cc.e()));
                break;
            case ListCommand.CMD_SIZE:
                cc.setReturnValue(m_memlist.size());
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

}


