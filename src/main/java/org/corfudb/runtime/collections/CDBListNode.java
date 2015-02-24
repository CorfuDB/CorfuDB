package org.corfudb.runtime.collections;

import org.corfudb.runtime.AbstractRuntime;
import org.corfudb.runtime.CorfuDBObject;
import org.corfudb.runtime.DirectoryService;
import org.corfudb.runtime.StreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.function.UnaryOperator;

public class CDBListNode<E> extends CorfuDBObject {

    static Logger dbglog = LoggerFactory.getLogger(CorfuDBFineList.class);
    public E value;
    public long next;
    public CorfuDBFineList<E> m_parent;

    public CDBListNode(AbstractRuntime tr, E _val, long oid, CorfuDBFineList<E> parent) {
        super(tr, oid);
        value = _val;
        next = -1;
        m_parent = parent;
    }

    CDBListNode findByOid(long oid) {
        for(CDBListNode<E> node : m_parent.m_memlist) {
            if(node.oid == oid)
                return node;
        }
        return null;
    }

    public void applyToObject(Object bs) {
        dbglog.debug("CorfuDBList received upcall");
        CDBListNode<E> node = null;
        RWCommand<E> cc = (RWCommand<E>) bs;
        switch(cc.cmd()) {
            case RWCommand.CMD_WRITE_VALUE:
                assert (oid == cc.oid());
                value = cc.e();
                node = findByOid(oid);
                node.value = value;
                break;
            case RWCommand.CMD_WRITE_NEXT:
                assert (oid == cc.oid());
                next = cc.next();
                node = findByOid(oid);
                node.next = next;
                break;
            case RWCommand.CMD_WRITE_HEAD:
                assert (oid == cc.oid());
                next = cc.next();
                node = cc.m_node;
                value = cc.m_elem;
                m_parent.oid = next;
                if(value == null)
                m_parent.add(0, cc.m_elem);
                break;
            case RWCommand.CMD_READ:
                assert (oid == cc.oid());
                next = cc.next();
                value = cc.e();
                cc.setReturnValue(value);
                break;
            case RWCommand.CMD_READ_NEXT:
                assert (oid == cc.oid());
                next = cc.next();
                cc.setReturnValue(next);
                break;
        }
    }
}
