package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.AbstractRuntime;
import org.corfudb.runtime.smr.CorfuDBObject;
import org.corfudb.runtime.smr.IStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.corfudb.runtime.stream.ITimestamp;

public class CDBLinkedListNode<E> extends CorfuDBObject {

    public static final boolean isDistributed = false;
    static Logger dbglog = LoggerFactory.getLogger(CDBLinkedListNode.class);

    public E value;
    public long oidnext;
    public long oidparent;
    protected transient CDBLinkedList<E> _parentlist;
    public transient IStreamFactory _sf;

    public CDBLinkedListNode(
            AbstractRuntime tr,
            IStreamFactory tsf,
            E _val,
            long oid,
            CDBLinkedList<E> parent
        )
    {
        super(tr, oid);
        assert(parent != null);
        value = _val;
        oidnext = oidnull;
        _parentlist = parent;
        oidparent = _parentlist == null ? oidnull : _parentlist.oid;
        _sf = tsf;
    }

    protected CDBLinkedList<E> parent() {
        assert(rlockheld());
        if (_parentlist == null) {
            assert (isDistributed);
            _parentlist = CDBLinkedList.findList(oidparent);
            _parentlist = (_parentlist != null) ? _parentlist : new CDBLinkedList<>(TR, _sf, oidparent);
        }
        return _parentlist;
    }

    public E applyReadValue(NodeOp<E> cc) {
        rlock();
        try {
            assert (oid == cc.nodeid());
            cc.setReturnValue(value);
        } finally {
            runlock();
        }
        return (E) cc.getReturnValue();
    }

    public long applyReadNext(NodeOp<E> cc) {
        rlock();
        try {
            assert (oid == cc.nodeid());
            cc.setReturnValue(oidnext);
        } finally {
            runlock();
        }
        return (long) cc.getReturnValue();
    }

    public void applyWriteValue(NodeOp<E> cc) {
        wlock();
        try {
            assert (oid == cc.nodeid());
            value = cc.e();
        } finally {
            wunlock();
        }
    }

    public void applyWriteNext(NodeOp<E> cc) {
        wlock();
        try {
            assert (oid == cc.nodeid());
            oidnext = cc.oidparam();
            assert(oidnext != oid);
        } finally {
            wunlock();
        }
    }

    public void applyToObject(Object bs, ITimestamp timestamp) {

        dbglog.debug("CDBMNode received upcall");
        NodeOp<E> cc = (NodeOp<E>) bs;
        switch (cc.cmd()) {
            case NodeOp.CMD_READ_VALUE: applyReadValue(cc); break;
            case NodeOp.CMD_READ_NEXT: applyReadNext(cc); break;
            case NodeOp.CMD_WRITE_VALUE: applyWriteValue(cc); break;
            case NodeOp.CMD_WRITE_NEXT: applyWriteNext(cc); break;
            case NodeOp.CMD_WRITE_PREV: throw new RuntimeException("invalid operation");
            case NodeOp.CMD_READ_PREV: throw new RuntimeException("invalid operation");
        }
    }
}

