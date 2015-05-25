package org.corfudb.runtime.collections;

import org.corfudb.runtime.smr.legacy.AbstractRuntime;
import org.corfudb.runtime.smr.legacy.CorfuDBObject;
import org.corfudb.runtime.smr.IStreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.corfudb.runtime.stream.ITimestamp;

import java.util.UUID;

public class CDBDoublyLinkedListNode<E> extends CorfuDBObject {

    public static final boolean isDistributed = false;
    static Logger dbglog = LoggerFactory.getLogger(CDBDoublyLinkedListNode.class);

    public E value;
    public UUID oidnext;
    public UUID oidprev;
    public UUID oidparent;
    protected transient CDBDoublyLinkedList<E> _parentlist;
    public transient IStreamFactory _sf;

    public CDBDoublyLinkedListNode(
            AbstractRuntime tr,
            IStreamFactory tsf,
            E _val,
            UUID oid,
            CDBDoublyLinkedList<E> parent
        )
    {
        super(tr, oid);
        assert(parent != null);
        value = _val;
        oidnext = oidnull;
        oidprev = oidnull;
        _parentlist = parent;
        oidparent = _parentlist == null ? oidnull : _parentlist.oid;
        _sf = tsf;
    }

    protected CDBDoublyLinkedList<E> parent() {
        assert(rlockheld());
        if (_parentlist == null) {
            assert (isDistributed);
            _parentlist = CDBDoublyLinkedList.findList(oidparent);
            _parentlist = (_parentlist != null) ? _parentlist : new CDBDoublyLinkedList<>(TR, _sf, oidparent);
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

    public UUID applyReadNext(NodeOp<E> cc) {
        rlock();
        try {
            assert (oid == cc.nodeid());
            cc.setReturnValue(oidnext);
        } finally {
            runlock();
        }
        return (UUID) cc.getReturnValue();
    }

    public UUID applyReadPrev(NodeOp<E> cc) {
        rlock();
        try {
            assert (oid == cc.nodeid());
            cc.setReturnValue(oidprev);
        } finally {
            runlock();
        }
        return (UUID) cc.getReturnValue();
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

    protected void applyWritePrev(NodeOp<E> cc) {
        wlock();
        try {
            assert (oid == cc.nodeid());
            oidprev = cc.oidparam();
            assert(oidprev != oid);
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
            case NodeOp.CMD_READ_PREV: applyReadPrev(cc); break;
            case NodeOp.CMD_WRITE_VALUE: applyWriteValue(cc); break;
            case NodeOp.CMD_WRITE_NEXT: applyWriteNext(cc); break;
            case NodeOp.CMD_WRITE_PREV: applyWritePrev(cc); break;
        }
    }
}

