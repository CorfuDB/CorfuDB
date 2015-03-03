package org.corfudb.runtime.collections;

import org.corfudb.runtime.AbstractRuntime;
import org.corfudb.runtime.CorfuDBObject;
import org.corfudb.runtime.DirectoryService;
import org.corfudb.runtime.StreamFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.function.UnaryOperator;


public class CDBNode<E> extends CorfuDBObject {

    public static final boolean isDistributed = false;
    public static final long oidnull = -1;
    static Logger dbglog = LoggerFactory.getLogger(CDBNode.class);

    public E value;
    public long oidnext;
    public long oidprev;
    public long oidparent;
    protected transient CDBList<E> _parentlist;
    protected transient CDBNode<E> _next;
    protected transient CDBNode<E> _prev;
    public transient StreamFactory _sf;

    void rlock() { lock(false); }
    void runlock() { unlock(false); }
    void wlock() { lock(true); }
    void wunlock() { unlock(true); }

    public CDBNode(
            AbstractRuntime tr,
            StreamFactory tsf,
            E _val,
            long oid,
            CDBList<E> parent
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

    protected CDBList<E> parent() {
        rlock();
        try {
            if (_parentlist == null) {
                assert (isDistributed);
                _parentlist = CDBList.findList(oidparent);
                _parentlist = (_parentlist != null) ? _parentlist : new CDBList<E>(TR, _sf, oidparent);
            }
            return _parentlist;
        } finally {
            runlock();
        }
    }

    protected CDBNode<E> next() {
        rlock();
        try {
            if (oidnext == oidnull) return null;
            assert (oidnext != this.oid);
            if (_next == null)
                _next = parent().findNode(oidnext, null, isDistributed);
            assert(this._next != this);
            return _next;
        } finally {
            runlock();
        }
    }

    protected CDBNode<E> prev() {
        rlock();
        try {
            if (oidprev == oidnull) return null;
            assert (oidprev != this.oid);
            if (_prev == null)
                _prev = parent().findNode(oidprev, null, isDistributed);
            return _prev;
        } finally {
            runlock();
        }
    }

    protected void applyReadValue(NodeOp<E> cc) {
        rlock();
        try {
            assert (oid == cc.nodeid());
            cc.setReturnValue(value);
        } finally {
            runlock();
        }
    }

    protected void applyReadNext(NodeOp<E> cc) {
        rlock();
        try {
            assert (oid == cc.nodeid());
            oidnext = cc.oidparam();
            cc.setReturnValue(next());
        } finally {
            runlock();
        }
    }

    protected void applyReadPrev(NodeOp<E> cc) {
        rlock();
        try {
            assert (oid == cc.nodeid());
            oidprev = cc.oidparam();
            cc.setReturnValue(prev());
        } finally {
            runlock();
        }
    }

    protected void applyWriteValue(NodeOp<E> cc) {
        wlock();
        try {
            assert (oid == cc.nodeid());
            value = cc.e();
        } finally {
            wunlock();
        }
    }

    protected void applyWriteNext(NodeOp<E> cc) {
        wlock();
        try {
            assert (oid == cc.nodeid());
            CDBNode<E> onext = _next;
            oidnext = cc.oidparam();
            _next = next();
        } finally {
            wunlock();
        }
    }

    protected void applyWritePrev(NodeOp<E> cc) {
        wlock();
        try {
            assert (oid == cc.nodeid());
            CDBNode<E> oprev = _prev;
            oidprev = cc.oidparam();
            _prev = prev();
        } finally {
            wunlock();
        }
    }


    public void applyToObject(Object bs) {

        dbglog.debug("CDBNode received upcall");
        CDBNode<E> node = this;
        CDBNode<E> oprev = null;
        CDBNode<E> onext = null;
        long ooidnext = oidnull;
        long ooidprev = oidnull;
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

