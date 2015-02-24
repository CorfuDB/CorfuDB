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
        if(_parentlist == null) {
            assert (isDistributed);
            _parentlist = CDBList.findList(oidparent);
            _parentlist = (_parentlist != null) ? _parentlist : new CDBList<E>(TR, _sf, oidparent);
        }
        return _parentlist;
    }

    protected CDBNode<E> next() {
        if(oidnext == oidnull) return null;
        assert(oidnext != this.oid);
        if(_next == null)
            _next = parent().findNode(oidnext, null, isDistributed);
        return _next;
    }

    protected CDBNode<E> prev() {
        if(oidprev == oidnull) return null;
        assert(oidprev != this.oid);
        if(_prev == null)
            _prev = parent().findNode(oidprev, null, isDistributed);
        return _prev;
    }


    public void applyToObject(Object bs) {

        dbglog.debug("CDBNode received upcall");
        CDBNode<E> node = null;
        CDBNode<E> oprev = null;
        CDBNode<E> onext = null;
        long ooidnext = oidnull;
        long ooidprev = oidnull;
        NodeOp<E> cc = (NodeOp<E>) bs;

        switch (cc.cmd()) {
            case NodeOp.CMD_READ_VALUE:
                assert (oid == cc.nodeid());
                value = cc.e();
                cc.setReturnValue(value);
                break;
            case NodeOp.CMD_READ_NEXT:
                assert (oid == cc.nodeid());
                oidnext = cc.oidparam();
                cc.setReturnValue(next());
                break;
            case NodeOp.CMD_READ_PREV:
                assert (oid == cc.nodeid());
                oidprev = cc.oidparam();
                cc.setReturnValue(prev());
                break;
            case NodeOp.CMD_WRITE_VALUE:
                assert (oid == cc.nodeid());
                value = cc.e();
                node.value = value;
                break;
            case NodeOp.CMD_WRITE_NEXT:
                assert (oid == cc.nodeid());
                onext = _next;
                ooidnext = oidnext;
                oidnext = cc.oidparam();
                _next = next();
//                if(ooidnext == oidnull || _next == null || oidnext == oidnull) {
//                    // tail update required. Either we are:
//                    // a) changing the next pointer from null to
//                    //    something else--this implies (at least) that this node
//                    //    was the tail of the list. A change of the next pointer
//                    //    can result from append to the list (either single of sublist)
//                    //    or an idempotent write of the null value.
//                    // b) appending something with a null pointer
//                    // c) truncating the list
//                    long startnode = ooidnext == oidnull ? oidnext : ooidnext;
//                    parent().updateTail(startnode);
//                }
                break;
            case NodeOp.CMD_WRITE_PREV:
                assert (oid == cc.nodeid());
                ooidprev = oidprev;
                oprev = _prev;
                oidprev = cc.oidparam();
                _prev = prev();
//                if(ooidprev == oidnull || _prev == null || oidprev == oidnull) {
//                    long startnode = ooidprev == oidnull ? oidprev : ooidprev;
//                    parent().updateHead(startnode);
//                }
                break;
        }
    }
}

