package org.corfudb.runtime.collections;

import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.HashMap;

import org.corfudb.runtime.smr.legacy.*;
import org.corfudb.runtime.stream.ITimestamp;
import org.corfudb.runtime.smr.*;

public class CDBPhysicalBTree<K extends Comparable<K>, V> extends CDBAbstractBTree<K, V> {

    public static boolean extremeDebug = false;

    /**
     * console logging for verbose mode.
     * @param strFormat
     * @param args
     */
    protected static void
    inform(
            String strFormat,
            Object... args
        )
    {
        if(extremeDebug)
            System.out.format(strFormat, args);
    }


    private UUID m_root;
    private int m_height;
    private int m_size;
    private HashMap<UUID, Entry> m_entries;
    private HashMap<UUID, Node> m_nodes;
    private PendingUpdates m_pending;

    private abstract static class PBTreeOp extends CorfuDBObjectCommand {

        private int m_cmd;
        private boolean m_mutator;
        private UUID m_utxid;
        private UUID m_oid;
        protected transient PBTreeOp m_prevupdate;
        protected boolean m_prevvalid;
        protected boolean m_markedpending;
        private UUID m_uuuid;
        public long m_reqstart;
        public long m_reqcomplete;

        public int cmd() { return m_cmd; }
        public boolean mutator() { return m_mutator; }
        public UUID oid() { return m_oid; }
        public long latency() { if(m_reqstart != 0 && m_reqcomplete != 0) return m_reqcomplete - m_reqstart; return 0; }
        public void start() { m_reqstart = System.currentTimeMillis(); }
        public void complete() { m_reqcomplete = System.currentTimeMillis(); }
        public static String cmdstr(int i) {
            if(NodeOp.isvalidcmd(i)) return NodeOp.cmdstring(i);
            if(EntryOp.isvalidcmd(i)) return EntryOp.cmdstring(i);
            if(BTreeOp.isvalidcmd(i)) return BTreeOp.cmdstring(i);
            return "CMD_INVALID";
        }

        /**
         * ctor
         * @param p
         * @param cmd
         * @param mutator
         * @param oid
         */
        public PBTreeOp(
            PendingUpdates p,
            int cmd,
            boolean mutator,
            UUID oid
            ) {
            m_cmd = cmd;
            m_mutator = mutator;
            m_utxid = p.m_tr.getTxid();
            m_oid = oid;
            m_prevupdate = null;
            m_prevvalid = false;
            m_uuuid = UUID.randomUUID();
            m_reqcomplete = 0;
            m_markedpending = false;
            start();
        }

        /**
         * return true if this is a read command
         * which conflicts with a previous uncommitted write.
         * @return
         */
        public boolean isReadAfterWrite(PendingUpdates p) {
            return getPreviousWrite(p) != null;
        }

        /**
         * return the last conflicting write that
         * should serve this read
         * @return
         */
        public PBTreeOp getPreviousWrite(PendingUpdates p) {
            if(m_mutator) return null;
            if(!m_prevvalid) {
                m_prevupdate = getPreviousWriteImpl(p);
                m_prevvalid = true;
            }
            return m_prevupdate;
        }

        /**
         * subclasses know which commands conflict
         * @return
         */
        public abstract PBTreeOp getPreviousWriteImpl(PendingUpdates p);
    }

    /**
     * class to track pending updates -- implements read after write consistency
     */
    private static class PendingUpdates {

        private ThreadLocal<UUID> m_lasttxid;
        private HashMap<UUID, HashMap<UUID, ArrayList<PBTreeOp>>> m_pending;
        private HashMap<UUID, ArrayList<PBTreeOp>> m_rpending;
        private HashMap<UUID, PBTreeOp> m_rpmap;
        private TXRuntime m_tr;
        private ReentrantReadWriteLock m_lock;
        private static final UUID m_notxid = UUID.randomUUID();

        /**
         * ctor
         */
        public PendingUpdates(AbstractRuntime tr) {
            m_tr = (TXRuntime) tr;
            m_lock = new ReentrantReadWriteLock();
            m_pending = new HashMap<UUID, HashMap<UUID, ArrayList<PBTreeOp>>>();
            m_rpending = new HashMap<UUID, ArrayList<PBTreeOp>>();
            m_rpmap = new HashMap<UUID, PBTreeOp>();
            m_lasttxid = new ThreadLocal<UUID>();
        }

        /**
         * get the last txid for this thread
         *
         * @return
         */
        private UUID lasttxid() {
            UUID local = m_lasttxid.get();
            if (local == null)
                return m_notxid;
            return local;
        }

        /**
         * check to see if we are in a new tx context
         */
        private void checknewtx() {
            UUID curtx = m_tr.getTxid();
            UUID lasttx = lasttxid();
            if(lasttx.equals(m_notxid) && curtx != null) {
                m_lasttxid.set(curtx);
            } else if(curtx != null && !curtx.equals(lasttx)) {
                m_lock.writeLock().lock();
                try {
                    CDBPhysicalBTree.inform("PendingUpdates: retire tx:%s?\n", lasttx.toString());
                    HashMap<UUID, ArrayList<PBTreeOp>> txcmds = m_pending.getOrDefault(lasttx, null);
                    if(txcmds == null) {
                        CDBPhysicalBTree.inform("XXXX why is there no pending list for tx:%s? RO?\n", lasttx.toString());
                    } else {
                        Set<UUID> keySet = txcmds.keySet();
                        for(UUID key : keySet) {
                            ArrayList<PBTreeOp> outstanding = txcmds.get(key);
                            if(!outstanding.isEmpty()) {
                                CDBPhysicalBTree.inform("XXXX unretired commands for Tx:%s, cob:%d:\n    [", lasttx, key);
                                boolean first = true;
                                for(PBTreeOp op : outstanding) {
                                    if(!first)
                                        CDBPhysicalBTree.inform(", ");
                                    CDBPhysicalBTree.inform(op.toString());
                                }
                                CDBPhysicalBTree.inform("]\n");
                            }
                        }
                        m_pending.remove(lasttx);
                    }
                    m_lasttxid.set(curtx);
                } finally {
                    m_lock.writeLock().unlock();
                }
            }
        }

        /**
         * return true if there are pending updates for this object
         *
         * @param cob
         * @return
         */
        public boolean hasPendingUpdates(UUID cob) {
            UUID curtx = m_tr.getTxid();
            if (curtx == null) return false;
            m_lock.readLock().lock();
            try {
                HashMap<UUID, ArrayList<PBTreeOp>> txcmds = m_pending.getOrDefault(curtx, null);
                if (txcmds == null || txcmds.isEmpty()) return false;
                return txcmds.containsKey(cob);
            } finally {
                m_lock.readLock().unlock();
            }
        }

        /**
         * return all pending updates for this object
         *
         * @param cob
         * @return
         */
        public ArrayList<PBTreeOp> getPendingUpdates(UUID cob) {
            UUID curtx = m_tr.getTxid();
            if (curtx == null) return null;
            checknewtx();
            m_lock.readLock().lock();
            try {
                HashMap<UUID, ArrayList<PBTreeOp>> txcmds = m_pending.getOrDefault(curtx, null);
                if (txcmds == null || txcmds.isEmpty()) return null;
                ArrayList<PBTreeOp> cobcmds = txcmds.getOrDefault(cob, null);
                if (cobcmds == null || cobcmds.isEmpty()) return null;
                return cobcmds;
            } finally {
                m_lock.readLock().unlock();
            }
        }

        /**
         * add a pending update for this object
         *
         * @param cob
         * @param cmd
         */
        public void addPendingUpdate(UUID cob, PBTreeOp cmd) {
            UUID curtx = m_tr.getTxid();
            if (curtx == null) return;
            cmd.m_markedpending = true;
            checknewtx();
            m_lock.writeLock().lock();
            try {
                HashMap<UUID, ArrayList<PBTreeOp>> txcmds = m_pending.getOrDefault(curtx, null);
                if (txcmds == null) {
                    txcmds = new HashMap<UUID, ArrayList<PBTreeOp>>();
                    m_pending.put(curtx, txcmds);
                }
                ArrayList<PBTreeOp> cobcmds = txcmds.getOrDefault(cob, null);
                if (cobcmds == null) {
                    cobcmds = new ArrayList<PBTreeOp>();
                    txcmds.put(cob, cobcmds);
                }
                cobcmds.add(cmd);
                m_rpending.put(cmd.m_uuuid, cobcmds);
                m_rpmap.put(cmd.m_uuuid, cmd);
            } finally {
                m_lock.writeLock().unlock();
            }
        }

        /**
         * retire a pending update
         *
         * @param cmd
         */
        public void retirePendingUpdate(PBTreeOp cmd) {
            if(!cmd.mutator() || !cmd.m_markedpending)
                return;
            m_lock.writeLock().lock();
            try {
                ArrayList<PBTreeOp> cmds = m_rpending.getOrDefault(cmd.m_uuuid, null);
                PBTreeOp cpcmd = m_rpmap.getOrDefault(cmd.m_uuuid, null);
                if(cmds == null || cpcmd == null) {
                    CDBPhysicalBTree.inform("failed to retire %s (not found)!\n", cmd);
                } else {
                    m_rpending.remove(cpcmd);
                }
            } finally {
                m_lock.writeLock().unlock();
            }
        }

        /**
         * find commands that match the given object and command type
         *
         * @param cob
         * @param cmd
         * @return
         */
        public ArrayList<PBTreeOp> match(UUID cob, int cmd) {
            UUID curtx = m_tr.getTxid();
            if (curtx == null) return null;
            m_lock.readLock().lock();
            try {
                HashMap<UUID, ArrayList<PBTreeOp>> txcmds = m_pending.getOrDefault(curtx, null);
                if (txcmds == null) return null;
                ArrayList<PBTreeOp> cobcmds = txcmds.getOrDefault(cob, null);
                if (cobcmds == null || cobcmds.isEmpty()) return null;
                ArrayList<PBTreeOp> matching = null;
                Iterator it = cobcmds.iterator();
                while (it.hasNext()) {
                    PBTreeOp op = (PBTreeOp) it.next();
                    if (op.cmd() == cmd) {
                        if (matching == null)
                            matching = new ArrayList<PBTreeOp>();
                        matching.add(op);
                    }
                }
                return matching;
            } finally {
                m_lock.readLock().unlock();
            }
        }
    }

    private static class NodeOp extends PBTreeOp {

        static final int CMD_READ_CHILD_COUNT = 1;
        static final int CMD_READ_CHILD = 2;
        static final int CMD_WRITE_CHILD_COUNT = 3;
        static final int CMD_WRITE_CHILD = 4;
        public static boolean isvalidcmd(int i) { return i>=CMD_READ_CHILD_COUNT && i<= CMD_WRITE_CHILD; }
        public static String cmdstring(int i) {
            switch(i) {
                case CMD_READ_CHILD_COUNT:
                    return "CMD_READ_CHILD_COUNT";
                case CMD_READ_CHILD:
                    return "CMD_READ_CHILD";
                case CMD_WRITE_CHILD_COUNT:
                    return "CMD_WRITE_CHILD_COUNT";
                case CMD_WRITE_CHILD:
                    return "CMD_WRITE_CHILD";
                default:
                    return "CMD_INVALID";
            }
        }

        public int m_childindex;
        public int m_childcount;
        public UUID m_oidparam;
        public int childindex() { return m_childindex; }
        public int childcount() { return m_childcount; }
        public UUID oidparam() { return m_oidparam; }

        /**
         * ctor
         * @param p
         * @param _cmd
         * @param _mutator
         * @param _oid
         * @param _index
         * @param _num
         * @param _oidparam
         */
        public
        NodeOp( PendingUpdates p,
                int _cmd,
                boolean _mutator,
                UUID _oid,
                int _index,
                int _num,
                UUID _oidparam
            )
        {
            super(p, _cmd, _mutator, _oid);
            m_childindex = _index;
            m_childcount = _num;
            m_oidparam = _oidparam;
            if(!mutator())
                getPreviousWrite(p);
        }

        /**
         * toString
         * @return
         */
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("NodeOp[n:");
            sb.append(oid());
            sb.append("]: ");
            switch(cmd()) {
                case CMD_READ_CHILD:
                    sb.append("r-ch(");
                    sb.append(childindex());
                    sb.append(")");
                    break;
                case CMD_READ_CHILD_COUNT:
                    sb.append("r-num-ch");
                    break;
                case CMD_WRITE_CHILD:
                    sb.append("w-ch(");
                    sb.append(childindex());
                    sb.append("=>");
                    sb.append(oidparam());
                    break;
                case CMD_WRITE_CHILD_COUNT:
                    sb.append("w-num-ch(");
                    sb.append(childcount());
                    sb.append(")");
                    break;
            }
            if(m_prevvalid) {
                sb.append(" prevconf: ");
                sb.append(m_prevupdate);
            }
            return sb.toString();
        }


        public static NodeOp readChildCountCmd(PendingUpdates p, UUID _oidnode) { return new NodeOp(p, CMD_READ_CHILD_COUNT, false, _oidnode, 0, 0, oidnull); }
        public static NodeOp readChildCmd(PendingUpdates p, UUID _oidnode, int idx) { return new NodeOp(p, CMD_READ_CHILD, false, _oidnode, idx, 0, oidnull); }
        public static NodeOp writeChildCountCmd(PendingUpdates p, UUID _oidnode, int _count) {
            NodeOp cmd = new NodeOp(p, CMD_WRITE_CHILD_COUNT, true, _oidnode, 0, _count, oidnull);
            p.addPendingUpdate(_oidnode, cmd);
            return cmd;
        }
        public static NodeOp writeChildCmd(PendingUpdates p, UUID _oidnode, int _index, UUID _oidparam) {
            NodeOp cmd = new NodeOp(p, CMD_WRITE_CHILD, true, _oidnode, _index, 0, _oidparam);
            p.addPendingUpdate(_oidnode, cmd);
            return cmd;
        }

        /**
         * look for previous writes that conflict with this read
         * @return
         */
        public PBTreeOp getPreviousWriteImpl(PendingUpdates p) {
            if (mutator()) return null;
            ArrayList<PBTreeOp> m = null;
            switch (cmd()) {
                case CMD_READ_CHILD_COUNT:
                    m = p.match(oid(), CMD_WRITE_CHILD_COUNT);
                    break;
                case CMD_READ_CHILD:
                    m = p.match(oid(), CMD_WRITE_CHILD);
                    if(m != null) {
                        ArrayList<PBTreeOp> newM = new ArrayList<PBTreeOp>();
                        for (PBTreeOp op : m) {
                            if(m_childindex == ((NodeOp)op).m_childindex)
                                newM.add(op);
                        }
                        m = newM;
                    }
                    break;
                default:
                    break;
            }
            if (m != null && !m.isEmpty())
                return m.get(m.size()-1);
            return null;
        }
    }

    private static class Node<K extends Comparable<K>, V> extends CorfuDBObject {

        private int m_nChildren;
        private UUID[] m_vChildren;
        transient PendingUpdates m_pending;

        /**
         * ctor
         * @param tr
         * @param toid
         * @param nChildren
         */
        private Node(
            AbstractRuntime tr,
            UUID toid,
            int nChildren,
            PendingUpdates pending
            )
        {
            super(tr, toid);
            m_vChildren = new UUID[M];
            m_nChildren = nChildren;
            m_pending = pending;
        }

        /**
         * corfu runtime upcall
         * @param bs
         * @param timestamp
         */
        public void
        applyToObject(Object bs, ITimestamp timestamp) {

            NodeOp cc = (NodeOp) bs;
            if(cc.mutator())
                CDBPhysicalBTree.inform("APPLY: " + cc);
            switch (cc.cmd()) {
                case NodeOp.CMD_READ_CHILD_COUNT: cc.setReturnValue(applyReadChildCount()); break;
                case NodeOp.CMD_WRITE_CHILD_COUNT: applyWriteChildCount(cc.childcount()); break;
                case NodeOp.CMD_READ_CHILD: cc.setReturnValue(applyReadChild(cc.childindex())); break;
                case NodeOp.CMD_WRITE_CHILD: applyWriteChild(cc.childindex(), cc.oidparam()); break;
            }
            cc.complete();
            m_pending.retirePendingUpdate(cc);
        }

        /**
         * read the child count
         * @return the number of children in the given node
         */
        public int
        applyReadChildCount() {
            rlock();
            try {
                return m_nChildren;
            } finally {
                runlock();
            }
        }

        /**
         * write the child count
         * @param n
         */
        public void
        applyWriteChildCount(int n) {
            wlock();
            try {
                m_nChildren = n;
            } finally {
                wunlock();
            }
        }

        /**
         * getChild
         * @param index
         * @return
         */
        protected UUID getChild(int index) {
            if (index < 0 || index >= M)
                return oidnull;
            return m_vChildren[index];
        }

        /**
         * apply an indexed read child operation
         * @param index
         * @return
         */
        public UUID
        applyReadChild(int index) {
            rlock();
            try {
                return getChild(index);
            } finally {
                runlock();
            }
        }

        /**
         * apply a write child operation
         * @param n
         * @param _oid
         */
        public void
        applyWriteChild(int n, UUID _oid) {
            wlock();
            try {
                if(n>=0 && n<M)
                    m_vChildren[n] = _oid;
            } finally {
                wunlock();
            }
        }

        /**
         * toString
         * @return
         */
        @Override
        public String toString() {
            rlock();
            try {
                StringBuilder sb = new StringBuilder();
                sb.append("N");
                sb.append(oid);
                boolean first = true;
                for(int i=0; i<m_nChildren; i++) {
                    boolean last = i==m_nChildren-1;
                    if(first) {
                        sb.append("[");
                    } else {
                        sb.append(", ");
                    }
                    sb.append("c");
                    sb.append(i);
                    sb.append("=");
                    sb.append(getChild(i));
                    if(last) sb.append("]");
                    first = false;
                }
                return sb.toString();
            } finally {
                runlock();
            }
        }

    }

    private static class EntryOp<K extends Comparable<K>, V> extends PBTreeOp {

        static final int CMD_READ_KEY = 101;
        static final int CMD_WRITE_KEY = 201;
        static final int CMD_READ_VALUE = 301;
        static final int CMD_WRITE_VALUE = 401;
        static final int CMD_READ_NEXT = 501;
        static final int CMD_WRITE_NEXT = 601;
        static final int CMD_READ_DELETED = 701;
        static final int CMD_WRITE_DELETED = 801;
        transient static HashMap<Integer, String> s_cmds = new HashMap();
        static {
            s_cmds.put(CMD_READ_KEY, "CMD_READ_KEY");
            s_cmds.put(CMD_WRITE_KEY, "CMD_READ_KEY");
            s_cmds.put(CMD_READ_VALUE, "CMD_READ_VALUE");
            s_cmds.put(CMD_WRITE_VALUE, "CMD_WRITE_VALUE");
            s_cmds.put(CMD_READ_NEXT, "CMD_READ_NEXT");
            s_cmds.put(CMD_WRITE_NEXT, "CMD_WRITE_NEXT");
            s_cmds.put(CMD_READ_DELETED, "CMD_READ_DELETED");
            s_cmds.put(CMD_WRITE_DELETED, "CMD_WRITE_DELETED");
        }
        public static boolean isvalidcmd(int i) { return s_cmds.containsKey(new Integer(i)); }
        public static String cmdstring(int i) { return s_cmds.getOrDefault(new Integer(i), "CMD_INVALID"); }

        public K m_key;
        public V m_value;
        public UUID m_oidnext;
        public boolean m_deleted;
        public K key() { return m_key; }
        public V value() { return m_value; }
        public UUID oidnext() { return m_oidnext; }
        public boolean deleted() { return m_deleted; }

        /**
         * toString
         * @return
         */
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("EntryOp[E");
            sb.append(oid());
            sb.append("] ");
            switch(cmd()) {
                case CMD_READ_KEY: sb.append("r-key"); break;
                case CMD_READ_VALUE: sb.append("r-val"); break;
                case CMD_READ_NEXT: sb.append("r-next"); break;
                case CMD_READ_DELETED: sb.append("r-deleted"); break;
                case CMD_WRITE_KEY:
                    sb.append("w-key(");
                    sb.append(key());
                    sb.append(")");
                    break;
                case CMD_WRITE_VALUE:
                    sb.append("w-val(");
                    sb.append(value());
                    sb.append(")");
                    break;
                case CMD_WRITE_NEXT:
                    sb.append("w-next(");
                    sb.append(oidnext());
                    sb.append(")");
                    break;
                case CMD_WRITE_DELETED:
                    sb.append("w-deleted(");
                    sb.append(deleted());
                    sb.append(")");
                    break;
            }
            if(m_prevvalid) {
                sb.append(" prevconf: ");
                sb.append(m_prevupdate);
            }
            return sb.toString();
        }

        /**
         * ctor
         * @param _cmd
         * @param _oid
         * @param key
         * @param value
         * @param oidnext
         * @param deleted
         */
        private
        EntryOp(PendingUpdates p,
                int _cmd,
                boolean _mutator,
                UUID _oid,
                K key,
                V value,
                UUID oidnext,
                boolean deleted
            )
        {
            super(p, _cmd, _mutator, _oid);
            m_key = key;
            m_value = value;
            m_oidnext = oidnext;
            m_deleted = deleted;
            if(!_mutator)
                getPreviousWrite(p);
        }

        public static <K extends Comparable<K>, V> EntryOp<K, V> readKeyCmd(PendingUpdates p, UUID _oid) { return new EntryOp<K, V>(p, CMD_READ_KEY, false, _oid, null, null, oidnull, false); }
        public static <K extends Comparable<K>, V> EntryOp<K, V> readValueCmd(PendingUpdates p,UUID _oid) { return new EntryOp<K, V>(p, CMD_READ_VALUE, false, _oid, null, null, oidnull, false); }
        public static <K extends Comparable<K>, V> EntryOp<K, V> readNextCmd(PendingUpdates p, UUID _oid) { return new EntryOp<K, V>(p, CMD_READ_NEXT, false, _oid, null, null, oidnull, false); }
        public static <K extends Comparable<K>, V> EntryOp<K, V> readDeletedCmd(PendingUpdates p, UUID _oid) { return new EntryOp<K, V>(p, CMD_READ_DELETED, false, _oid, null, null, oidnull, false); }

        public static <K extends Comparable<K>, V> EntryOp<K, V> writeKeyCmd(PendingUpdates p, UUID _oid, K _key) {
            EntryOp<K,V> cmd = new EntryOp<K, V>(p, CMD_WRITE_KEY, true, _oid, _key, null, oidnull, false);
            p.addPendingUpdate(_oid, cmd);
            return cmd;
        }

        public static <K extends Comparable<K>, V> EntryOp<K, V> writeValueCmd(PendingUpdates p, UUID _oid, V _value) {
            EntryOp<K, V> cmd = new EntryOp<K, V>(p, CMD_WRITE_VALUE, true, _oid, null, _value, oidnull, false);
            p.addPendingUpdate(_oid, cmd);
            return cmd;
        }

        public static <K extends Comparable<K>, V> EntryOp<K, V> writeNextCmd(PendingUpdates p, UUID _oid, UUID _oidnext) {
            EntryOp<K, V> cmd = new EntryOp<K, V>(p, CMD_WRITE_NEXT, true, _oid, null, null, _oidnext, false);
            p.addPendingUpdate(_oid, cmd);
            return cmd;
        }

        public static <K extends Comparable<K>, V> EntryOp<K, V> writeDeletedCmd(PendingUpdates p, UUID _oid, boolean b) {
            EntryOp<K, V> cmd = new EntryOp<K, V>(p, CMD_WRITE_DELETED, true, _oid, null, null, oidnull, b);
            p.addPendingUpdate(_oid, cmd);
            return cmd;
        }

        /**
         * look for previous writes that conflict with this read
         * @return
         */
        public PBTreeOp getPreviousWriteImpl(PendingUpdates p) {
            if (mutator()) return null;
            ArrayList<PBTreeOp> m = null;
            switch (cmd()) {
                case CMD_READ_KEY:
                    m = p.match(oid(), CMD_WRITE_KEY);
                    break;
                case CMD_READ_VALUE:
                    m = p.match(oid(), CMD_WRITE_VALUE);
                    break;
                case CMD_READ_NEXT:
                    m = p.match(oid(), CMD_WRITE_NEXT);
                    break;
                case CMD_READ_DELETED:
                    m = p.match(oid(), CMD_WRITE_DELETED);
                    break;
                default:
                    break;
            }
            if (m != null)
                return m.get(0);
            return null;
        }
    }

    private static class Entry<K extends Comparable<K>, V> extends CorfuDBObject {

        private Comparable key;
        private V value;
        private UUID oidnext;
        private boolean deleted;
        private transient PendingUpdates m_pending;

        /**
         * ctor
         * @param tr
         * @param toid
         * @param _key
         * @param _value
         * @param _next
         */
        public Entry(
                AbstractRuntime tr,
                UUID toid,
                K _key,
                V _value,
                UUID _next,
                PendingUpdates pending
            ) {
            super(tr, toid);
            key = _key;
            value = _value;
            oidnext = _next;
            deleted = false;
            m_pending = pending;
        }

        /**
         * corfu runtime upcall
         * @param bs
         * @param timestamp
         */
        public void
        applyToObject(Object bs, ITimestamp timestamp) {

            EntryOp<K,V> cc = (EntryOp<K,V>) bs;
            if(cc.mutator())
                CDBPhysicalBTree.inform("APPLY: " + cc);
            switch (cc.cmd()) {
                case EntryOp.CMD_READ_KEY: cc.setReturnValue(applyReadKey()); break;
                case EntryOp.CMD_WRITE_KEY: cc.setReturnValue(applyWriteKey(cc.key())); break;
                case EntryOp.CMD_READ_VALUE: cc.setReturnValue(applyReadValue()); break;
                case EntryOp.CMD_WRITE_VALUE: cc.setReturnValue(applyWriteValue(cc.value())); break;
                case EntryOp.CMD_READ_NEXT: cc.setReturnValue(applyReadNext()); break;
                case EntryOp.CMD_WRITE_NEXT: cc.setReturnValue(applyWriteNext(cc.oidnext())); break;
                case EntryOp.CMD_READ_DELETED: cc.setReturnValue(applyReadDeleted()); break;
                case EntryOp.CMD_WRITE_DELETED: cc.setReturnValue(applyWriteDeleted(cc.deleted())); break;
            }
            cc.complete();
            m_pending.retirePendingUpdate(cc);
        }

        /**
         * read the deleted flag on this entry
         * @return
         */
        public Boolean applyReadDeleted() {
            rlock();
            try {
                return deleted;
            } finally {
                runlock();
            }
        }

        /**
         * apply a delete command
         * @param b
         * @return
         */
        public Boolean applyWriteDeleted(boolean b) {
            wlock();
            try {
                Boolean oval = deleted;
                deleted = b;
                return oval;
            } finally {
                wunlock();
            }
        }

        /**
         * read the key
         * @return
         */
        public K applyReadKey() {
            rlock();
            try {
                return (K)key;
            } finally {
                runlock();
            }
        }

        /**
         * write the key
         * @param k
         * @return
         */
        public K applyWriteKey(K k) {
            wlock();
            try {
                key = k;
            } finally {
                wunlock();
            }
            return k;
        }

        /**
         * read the value
         * @return
         */
        public V applyReadValue() {
            rlock();
            try {
                return value;
            } finally {
                runlock();
            }
        }

        /**
         * write the value
         * @param v
         * @return
         */
        public V applyWriteValue(V v) {
            wlock();
            try {
                value = v;
            } finally {
                wunlock();
            }
            return v;
        }

        /**
         * read the next pointer
         * @return
         */
        public UUID applyReadNext() {
            rlock();
            try {
                return oidnext;
            } finally {
                runlock();
            }
        }

        /**
         * write the next pointer
         * @param _next
         * @return
         */
        public UUID applyWriteNext(UUID _next) {
            wlock();
            try {
                oidnext = _next;
            } finally {
                wunlock();
            }
            return oidnext;
        }

        /**
         * toString
         * @return
         */
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if(deleted)
                sb.append("DEL: ");
            sb.append("E");
            sb.append(oid);
            sb.append(":[k=");
            sb.append(key);
            sb.append(", v=");
            sb.append(value);
            sb.append(", n=");
            sb.append(oidnext);
            sb.append("]");
            return sb.toString();
        }

    }

    public static class BTreeOp extends PBTreeOp {

        static final int CMD_READ_SIZE = 771;
        static final int CMD_READ_HEIGHT = 772;
        static final int CMD_READ_ROOT = 773;
        static final int CMD_WRITE_SIZE = 774;
        static final int CMD_WRITE_HEIGHT = 775;
        static final int CMD_WRITE_ROOT = 776;
        public static boolean isvalidcmd(int i) { return i>=CMD_READ_SIZE && i<= CMD_WRITE_ROOT; }
        public static String cmdstring(int i) {
            switch(i) {
                case CMD_READ_SIZE:
                    return "CMD_READ_SIZE";
                case CMD_READ_HEIGHT:
                    return "CMD_READ_HEIGHT";
                case CMD_READ_ROOT:
                    return "CMD_READ_ROOT";
                case CMD_WRITE_SIZE:
                    return "CMD_WRITE_SIZE";
                case CMD_WRITE_HEIGHT:
                    return "CMD_WRITE_HEIGHT";
                case CMD_WRITE_ROOT:
                    return "CMD_WRITE_ROOT";
                default:
                    return "CMD_INVALID";
            }
        }

        public int m_iparam;
        public UUID m_oidparam;
        public int iparam() { return m_iparam; }
        public UUID oidparam() { return m_oidparam; }

        /**
         * toString
         * @return
         */
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("BTreeOp[T");
            sb.append(oid());
            sb.append("]: ");
            switch(cmd()) {
                case CMD_READ_SIZE: sb.append("r-size"); break;
                case CMD_READ_HEIGHT: sb.append("r-height"); break;
                case CMD_READ_ROOT: sb.append("r-root"); break;
                case CMD_WRITE_SIZE:
                    sb.append("w-size(");
                    sb.append(iparam());
                    sb.append(")");
                    break;
                case CMD_WRITE_HEIGHT:
                    sb.append("w-height(");
                    sb.append(iparam());
                    sb.append(")");
                    break;
                case CMD_WRITE_ROOT:
                    sb.append("w-root(");
                    sb.append(oidparam());
                    sb.append(")");
                    break;
            }
            if(m_prevvalid) {
                sb.append(" prevconf: ");
                sb.append(m_prevupdate);
            }
            return sb.toString();
        }


        /**
         * ctor
         * @param _cmd
         * @param _oid
         * @param _iparam
         * @param _oidparam
         */
        private
        BTreeOp(PendingUpdates p,
                int _cmd,
                boolean _mutator,
                UUID _oid,
                int _iparam,
                UUID _oidparam
            )
        {
            super(p, _cmd, _mutator, _oid);
            m_iparam = _iparam;
            m_oidparam = _oidparam;
            if(!_mutator)
                getPreviousWrite(p);
        }

        public static BTreeOp readSizeCmd(PendingUpdates p, UUID _oid) { return new BTreeOp(p, CMD_READ_SIZE, false, _oid, 0, oidnull); }
        public static BTreeOp readHeightCmd(PendingUpdates p, UUID _oid) { return new BTreeOp(p, CMD_READ_HEIGHT, false, _oid, 0, oidnull); }
        public static BTreeOp readRootCmd(PendingUpdates p, UUID _oid) { return new BTreeOp(p, CMD_READ_ROOT, false, _oid, 0, oidnull); }
        public static BTreeOp writeSizeCmd(PendingUpdates p, UUID _oid, int i) {
            BTreeOp cmd = new BTreeOp(p, CMD_WRITE_SIZE, true, _oid, i, oidnull);
            p.addPendingUpdate(_oid, cmd);
            return cmd;
        }
        public static BTreeOp writeHeightCmd(PendingUpdates p, UUID _oid, int i) {
            BTreeOp cmd = new BTreeOp(p, CMD_WRITE_HEIGHT, true, _oid, i, oidnull);
            p.addPendingUpdate(_oid, cmd);
            return cmd;
        }
        public static BTreeOp writeRootCmd(PendingUpdates p, UUID _oid, UUID _param) {
            BTreeOp cmd = new BTreeOp(p, CMD_WRITE_ROOT, true, _oid, 0, _param);
            p.addPendingUpdate(_oid, cmd);
            return cmd;
        }

        /**
         * look for previous writes that conflict with this read
         * @return
         */
        public PBTreeOp getPreviousWriteImpl(PendingUpdates p) {
            if (mutator()) return null;
            ArrayList<PBTreeOp> m = null;
            switch (cmd()) {
                case CMD_READ_ROOT:
                    m = p.match(oid(), CMD_WRITE_ROOT);
                    break;
                case CMD_READ_SIZE:
                    m = p.match(oid(), CMD_WRITE_SIZE);
                    break;
                case CMD_READ_HEIGHT:
                    m = p.match(oid(), CMD_WRITE_HEIGHT);
                    break;
                default:
                    break;
            }
            if (m != null)
                return m.get(0);
            return null;
        }
    }

    /**
     * if we are collecting latency
     * statistics, note the start time for this
     * request
     * @param _op
     */
    @Override
    protected void startRequestImpl(CorfuDBObjectCommand _op) {
        PBTreeOp op = (PBTreeOp)_op;
        if(op == null) return;
        op.start();
    }

    /**
     * stream completion time. return true if
     * we actually completed this request through the
     * normal channels...
     * @param _op
     * @return
     */
    @Override
    protected boolean completeRequestImpl(CorfuDBObjectCommand _op) {
        PBTreeOp op = (PBTreeOp)_op;
        if(op == null) return false;
        op.complete();
        return op.latency() != 0;
    }

    /**
     * get the latency stats as a string
     * @param ops
     * @return
     */
    @Override
    protected String
    getLatencyStatsImpl(
            Collection<CorfuDBObjectCommand> ops
        ) {
        HashMap<Integer, ArrayList<PBTreeOp>> map = new HashMap();
        for(CorfuDBObjectCommand _op : ops) {
            PBTreeOp op = (PBTreeOp)_op;
            if(op == null) continue;
            ArrayList<PBTreeOp> list = map.getOrDefault(new Integer(op.cmd()), new ArrayList());
            list.add(op);
            map.put(op.cmd(), list);
        }
        StringBuilder sb = new StringBuilder();
        for(Integer cmd : map.keySet()) {
            sb.append("FSCMDLAT, ");
            sb.append(PBTreeOp.cmdstr(cmd));
            ArrayList<PBTreeOp> list = map.get(cmd);
            for(PBTreeOp op : list) {
                sb.append(", ");
                sb.append(op.latency());
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * ctor
     * @param tTR
     * @param tsf
     * @param toid
     */
    public CDBPhysicalBTree(
            AbstractRuntime tTR,
            IStreamFactory tsf,
            UUID toid
        )
    {
        super(tTR, tsf, toid);
        m_entries = new HashMap<UUID, Entry>();
        m_nodes = new HashMap<UUID, Node>();
        m_root = oidnull;
        m_size = 0;
        m_height = 0;
        m_pending = new PendingUpdates((TXRuntime)tTR);
        tTR.BeginTX();
        Node oRoot = allocNode(0);
        writeroot(oRoot.oid);
        if(!tTR.EndTX())
            throw new RuntimeException("CDBPhysicalBTree constructor failed!");
        CDBPhysicalBTree.inform("created pbtree.oid=%d root-node=%d, m_root=%d\n", toid, oRoot.oid, m_root);
    }

    /**
     * corfu runtime upcall
     * @param bs
     * @param timestamp
     */
    public void
    applyToObject(Object bs, ITimestamp timestamp) {

        BTreeOp cc = (BTreeOp) bs;
        if(cc.mutator())
            CDBPhysicalBTree.inform("APPLY: %s\n", cc.toString());
        switch (cc.cmd()) {
            case BTreeOp.CMD_READ_HEIGHT: cc.setReturnValue(applyReadHeight()); break;
            case BTreeOp.CMD_WRITE_HEIGHT: applyWriteHeight(cc.iparam()); break;
            case BTreeOp.CMD_READ_ROOT: cc.setReturnValue(applyReadRoot()); break;
            case BTreeOp.CMD_WRITE_ROOT:  applyWriteRoot(cc.oidparam());  break;
            case BTreeOp.CMD_READ_SIZE: cc.setReturnValue(applyReadSize()); break;
            case BTreeOp.CMD_WRITE_SIZE: applyWriteSize(cc.iparam()); break;
        }
        cc.complete();
        m_pending.retirePendingUpdate(cc);
    }

    /**
     * print the current view (consistent or otherwise)
     * @return
     */
    public String printview() { return printview(nodeById(m_root), m_height, "") + "\n"; }

    /**
     * printview helper function
     * @param node
     * @param height
     * @param indent
     * @return
     */
    private String
    printview(Node<K, V> node, int height, String indent) {
        if(node == null) return "";
        StringBuilder sb = new StringBuilder();
        int nChildren = node.m_nChildren;
        if(height == 0) {
            for(int i=0; i<nChildren; i++) {
                Entry child = entryById(node.getChild(i));
                if(child == null) {
                    sb.append("OIDNULL");
                } else {
                    if(child.deleted)
                        sb.append("DEL: ");
                    sb.append(indent);
                    sb.append(child.key);
                    sb.append(" ");
                    sb.append(child.value);
                    sb.append("\n");
                }
            }
        } else {
            for(int i=0; i<nChildren; i++) {
                if(i>0) {
                    sb.append(indent);
                    sb.append("(");
                    sb.append(entryById(node.getChild(i)).key);
                    sb.append(")\n");
                }
                Entry<K,V> echild = entryById(node.getChild(i));
                if(echild == null) {
                    sb.append("null-child-entry");
                } else {
                    Node next = nodeById(echild.oidnext);
                    if (next == null) {
                        sb.append("null-child-next");
                    } else {
                        sb.append(printview(next, height - 1, indent + "    "));
                    }
                }
            }
        }
        return sb.toString();
    }

    /**
     * print the b-tree
     * @return
     */
    public String print() {
        return print(readroot(), readheight(), "") + "\n";
    }

    /**
     * print helper function
     * @param node
     * @param height
     * @param indent
     * @return
     */
    private String
    print(Node<K, V> node, int height, String indent) {
        if(node == null) return "";
        StringBuilder sb = new StringBuilder();
        int nChildren = readchildcount(node);
        if(height == 0) {
            for(int i=0; i<nChildren; i++) {
                Entry child = entryById(readchild(node, i));
                boolean deleted = readdeleted(child);
                if(deleted)
                    sb.append("DEL: ");
                sb.append(indent);
                sb.append(readkey(child));
                sb.append(" ");
                sb.append(readvalue(child));
                sb.append("\n");
            }
        } else {
            for(int i=0; i<nChildren; i++) {
                if(i>0) {
                    sb.append(indent);
                    sb.append("(");
                    sb.append(readkey(readchild(node, i)));
                    sb.append(")\n");
                }
                Node next = nodeById(readnext(readchild(node, i)));
                sb.append(print(next, height-1, indent + "    "));
            }
        }
        return sb.toString();
    }

    /**
     * return the size of the tree
     * @return
     */
    @Override
    public int size() {
        return readsize();
    }

    /**
     * return the height of the btree.
     * pray for a correct answer.
     * @return
     */
    public int height() {
        return readheight();
    }

    /**
     * maps. they rock.
     * @param key
     * @return
     */
    public V get(K key) {
        if(key == null)
            return null;
        UUID root = readrootoid();
        int height = readheight();
        Entry entry = searchEntry(root, key, height);
        if(entry != null) {
            boolean deleted = readdeleted(entry);
            if(!deleted) {
                V result = (V) readvalue(entry);
                return result;
            }
        }
        return null;
    }

    /**
     *
     * @param key
     * @return
     */
    public V remove(K key) {
        if(key == null)
            return null;
        V result = null;
        UUID root = readrootoid();
        int height = readheight();
        Entry entry = searchEntry(root, key, height);
        if(entry != null) {
            boolean deleted = readdeleted(entry);
            if(!deleted) {
                result = (V) readvalue(entry);
                writedeleted(entry, true);
                int size = readsize();
                writesize(size-1);
                return result;
            }
        }
        return result;
    }

    /**
     * update the value at the given key
     * @param key
     * @param value
     * @return
     */
    public boolean update(K key, V value) {
        if(key == null)
            return false;
        UUID root = readrootoid();
        int height = readheight();
        Entry entry = searchEntry(root, key, height);
        if(entry != null) {
            boolean deleted = readdeleted(entry);
            if(!deleted) {
                V oval = (V) readvalue(entry);
                writevalue(entry, value);
                return true;
            }
        }
        return false;
    }


    /**
     * clear the tree
     */
    public void clear() {
        UUID root = readrootoid();
        writeroot(oidnull);
        writesize(0);
        writeheight(0);
    }


    /**
     * maps. was their rocking-ness mentioned?
     * @param key
     * @param value
     */
    public V
    put(K key, V value) {
        V result = null;
        if(key == null)
            return result;
        UUID root = readrootoid();
        int height = readheight();
        Entry entry = searchEntry(root, key, height);
        if(entry != null) {
            boolean deleted = readdeleted(entry);
            if(!deleted) {
                result = (V) readvalue(entry);
            } else {
                writedeleted(entry, false);
            }
            writevalue(entry, value);
        } else {
            int size = readsize();
            UUID unodeoid = insert(root, key, value, height);
            writesize(size + 1);
            if (unodeoid != oidnull) {
                // split required
                Node t = allocNode(2);
                UUID rootchild0 = readchild(nodeById(root), 0);
                UUID uchild0 = readchild(nodeById(unodeoid), 0);
                Comparable r0key = readkey(entryById(rootchild0));
                Comparable u0key = readkey(entryById(uchild0));
                Entry tc0 = allocEntry((K) r0key, null);
                Entry tc1 = allocEntry((K) u0key, null);
                writechild(t, 0, tc0.oid);
                writechild(t, 1, tc1.oid);
                writenext(tc0, root);
                writenext(tc1, unodeoid);
                writeroot(t.oid);
                writeheight(height + 1);
            }
        }
        return result;
    }

    /**
     * search for a key starting at the given
     * node and height
     * @param oidnode
     * @param key
     * @param height
     * @return
     */
    private V
    search(
        UUID oidnode,
        K key,
        int height
        )
    {
        Node<K,V> node = nodeById(oidnode);
        int nChildren = readchildcount(node);

        if(height == 0) {
            // external node
            for(int i=0; i<nChildren; i++) {
                UUID oidchild = readchild(node, i);
                Entry child = entryById(oidchild);
                Comparable ckey = readkey(child);
                if(eq(key, ckey))
                    return (V) readvalue(child);
            }
        } else {
            // internal node
            for(int i=0; i<nChildren; i++) {
                if(i+1 == nChildren || lt(key, readkey(readchild(node, i+1)))) {
                    return search(readnext(readchild(node, i)), key, height-1);
                }
            }
        }
        return null;
    }

    /**
     * search for a key starting at the given
     * node and height
     * @param oidnode
     * @param key
     * @param height
     * @return
     */
    private Entry
    searchEntry(
        UUID oidnode,
        K key,
        int height
        ) {

        if(oidnode == oidnull)
            return null;

        Node<K, V> node = nodeById(oidnode);
        int nChildren = readchildcount(node);

        if (height == 0) {
            // external node
            for (int i = 0; i < nChildren; i++) {
                UUID oidchild = readchild(node, i);
                Entry child = entryById(oidchild);
                Comparable ckey = readkey(child);
                if (eq(key, ckey))
                    return child;
            }
        } else {
            // internal node
            for (int i = 0; i < nChildren; i++) {
                if (i + 1 == nChildren || lt(key, readkey(readchild(node, i + 1)))) {
                    return searchEntry(readnext(readchild(node, i)), key, height - 1);
                }
            }
        }
        return null;
    }

    /**
     * insert a node starting at the given parent
     * @param oidnode
     * @param key
     * @param value
     * @param height
     * @return oid of a node to be split, if needed
     */
    private UUID
    insert(
        UUID oidnode,
        K key,
        V value,
        int height
        )
    {
        int idx = 0;
        Node<K,V> node = nodeById(oidnode);
        int nChildren = readchildcount(node);
        Entry entry = allocEntry(key, value);

        if(height == 0) {
            for(idx=0; idx<nChildren; idx++)
                if(lt(key, readkey(readchild(node, idx))))
                    break;
        } else {
            // internal node
            for(idx=0; idx<nChildren; idx++) {
                if(idx+1==nChildren || lt(key, readkey(readchild(node, idx+1)))) {
                    UUID oidunode = insert(readnext(readchild(node, idx++)), key, value, height-1);
                    if(oidunode == oidnull)
                        return oidnull;
                    Node<K, V> unode = nodeById(oidunode);
                    UUID uchild0 = readchild(unode, 0);
                    Entry<K, V> uentry0 = entryById(uchild0);
                    Comparable ukey = readkey(uentry0);
                    writekey(entry, ukey);
                    writenext(entry, oidunode);
                    break;
                }
            }
        }

        for(int i=nChildren; i>idx; i--)
            writechild(node, i, readchild(node, i-1));
        writechild(node, idx, entry.oid);
        writechildcount(node, nChildren+1);
        if(nChildren+1 < M)
            return oidnull;
        return split(node);
    }

    /**
     * split a full node
     * @param node
     * @return
     */
    private UUID
    split(
        Node node
        )
    {
        Node t = allocNode(M/2);
        writechildcount(node, M/2);
        for(int i=0; i<M/2; i++)
            writechild(t, i, readchild(node, M/2+i));
        return t.oid;
    }

    /**
     * find the given node
     * @param noid
     * @return
     */
    protected Node<K,V>
    nodeById(UUID noid) {
        rlock();
        try {
            if (!m_nodes.containsKey(noid))
                CDBPhysicalBTree.inform("nodeById(L%d) request for oid=%d: not found!\n", oid, noid);
            return m_nodes.getOrDefault(noid, null);
        } finally {
            runlock();
        }
    }

    /**
     * find the given node
     * @param noid
     * @return
     */
    protected Entry<K,V>
    entryById(UUID noid) {
        rlock();
        try {
            return m_entries.getOrDefault(noid, null);
        } finally {
            runlock();
        }
    }

    /**
     * allocate a new node.
     * @param nChildren
     * @return
     */
    private Node<K, V>
    allocNode(int nChildren) {

        Node<K, V> newnode = new Node<K, V>(TR, DirectoryService.getUniqueID(sf), nChildren, m_pending);

        wlock();
        try {
            m_nodes.put(newnode.oid, newnode);
        } finally {
            wunlock();
        }

        UUID noid = newnode.oid;
        TR.update_helper(newnode, NodeOp.writeChildCountCmd(m_pending, noid, nChildren));
        for(int i=0; i<M; i++)
            TR.update_helper(newnode, NodeOp.writeChildCmd(m_pending, noid, i, oidnull));
        return newnode;
    }

    /**
     * allocate a new entry
     * @param k
     * @param v
     * @return
     */
    private Entry<K, V>
    allocEntry(K k, V v) {

        Entry<K, V> newentry = new Entry<K, V>(TR, DirectoryService.getUniqueID(sf), k, v, oidnull, m_pending);

        wlock();
        try {
            m_entries.put(newentry.oid, newentry);
        } finally {
            wunlock();
        }

        UUID noid = newentry.oid;
        TR.update_helper(newentry, EntryOp.writeKeyCmd(m_pending, noid, k));
        TR.update_helper(newentry, EntryOp.writeValueCmd(m_pending, noid, v));
        TR.update_helper(newentry, EntryOp.writeNextCmd(m_pending, noid, oidnull));
        return newentry;
    }

    /**
     * read the root of the tree
     * Note, this has the effect of inserting the tree container
     * object into the read set, but does not put the actual node
     * there. If query_helper returns false, it means we've already
     * read the tree root in the current transaction, so we're forced to
     * return the most recently observed value.
     * @return
     */
    protected Node<K, V>
    readroot() {

        BTreeOp cmd = BTreeOp.readRootCmd(m_pending, oid);

        if (cmd.isReadAfterWrite(m_pending)) {
            BTreeOp op = (BTreeOp) cmd.getPreviousWrite(m_pending);
            return nodeById(op.oidparam());
        }

        if (TR.query_helper(this, null, cmd))
            return nodeById((UUID) cmd.getReturnValue());

        rlock();
        try {
            CDBPhysicalBTree.inform("readroot(L%d) returning current view root=%d\n", oid, m_root);
            return nodeById(m_root);
        } finally {
            runlock();
        }
    }

    /**
     * return the size based on the most recent view
     * @return
     */
    public int applyReadSize() {
        rlock();
        try {
            return m_size;
        } finally {
            runlock();
        }
    }

    /**
     * return the height based on the current view
     * @return
     */
    public int applyReadHeight() {
        rlock();
        try {
            return m_height;
        } finally {
            runlock();
        }
    }

    /**
     * return the size based on the most recent view
     * @return
     */
    public void applyWriteSize(int size) {
        wlock();
        try {
            m_size = size;
        } finally {
            wunlock();
        }
    }

    /**
     * return the height based on the current view
     * @return
     */
    public void applyWriteHeight(int height) {
        wlock();
        try {
            m_height = height;
        } finally {
            wunlock();
        }
    }

    /**
     * return the size based on the most recent view
     * @return
     */
    public UUID applyReadRoot() {
        rlock();
        try {
            return m_root;
        } finally {
            runlock();
        }
    }

    public void applyWriteRoot(UUID _oid) {
        wlock();
        try {
            CDBPhysicalBTree.inform("L%d setting m_root = %d\n", oid, _oid);
            m_root = _oid;
        } finally {
            wunlock();
        }
    }

    /**
     * read the root oid
     * @return
     */
    private UUID readrootoid() {
        BTreeOp cmd = BTreeOp.readRootCmd(m_pending, oid);
        if(cmd.isReadAfterWrite(m_pending)) {
            BTreeOp op = (BTreeOp) cmd.getPreviousWrite(m_pending);
            return op.oidparam();
        }
        if(!TR.query_helper(this, null, cmd))
            return applyReadRoot();
        return (UUID) cmd.getReturnValue();
    }

    /**
     * read the size
     * @return
     */
    private int readsize() {
        BTreeOp cmd = BTreeOp.readSizeCmd(m_pending, oid);
        if(cmd.isReadAfterWrite(m_pending))
            return ((BTreeOp)cmd.getPreviousWrite(m_pending)).iparam();
        if(!TR.query_helper(this, null, cmd))
            return applyReadSize();
        return (int) cmd.getReturnValue();
    }

    /**
     * read the size
     * @return
     */
    private int readheight() {
        BTreeOp cmd = BTreeOp.readHeightCmd(m_pending, oid);
        if(cmd.isReadAfterWrite(m_pending))
            return ((BTreeOp)cmd.getPreviousWrite(m_pending)).iparam();
        if(!TR.query_helper(this, null, cmd))
            return applyReadHeight();
        return (int) cmd.getReturnValue();
    }

    /**
     * write the child oid at the given index in the node
     * @param node
     * @param index
     * @param oidchild
     */
    private void
    writechild(
        Node<K,V> node,
        int index,
        UUID oidchild
        )
    {
        NodeOp cmd = NodeOp.writeChildCmd(m_pending, node.oid, index, oidchild);
        TR.update_helper(node, cmd);
    }

    /**
     * write the node's child count
     * @param node
     * @param count
     */
    private void
    writechildcount(
        Node<K,V> node,
        int count
        )
    {
        NodeOp cmd = NodeOp.writeChildCountCmd(m_pending, node.oid, count);
        TR.update_helper(node, cmd);
    }

    /**
     * write the entry key
     * @param entry
     * @param ckey
     */
    private void
    writekey(
        Entry<K, V> entry,
        Comparable ckey
        )
    {
        EntryOp<K,V> cmd = EntryOp.writeKeyCmd(m_pending, entry.oid, (K) ckey);
        TR.update_helper(entry, cmd);
    }

    /**
     * write the entry value
     * @param entry
     * @param value
     */
    private void
    writevalue(
        Entry<K, V> entry,
        V value
        )
    {
        EntryOp<K,V> cmd = EntryOp.writeValueCmd(m_pending, entry.oid, value);
        TR.update_helper(entry, cmd);
    }

    /**
     * write the entry's next pointer
     * @param entry
     * @param next
     */
    private void
    writenext(
        Entry<K, V> entry,
        UUID next
        )
    {
        EntryOp<K,V> cmd = EntryOp.writeNextCmd(m_pending, entry.oid, next);
        TR.update_helper(entry, cmd);
    }

    /**
     * write the deleted flag for the entry
     * @param entry
     * @param deleted
     */
    private void
    writedeleted(
        Entry<K, V> entry,
        boolean deleted
        )
    {
        EntryOp<K,V> cmd = EntryOp.writeDeletedCmd(m_pending, entry.oid, deleted);
        TR.update_helper(entry, cmd);
    }

    /**
     * read the key of the given entry
     * @param entry
     * @return
     */
    private Comparable
    readkey(
        Entry<K, V> entry
        )
    {
        EntryOp<K, V> cmd = EntryOp.readKeyCmd(m_pending, entry.oid);
        if(cmd.isReadAfterWrite(m_pending))
            return ((EntryOp<K,V>)cmd.getPreviousWrite(m_pending)).key();
        if(!TR.query_helper(entry, null, cmd))
            return entry.applyReadKey();
        return (Comparable) cmd.getReturnValue();
    }

    /**
     * read the key of the given entry
     * @param entry
     * @return
     */
    private boolean
    readdeleted(
        Entry<K, V> entry
        )
    {
        EntryOp<K, V> cmd = EntryOp.readDeletedCmd(m_pending, entry.oid);
        if(cmd.isReadAfterWrite(m_pending))
            return ((EntryOp<K,V>)cmd.getPreviousWrite(m_pending)).deleted();
        if(!TR.query_helper(entry, null, cmd))
            return entry.applyReadDeleted();
        return (boolean) cmd.getReturnValue();
    }

    /**
     * read the key of the given entry
     * @param entryoid
     * @return
     */
    private Comparable
    readkey(
        UUID entryoid
        )
    {
        EntryOp<K, V> cmd = EntryOp.readKeyCmd(m_pending, entryoid);
        if(cmd.isReadAfterWrite(m_pending))
            return ((EntryOp<K,V>)cmd.getPreviousWrite(m_pending)).key();
        Entry<K, V> entry = entryById(entryoid);
        if(!TR.query_helper(entry, null, cmd))
            return entry.applyReadKey();
        return (Comparable) cmd.getReturnValue();
    }

    /**
     * read the value field of the given node
     * @param entry
     * @return
     */
    private V
    readvalue(
        Entry<K, V> entry
        )
    {
        EntryOp<K,V> cmd = EntryOp.readValueCmd(m_pending, entry.oid);
        if(cmd.isReadAfterWrite(m_pending))
            return ((EntryOp<K,V>)cmd.getPreviousWrite(m_pending)).value();
        if(!TR.query_helper(entry, null, cmd))
            return entry.applyReadValue();
        return (V) cmd.getReturnValue();
    }

    /**
     * read the next pointer of the entry
     * @param entry
     * @return
     */
    private UUID
    readnext(
        Entry<K, V> entry
        )
    {
        EntryOp<K,V> cmd = EntryOp.readNextCmd(m_pending, entry.oid);
        if(cmd.isReadAfterWrite(m_pending))
            return ((EntryOp<K,V>)cmd.getPreviousWrite(m_pending)).oidnext();
        if(!TR.query_helper(entry, null, cmd))
            return entry.applyReadNext();
        return (UUID) cmd.getReturnValue();
    }

    /**
     * read the next pointer of the entry
     * @param entryoid
     * @return
     */
    private UUID
    readnext(
            UUID entryoid
        )
    {
        Entry<K, V> entry = entryById(entryoid);
        EntryOp<K,V> cmd = EntryOp.readNextCmd(m_pending, entry.oid);
        if(cmd.isReadAfterWrite(m_pending))
            return ((EntryOp<K,V>)cmd.getPreviousWrite(m_pending)).oidnext();
        if(!TR.query_helper(entry, null, cmd))
            return entry.applyReadNext();
        return (UUID) cmd.getReturnValue();
    }


    /**
     * get the children array for the given node
     * @param node
     * @return
     */
    private UUID
    readchild(
        Node<K,V> node,
        int idx
        )
    {
        NodeOp cmd = NodeOp.readChildCmd(m_pending, node.oid, idx);
        if(cmd.isReadAfterWrite(m_pending))
            return ((NodeOp)cmd.getPreviousWrite(m_pending)).oidparam();
        if(!TR.query_helper(node, null, cmd)) {
            return node.applyReadChild(idx);
        }
        return (UUID) cmd.getReturnValue();
    }

    /**
     * read the number of valid child pointers
     * in the given node.
     * @param node
     * @return
     */
    private int
    readchildcount(
        Node<K,V> node
        )
    {
        NodeOp cmd = NodeOp.readChildCountCmd(m_pending, node.oid);
        if(cmd.isReadAfterWrite(m_pending))
            return ((NodeOp)cmd.getPreviousWrite(m_pending)).childcount();
        if(!TR.query_helper(node, null, cmd))
            return node.applyReadChildCount();
        return (int) cmd.getReturnValue();
    }

    /**
     * write the size field of the btree
     * @param size
     */
    private void writesize(int size) {
        BTreeOp cmd = BTreeOp.writeSizeCmd(m_pending, oid, size);
        TR.update_helper(this, cmd);
    }

    /**
     * write the height field
     * @param height
     */
    private void writeheight(int height) {
        BTreeOp cmd = BTreeOp.writeHeightCmd(m_pending, oid, height);
        TR.update_helper(this, cmd);
    }

    /**
     * write the root member
     * @param root
     */
    private void writeroot(UUID root) {
        BTreeOp cmd = BTreeOp.writeRootCmd(m_pending, oid, root);
        TR.update_helper(this, cmd);
    }

}


