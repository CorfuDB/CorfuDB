package org.corfudb.runtime.collections;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.corfudb.runtime.smr.AbstractRuntime;
import org.corfudb.runtime.smr.CorfuDBObject;
import org.corfudb.runtime.smr.CorfuDBObjectCommand;
import org.corfudb.runtime.smr.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.corfudb.runtime.abstractions.ITimestamp;

public class CorfuDBZK extends CorfuDBObject implements IZooKeeper
{
    static Logger dbglog = LoggerFactory.getLogger(CorfuDBZK.class);

    Map<File, ZNode> map;
    Watcher defaultwatcher;
    AbstractRuntime CR;

    @Override
    public void applyToObject(Object update, ITimestamp timestamp) throws Exception
    {
        if (update instanceof CreateOp)
            ((CreateOp)update).setReturnValue(apply((CreateOp)update));
        else if(update instanceof ExistsOp)
            ((ExistsOp)update).setReturnValue(apply((ExistsOp)update));
        else if(update instanceof SetOp)
            ((SetOp)update).setReturnValue(apply((SetOp)update));
        else if(update instanceof GetOp)
            ((GetOp)update).setReturnValue(apply((GetOp)update));
        else if(update instanceof DeleteOp)
            ((DeleteOp)update).setReturnValue(apply((DeleteOp)update));
        else if(update instanceof GetChildrenOp)
            ((GetChildrenOp)update).setReturnValue(apply((GetChildrenOp)update));
        else
            throw new RuntimeException("unrecognized command");
    }

    public Object apply(CreateOp cop) throws KeeperException
    {
        dbglog.debug("applying create command");
        String path = cop.path;
        File f = new File(path);
        if(map.containsKey(f))
            throw new KeeperException.NodeExistsException();

        if(!map.containsKey(f.getParentFile()))
            throw new KeeperException.NoNodeException();

        ZNode N = map.get(f.getParentFile());

        if(cop.cm.isEphemeral()) throw new RuntimeException("not yet supported!");
        if(N.isEphemeral())
            throw new KeeperException.NoChildrenForEphemeralsException();


        if(cop.cm.isSequential())
        {
            AtomicInteger I = N.sequentialcounters.get(f);
            if(I==null)
            {
                I = new AtomicInteger(0);
                N.sequentialcounters.put(f,  I);
            }
            int x = I.getAndIncrement();
            String y = String.format("%010d", x);
            path = path + y;
            f = new File(path);
        }



        ZNode newnode = new ZNode(cop.data, path);
        newnode.stat.setCzxid((long) Math.random()); //copy stat either here or on read //todo: set this to curpos
        N.children.add(newnode);
//            triggerwatches(existswatches.get(newnode.path), new WatchedEvent(Watcher.Event.EventType.NodeCreated, KeeperState.SyncConnected,newnode.path));
//            triggerwatches(N.childrenwatches, new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, KeeperState.SyncConnected, N.path));
        map.put(f, newnode);
        return path;
    }

    Map<String, Set<Watcher>> existswatches = new HashMap(); //protected by map
    public Object apply(ExistsOp eop) throws KeeperException
    {
        dbglog.debug("applying exists command");
        File F = new File(eop.path);
        if(!map.containsKey(F))
        {
            if(eop.watch && defaultwatcher!=null)
            {
                Set<Watcher> S = existswatches.get(eop.path);
                if(S==null)
                {
                    S = new HashSet<Watcher>();
                    existswatches.put(eop.path, S);
                }
                S.add(defaultwatcher);
            }
            return null;
        }
        ZNode N = map.get(F);
        Stat x = N.stat;
        if(eop.watch && defaultwatcher!=null) N.datawatches.add(defaultwatcher);
        return x; //todo -- return copy of stat?
    }

    public Object apply(SetOp sop) throws KeeperException
    {
        File f = new File(sop.path);
        if(!map.containsKey(f))
            throw new KeeperException.NoNodeException();
        ZNode N = map.get(f);
        if(sop.version!=-1 && sop.version!=N.stat.getVersion())
            throw new KeeperException.BadVersionException();
        N.data = sop.data;
        N.stat.setMzxid((long) Math.random()); //todo: set to curpos
        Stat x = N.stat;
        //mb: triggers both 'exists' and 'getdata' watches
//            triggerwatches(N.datawatches, new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, Watcher.Event.KeeperState.SyncConnected, N.path));
        return x; //handle stat copying either here or at modification
    }

    public Object apply(GetOp gop) throws KeeperException
    {
        File F = new File(gop.path);
        if(!map.containsKey(F))
            throw new KeeperException.NoNodeException();
        ZNode N = map.get(F);
        Stat x = N.stat;
        byte[] y = N.data;
        if(gop.watch && defaultwatcher!=null) N.datawatches.add(defaultwatcher);
        return new Pair<byte[], Stat>(y, x); //todo -- copy data and stat
    }

    public Object apply(DeleteOp dop) throws KeeperException
    {
        File F = new File(dop.path);
        if(!map.containsKey(F))
            throw new KeeperException.NoNodeException();
        ZNode N = map.get(F);
        //		N.lock();
        if(dop.version!=-1 && N.stat.getVersion()!=dop.version)
            throw new KeeperException.BadVersionException();
        if(N.children.size()>0)
            throw new KeeperException.NotEmptyException();
        ZNode parent = map.get(F.getParentFile());
        parent.children.remove(N);
//        triggerwatches(parent.childrenwatches, new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, KeeperState.SyncConnected, parent.path));
//        triggerwatches(N.datawatches, new WatchedEvent(Watcher.Event.EventType.NodeDeleted, KeeperState.SyncConnected, N.path));
        map.remove(F);
        return null;
    }

    public Object apply(GetChildrenOp gcop) throws KeeperException
    {
        File F = new File(gcop.path);
        if(!map.containsKey(F))
            throw new KeeperException.NoNodeException();
        ZNode N = map.get(F);
        //		N.lock();
        LinkedList<String> children = new LinkedList<String>();
        Iterator<ZNode> it = N.children.iterator();
        while(it.hasNext())
        {
            children.add(it.next().path);
        }
        if(gcop.watch && defaultwatcher!=null) N.childrenwatches.add(defaultwatcher);
        //		N.unlock();
        return children;
    }


    class ZNode
    {
        Stat stat;
        Lock L;
        byte data[];
        boolean ephemeral;
        List<ZNode> children;
        Map<File, AtomicInteger> sequentialcounters;
        String path;
        Set<Watcher> datawatches;
        Set<Watcher> childrenwatches;
        public ZNode(byte data[], String tpath)
        {
            ephemeral = false;
            this.data = data;
            children = new ArrayList();
            L = new ReentrantLock();
            stat = new Stat();
            path = tpath;
            datawatches = new HashSet();
            childrenwatches = new HashSet();
            sequentialcounters = new HashMap();
        }
        public void lock()
        {
            L.lock();
        }
        public void unlock()
        {
            L.unlock();
        }
        public byte[] getData()
        {
            return data;
        }
        public boolean isEphemeral()
        {
            return ephemeral;
        }
        public String toString()
        {
            return path;
        }
    }

    public CorfuDBZK(AbstractRuntime truntime, long objectid, boolean remote, Watcher twatcher) throws IOException
    {
        super(truntime, objectid, remote);
        defaultwatcher = twatcher;
        CR = truntime;
        map = new HashMap();
        map.put(new File("/"), new ZNode("nullentry".getBytes(), "/"));
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException
    {
        CreateOp cop = new CreateOp(path, data, acl, createMode, null, null);
        CR.update_helper(this, cop);
        if(cop.getException()!=null)
        {
            if (cop.getException() instanceof KeeperException) throw (KeeperException) cop.getException();
            else if (cop.getException() instanceof InterruptedException) throw (InterruptedException) cop.getException();
            else throw new RuntimeException(cop.getException());
        }
        return (String)cop.getReturnValue();
    }

    @Override
    public Stat exists(String path, boolean watcher) throws KeeperException, InterruptedException
    {
        ExistsOp eop = new ExistsOp(path, watcher, null, null);
        CR.query_helper(this, null, eop);
        if(eop.getException()!=null)
        {
            if (eop.getException() instanceof KeeperException) throw (KeeperException) eop.getException();
            else if (eop.getException() instanceof InterruptedException) throw (InterruptedException) eop.getException();
            else throw new RuntimeException(eop.getException());
        }
        return (Stat)eop.getReturnValue();
    }

    @Override
    public void delete(String path, int version) throws KeeperException
    {
        DeleteOp dop = new DeleteOp(path, version, null, null);
        if(dop.getException()!=null)
        {
            if (dop.getException() instanceof KeeperException) throw (KeeperException) dop.getException();
            else throw new RuntimeException(dop.getException());
        }
        CR.update_helper(this, dop);
    }

    @Override
    public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException
    {
        SetOp sop = new SetOp(path, data, version, null, null);
        CR.update_helper(this, sop);
        if(sop.getException()!=null)
        {
            if (sop.getException() instanceof KeeperException) throw (KeeperException) sop.getException();
            else if (sop.getException() instanceof InterruptedException) throw (InterruptedException) sop.getException();
            else throw new RuntimeException(sop.getException());
        }
        return (Stat)sop.getReturnValue();
    }

    @Override
    public byte[] getData(String path, boolean watcher, Stat stat) throws KeeperException, InterruptedException
    {
        //todo: what about stat?
        GetOp gop = new GetOp(path, watcher, null, null);
        CR.query_helper(this, null, gop);
        if(gop.getException()!=null)
        {
            if (gop.getException() instanceof KeeperException) throw (KeeperException) gop.getException();
            else if (gop.getException() instanceof InterruptedException) throw (InterruptedException) gop.getException();
            else throw new RuntimeException(gop.getException());
        }
        return ((Pair<byte[], Stat>)gop.getReturnValue()).first;
    }

    @Override
    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException
    {
        GetChildrenOp gcop = new GetChildrenOp(path, watch, null, null);
        CR.query_helper(this, null, gcop);
        if(gcop.getException()!=null)
        {
            if (gcop.getException() instanceof KeeperException) throw (KeeperException) gcop.getException();
            else if (gcop.getException() instanceof InterruptedException) throw (InterruptedException) gcop.getException();
            else throw new RuntimeException(gcop.getException());
        }
        return (List<String>)gcop.getReturnValue();
    }

    @Override
    public void create(String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctxt)
    {
    }

    @Override
    public void delete(String path, int version, AsyncCallback.VoidCallback cb, Object ctx)
    {

    }

    @Override
    public void setData(String path, byte[] data, int version, AsyncCallback.StatCallback cb, Object ctx)
    {

    }

    @Override
    public void exists(String path, boolean watch, AsyncCallback.StatCallback cb, Object ctx)
    {

    }

    @Override
    public void getData(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx)
    {

    }

    @Override
    public void getChildren(String path, boolean watch, AsyncCallback.ChildrenCallback cb, Object ctx)
    {

    }
}

class CreateOp extends ZKOp implements Serializable
{
    CreateMode cm;
    byte[] data;
    List<ACL> acl;
    String path;
    AsyncCallback.StringCallback cb;
    Object ctxt;
    public CreateOp(String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctxt)
    {
        this.path = path;
        this.data = data;
        this.acl = acl;
        this.cm = createMode;
        this.cb = cb;
        this.ctxt = ctxt;
    }
}

abstract class ZKOp extends CorfuDBObjectCommand
{
    static AtomicInteger idcounter = new AtomicInteger();
    Object identifier;
    public ZKOp()
    {
        identifier = new Integer(idcounter.getAndIncrement());
    }
    public boolean equals(ZKOp cop)
    {
        return cop.identifier.equals(this.identifier);
    }
}

class ExistsOp extends ZKOp
{
    String path;
    boolean watch;
    AsyncCallback.StatCallback cb;
    Object ctx;

    public ExistsOp(String path, boolean watch, AsyncCallback.StatCallback cb, Object ctx)
    {
        this.path = path;
        this.watch = watch;
        this.cb = cb;
        this.ctx = ctx;
    }
}

class SetOp extends ZKOp implements Serializable
{
    String path;
    byte[] data;
    int version;
    AsyncCallback.StatCallback cb;
    Object ctx;
    public SetOp(String path, byte[] data, int version, AsyncCallback.StatCallback cb, Object ctx)
    {
        this.path = path;
        this.data = data;
        this.version = version;
        this.cb = cb;
        this.ctx = ctx;
    }
}

class GetOp extends ZKOp
{
    String path;
    boolean watch;
    AsyncCallback.DataCallback cb;
    Object ctx;

    public GetOp(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx)
    {
        this.path = path;
        this.watch = watch;
        this.cb = cb;
        this.ctx = ctx;
    }
}

class DeleteOp extends ZKOp implements Serializable
{
    String path;
    int version;
    AsyncCallback.VoidCallback cb;
    Object ctxt;
    public DeleteOp(String path, int version, AsyncCallback.VoidCallback tcb, Object tctxt)
    {
        this.path = path;
        this.version = version;
        this.cb = tcb;
        this.ctxt = tctxt;
    }
}

class GetChildrenOp extends ZKOp
{
    String path;
    boolean watch;
    AsyncCallback.ChildrenCallback cb;
    Object ctx;
    public GetChildrenOp(String path, boolean watch, AsyncCallback.ChildrenCallback cb, Object ctx)
    {
        this.path = path;
        this.watch = watch;
        this.cb = cb;
        this.ctx = ctx;
    }
}
