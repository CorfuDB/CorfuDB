/**
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.corfudb.runtime;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

abstract class BaseRuntime implements AbstractRuntime, SMRLearner, RPCServerHandler
{
    //underlying SMREngines
    Map<Long, SMREngine> enginemap;

    //map from object IDs to object instances; used for multiplexing
    Map<Long, CorfuDBObject> objectmap;

    //unique node id
    long uniquenodeid;

    final long reservedStreamIDStart = Long.MAX_VALUE-100;
    final long reservedStreamIDStop = Long.MAX_VALUE;
    final long reservedRemoteReadMapID = Long.MAX_VALUE;

    StreamFactory streamfactory;

    RPCClient rpcc;
    RPCServer rpcs;
    RemoteReadMap rrmap;
    String rpchostname;
    int rpcportnum;


    public BaseRuntime(StreamFactory fact, long tuniquenodeid, String trpchostname, int trpcportnum)
    {
        streamfactory = fact;
        objectmap = new HashMap();
        enginemap = new HashMap();
        uniquenodeid = tuniquenodeid;

        //rpc
        rpchostname = trpchostname;
        rpcportnum = trpcportnum;
        rpcc = new ThriftRPCClient();
        rpcs = new ThriftRPCServer();
        rrmap = new RemoteReadMapImpl(fact.newStream(reservedRemoteReadMapID), uniquenodeid);
        rpcs.registerHandler(rpcportnum, this);
    }

    CorfuDBObject getObject(long objectid)
    {
        synchronized(objectmap)
        {
//            if (!objectmap.containsKey(objectid)) throw new RuntimeException("object not registered!");
            //returns null if the object does not exist
            return objectmap.get(objectid);
        }
    }

    //returns any engine
    SMREngine getEngine()
    {
        synchronized(enginemap)
        {
            if(enginemap.size()==0) return null;
            return enginemap.values().iterator().next();
        }
    }

    /**
     * Returns the SMR engine corresponding to the passed in stream/object ID.
     *
     * @param objectid object/stream id
     * @return SMR Engine playing the stream with the passed in stream id
     */
    SMREngine getEngine(long objectid)
    {
        synchronized(enginemap)
        {
            if(!enginemap.containsKey(objectid)) return null;
            return enginemap.get(objectid);
        }
    }




    public void registerObject(CorfuDBObject obj)
    {
        registerObject(obj, false);
    }

    /**
     * Registers an object with the runtime
     *
     * @param  obj  the object to register
     */
    public void registerObject(CorfuDBObject obj, boolean remote)
    {
        if(!remote)
        {
            synchronized (objectmap)
            {
                synchronized (enginemap)
                {
                    if (objectmap.containsKey(obj.getID()))
                    {
                        System.out.println("object ID already registered!");
                        throw new RuntimeException();
                    }
                    System.out.println("registering object ID " + obj.getID());
                    objectmap.put(obj.getID(), obj);
                    SMREngine smre = new SMREngine(streamfactory.newStream(obj.getID()), uniquenodeid);
                    smre.registerLearner(this);
                    enginemap.put(obj.getID(), smre);
                    rrmap.putMyRuntime(obj.getID(), this.rpchostname, this.rpcportnum);
                }
            }
        }
        else
        {
            System.out.println("ignoring remote object registration for " + obj.getID());
        }
    }


    public void rpcRemoteRuntime(CorfuDBObject cob, CorfuDBObjectCommand command)
    {
        Pair<String, Integer> remoteruntime = rrmap.getRemoteRuntime(cob.getID());
        if(remoteruntime==null)
            throw new RuntimeException("unable to find object in system");
        CorfuDBObjectCommand retobj = (CorfuDBObjectCommand)rpcc.send(new Pair<Long, Object>(cob.getID(), command), remoteruntime.first, remoteruntime.second);
        if(retobj==null)
            throw new RuntimeException("remote read returned null...");
        command.setReturnValue(retobj.getReturnValue());
    }

    //receives incoming RPCs
    @Override
    public Object deliverRPC(Object cmd)
    {
        Pair<Long, Object> P = (Pair<Long, Object>)cmd;
        //only queries are supported --- should we check this here, or just enforce it at the send point?
        SMREngine smre = getEngine(P.first);
        if(smre==null) //we aren't playing this stream; the client was misinformed
        {
            System.out.println("received RPC for object that we aren't playing");
            return null; //todo: should we return a cleaner error code instead?
        }
        smre.sync(SMREngine.TIMESTAMP_INVALID, P.second);
        return P.second;
    }

    //we don't lock the object; the sub-classing runtime is responsible for locking, if required,
    //before calling this method
    public void applyCommandToObject(long curstream, Object command, long timestamp)
    {
        CorfuDBObject cob = getObject(curstream);
        if(cob==null) throw new RuntimeException("entry for stream " + curstream + " with no registered object");
        cob.applyToObject(command);
        //todo: verify that it's okay for this to not be atomic with the apply
        //in the worst case, the object thinks it has an older version than it really does
        //but all that should cause is spurious aborts
        //the alternative is to have the apply in the object always call a superclass version of apply
        //that sets the timestamp
        //only the apply thread sets the timestamp, so we only have to worry about concurrent reads
        if(timestamp!=SMREngine.TIMESTAMP_INVALID)
            cob.setTimestamp(timestamp);
    }


}

/**
 * This runtime implementation provides linearizable semantics for CorfuDB objects. It's unaware of transactions.
 * It does a simple, pass-through translation between the runtime API and SMR invocations, with the addition of
 * object multiplexing so that a single SMR instance can be shared by multiple objects.
 *
 */
public class SimpleRuntime extends BaseRuntime
{

    /**
     * Creates a SimpleRuntime
     *
     * @param  fact  a factory for creating new Stream objects
     * @param  tuniquenodeid    an identifier unique to this client process
     */
    public SimpleRuntime(StreamFactory fact, long tuniquenodeid, String trpchostname, int trpcportnum)
    {
        super(fact, tuniquenodeid, trpchostname, trpcportnum);
    }

    public void BeginTX()
    {
        throw new RuntimeException("SimpleRuntime does not support transactions.");
    }

    public boolean EndTX()
    {
        throw new RuntimeException("SimpleRuntime does not support transactions.");
    }


    public void query_then_update_helper(CorfuDBObject cob, CorfuDBObjectCommand query, CorfuDBObjectCommand update)
    {
        query_then_update_helper(cob, query, update, null);
    }

    public void query_then_update_helper(CorfuDBObject cob, CorfuDBObjectCommand query, CorfuDBObjectCommand update, Serializable key)
    {
        Set<Long> streams = new HashSet<Long>();
        streams.add(cob.getID());
        SMREngine smre = getEngine(cob.getID());
        if(smre==null) // we are not playing this stream
        {
            Pair<String, Integer> remotenode = rrmap.getRemoteRuntime(  cob.getID());
            if(remotenode==null) // we can't locate a remote runtime to read from either
                throw new RuntimeException("Cant find object in system");
            throw new RuntimeException("remote objects do not support query_then_update_helper");
        }

        getEngine(cob.getID()).propose(update, streams, query);
    }

    public void update_helper(CorfuDBObject cob, CorfuDBObjectCommand update)
    {
        update_helper(cob, update, null);
    }

    public void update_helper(CorfuDBObject cob, CorfuDBObjectCommand update, Serializable key)
    {
        query_then_update_helper(cob, null, update);
    }

    public void query_helper(CorfuDBObject cob)
    {
        query_helper(cob, null);
    }

    public void query_helper(CorfuDBObject cob, Serializable key)
    {
        query_helper(cob, key, null);
    }


    public void query_helper(CorfuDBObject cob, Serializable key, CorfuDBObjectCommand command)
    {
        SMREngine smre = getEngine(cob.getID());
        if(smre==null) //not playing stream
        {
            rpcRemoteRuntime(cob, command);
        }
        else
            smre.sync(SMREngine.TIMESTAMP_INVALID, command);
    }

    public void deliver(Object command, long curstream, long timestamp)
    {
        //we don't have to lock the object --- there's one thread per SMREngine,
        //and exactly one SMREngine per stream/object
        applyCommandToObject(curstream, command, timestamp);
    }

}

interface RPCServer
{
    public void registerHandler(int portnum, RPCServerHandler h);
}

interface RPCServerHandler
{
    public Object deliverRPC(Object cmd);
}

interface RPCClient
{
    public Object send(Serializable command, String hostname, int portnum);
}

class ThriftRPCClient implements RPCClient
{
    public void ThriftRPCClient()
    {

    }
    public Object send(Serializable command, String hostname, int portnum)
    {
        try
        {
            //todo: make this less brain-dead
            TTransport transport = new TSocket(hostname, portnum);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            RemoteReadService.Client client = new RemoteReadService.Client(protocol);

            Object ret = Utils.deserialize(client.remote_read(Utils.serialize(command)));
            transport.close();
            return ret;
        }
        catch (TTransportException e)
        {
            throw new RuntimeException(e);
        }
        catch (TException e)
        {
            throw new RuntimeException(e);
        }
    }
}

class ThriftRPCServer implements RPCServer
{

    public ThriftRPCServer()
    {

    }

    public void registerHandler(int portnum, RPCServerHandler h)
    {
        final RPCServerHandler handler = h;
        final TServer server;
        TServerSocket serverTransport;
        RemoteReadService.Processor<RemoteReadService.Iface> processor;
        try
        {
            serverTransport = new TServerSocket(portnum);
            processor = new RemoteReadService.Processor(new RemoteReadService.Iface()
            {
                @Override
                public ByteBuffer remote_read(ByteBuffer arg) throws TException
                {
                    return Utils.serialize(handler.deliverRPC(Utils.deserialize(arg)));
                }
            });
            server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
            new Thread(new Runnable()
            {
                @Override
                public void run()
                {
                    server.serve(); //this seems to be a blocking call, putting it in its own thread
                }
            }).start();
            System.out.println("listening on port " + portnum);
        }
        catch (TTransportException e)
        {
            throw new RuntimeException(e);
        }
    }

}


class Utils
{
    public static ByteBuffer serialize(Object obj)
    {
        try
        {
            //todo: make serialization less clunky!
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            byte b[] = baos.toByteArray();
            oos.close();
            return ByteBuffer.wrap(b);
        }
        catch(IOException e)
        {
            throw new RuntimeException(e);
        }
    }
    public static Object deserialize(ByteBuffer b)
    {
        try
        {
            //todo: make serialization less clunky!
            ByteArrayInputStream bais = new ByteArrayInputStream(b.array());
            ObjectInputStream ois = new ObjectInputStream(bais);
            Object obj = ois.readObject();
            return obj;
        }
        catch(IOException e)
        {
            throw new RuntimeException(e);
        }
        catch(ClassNotFoundException ce)
        {
            throw new RuntimeException(ce);
        }
    }
}

interface RemoteReadMap
{
    public Pair<String, Integer> getRemoteRuntime(long objectid);
    public void putMyRuntime(long objectid, String hostname, int port);
}

class RemoteReadMapImpl implements RemoteReadMap, SMRLearner
{
    //this can't be implemented over CorfuDBObjects to avoid
    //circularity issues.

    //for now, we maintain just one node per object
    Map<Long, Pair<String, Integer>> objecttoruntimemap;
    Lock biglock;

    SMREngine smre;

    public RemoteReadMapImpl(Stream s, long uniquenodeid)
    {
        objecttoruntimemap = new HashMap<Long, Pair<String, Integer>>();
//        System.out.println("creating remotereadmap...");
        smre = new SMREngine(s, uniquenodeid);
        smre.registerLearner(this);
//        System.out.println(smre + " has learner " + smre.smrlearner + " of class " + smre.smrlearner.getClass());
        biglock = new ReentrantLock();
    }

    @Override
    public Pair<String, Integer> getRemoteRuntime(long objectid)
    {
        smre.sync();
        biglock.lock();
        Pair<String, Integer> P = objecttoruntimemap.get(objectid);
        if(P==null)
        {
            System.out.println("object " + objectid + " not found");
            System.out.println(objecttoruntimemap);
        }
        biglock.unlock();
        return P;
    }

    @Override
    public void putMyRuntime(long objectid, String hostname, int port)
    {
        Triple T = new Triple(objectid, hostname, port);
//        System.out.println("RRM proposing command " + T + " on " + smre.curstream.getStreamID());
        smre.propose(T);
    }

    @Override
    public void deliver(Object command, long curstream, long timestamp)
    {
//        System.out.println("RRM got message on " + curstream);
        Triple<Long, String, Integer> T = (Triple<Long, String, Integer>)command;
        biglock.lock();
        objecttoruntimemap.put(T.first, new Pair(T.second, T.third));
        biglock.unlock();
    }
}
