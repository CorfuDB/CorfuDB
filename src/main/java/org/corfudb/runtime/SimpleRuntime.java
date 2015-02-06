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
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;
import org.corfudb.sharedlog.sequencer.SequencerService;
import org.corfudb.sharedlog.sequencer.SequencerTask;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * This runtime implementation provides linearizable semantics for CorfuDB objects. It's unaware of transactions.
 * It does a simple, pass-through translation between the runtime API and SMR invocations, with the addition of
 * object multiplexing so that a single SMR instance can be shared by multiple objects.
 *
 */
public class SimpleRuntime implements AbstractRuntime, SMRLearner, RPCServerHandler
{
    StreamFactory streamfactory;

    //underlying SMREngines
    Map<Long, SMREngine> enginemap;

    //map from object IDs to object instances; used for multiplexing
    Map<Long, CorfuDBObject> objectmap;

    //unique node id
    long uniquenodeid;

    /**
     * Registers an object with the runtime
     *
     * @param  obj  the object to register
     */
    public void registerObject(CorfuDBObject obj)
    {
        synchronized(objectmap)
        {
            synchronized(enginemap)
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
            }
        }
    }

    CorfuDBObject getObject(long objectid)
    {
        synchronized(objectmap)
        {
            if (!objectmap.containsKey(objectid)) throw new RuntimeException("object not registered!");
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

    RPCClient rpcc;
    RPCServer rpcs;

    /**
     * Creates a SimpleRuntime
     *
     * @param  fact  a factory for creating new Stream objects
     * @param  tuniquenodeid    an identifier unique to this client process
     */
    public SimpleRuntime(StreamFactory fact, long tuniquenodeid)
    {
        streamfactory = fact;
        objectmap = new HashMap();
        enginemap = new HashMap();
        uniquenodeid = tuniquenodeid;

        //rpc
        rpcc = new ThriftRPCClient();
        rpcs = new ThriftRPCServer();
        rpcs.registerHandler(9090, this);
        System.out.println(rpcc.send("Hello World", "localhost", 9090));
        //System.exit(0);
    }

    public void BeginTX()
    {
        throw new RuntimeException("SimpleRuntime does not support transactions.");
    }

    public boolean EndTX()
    {
        throw new RuntimeException("SimpleRuntime does not support transactions.");
    }


    public void query_then_update_helper(CorfuDBObject cob, Object query, Serializable update)
    {
        query_then_update_helper(cob, query, update, null);
    }

    public void query_then_update_helper(CorfuDBObject cob, Object query, Serializable update, Serializable key)
    {
        Set<Long> streams = new HashSet<Long>();
        streams.add(cob.getID());
        getEngine(cob.getID()).propose(update, streams, query);
    }

    public void update_helper(CorfuDBObject cob, Serializable update)
    {
        update_helper(cob, update, null);
    }

    public void update_helper(CorfuDBObject cob, Serializable update, Serializable key)
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


    public void query_helper(CorfuDBObject cob, Serializable key, Object command)
    {
        getEngine(cob.getID()).sync(SMREngine.TIMESTAMP_INVALID, command); //check getEngine return value?
    }


    public void apply(Object command, long curstream, Set<Long> streams, long timestamp)
    {
        if(streams.size()!=1) throw new RuntimeException("unimplemented");
        Long streamid = streams.iterator().next();
        synchronized(objectmap)
        {
            if(objectmap.containsKey(streamid))
            {
                CorfuDBObject cob = objectmap.get(streamid);
                cob.apply(command);
                //todo: verify that it's okay for this to not be atomic with the apply
                //in the worst case, the object thinks it has an older version than it really does
                //but all that should cause is spurious aborts
                //the alternative is to have the apply in the object always call a superclass version of apply
                //that sets the timestamp
                //only the apply thread sets the timestamp, so we only have to worry about concurrent reads
                if(timestamp!=SMREngine.TIMESTAMP_INVALID)
                    cob.setTimestamp(timestamp);
            }
            else
                throw new RuntimeException("entry for stream " + streamid + " with no registered object");
        }

    }

    //receives incoming RPCs
    @Override
    public Object deliver(Object cmd)
    {
        System.out.println(cmd);
        return "Woohoo!";
    }
}

interface RPCServer
{
    public void registerHandler(int portnum, RPCServerHandler h);
}

interface RPCServerHandler
{
    public Object deliver(Object cmd);
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
                    return Utils.serialize(handler.deliver(Utils.deserialize(arg)));
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