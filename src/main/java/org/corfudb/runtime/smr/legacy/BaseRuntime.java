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
package org.corfudb.runtime.smr.legacy;

import org.corfudb.runtime.smr.*;
import org.corfudb.runtime.stream.ITimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public abstract class BaseRuntime implements AbstractRuntime, SMRLearner, IRPCServerHandler
{

    static Logger dbglog = LoggerFactory.getLogger(BaseRuntime.class);

    //underlying SMREngines
    Map<UUID, SMREngine> enginemap;

    //map from object IDs to object instances; used for multiplexing
    Map<UUID, CorfuDBObject> objectmap;

    //unique node id
    UUID uniquenodeid;

    final UUID reservedStreamIDStart = new UUID(Long.MAX_VALUE-100, 0);
    final UUID reservedStreamIDStop = new UUID(Long.MAX_VALUE, 0);
    final UUID reservedRemoteReadMapID = new UUID(Long.MAX_VALUE, 0);

    IStreamFactory streamfactory;

    IRPCClient rpcc;
    IRPCServer rpcs;
    IRemoteReadMap rrmap;
    String rpchostname;
    int rpcportnum;


    public BaseRuntime(IStreamFactory fact, UUID tuniquenodeid, String trpchostname, int trpcportnum)
    {
        streamfactory = fact;
        objectmap = new HashMap();
        enginemap = new HashMap();
        uniquenodeid = tuniquenodeid;
        rpchostname = trpchostname;
        rpcportnum = trpcportnum;
        rpcc = new ThriftRPCClient();
        rpcs = new ThriftRPCServer();
        rrmap = new RemoteReadMap(fact.newStream(reservedRemoteReadMapID), uniquenodeid);
        // rpcs.registerHandler(rpcportnum, this);
    }

    CorfuDBObject getObject(UUID objectid) {
        synchronized (objectmap) {
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
    SMREngine getEngine(UUID objectid) {
        synchronized (enginemap) {
            if (!enginemap.containsKey(objectid)) return null;
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
                        throw new RuntimeException();
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
        CorfuDBObjectCommand retobj = (CorfuDBObjectCommand)rpcc.send(new Pair<UUID, Object>(cob.getID(), command), remoteruntime.first, remoteruntime.second);
        if(retobj==null)
            throw new RuntimeException("remote read returned null...");
        command.setReturnValue(retobj.getReturnValue());
    }

    //receives incoming RPCs
    @Override
    public Object deliverRPC(Object cmd)
    {
        Pair<UUID, Object> P = (Pair<UUID, Object>)cmd;
        //only queries are supported --- should we check this here, or just enforce it at the send point?
        SMREngine smre = getEngine(P.first);
        if(smre==null) //we aren't playing this stream; the client was misinformed
        {
            System.out.println("received RPC for object that we aren't playing");
            return null; //todo: should we return a cleaner error code instead?
        }
        try {
            smre.sync(null, P.second);
            return P.second;
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    //we don't lock the object; the sub-classing runtime is responsible for locking, if required,
    //before calling this method
    public void applyCommandToObject(UUID curstream, CorfuDBObjectCommand command, ITimestamp timestamp)
    {
        CorfuDBObject cob = getObject(curstream);
        if(cob==null) throw new RuntimeException("entry for stream " + curstream + " with no registered object");
        try
        {
            cob.applyToObject(command, timestamp);
        }
        catch(Exception e)
        {
//            System.out.println("caught exception " + e + " and setting exception on command");
            command.setException(e);
        }
        //todo: verify that it's okay for this to not be atomic with the apply
        //in the worst case, the object thinks it has an older version than it really does
        //but all that should cause is spurious aborts
        //the alternative is to have the apply in the object always call a superclass version of apply
        //that sets the timestamp
        //only the apply thread sets the timestamp, so we only have to worry about concurrent reads
        if(!(timestamp.equals(ITimestamp.getInvalidTimestamp())))
        {
            dbglog.debug("setting timestamp of object " + cob.getID() + " to " + timestamp);
            cob.setTimestamp(timestamp);
        }
        dbglog.debug("setting command timestamp to " + cob.getTimestamp());
        command.setTimestamp(cob.getTimestamp());
    }


}











