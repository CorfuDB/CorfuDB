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

import java.io.Serializable;
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
public class SimpleRuntime implements AbstractRuntime, SMRLearner
{
    //underlying SMREngine
    SMREngine smre;

    //map from object IDs to object instances; used for multiplexing
    Map<Long, CorfuDBObject> objectmap;

    /**
     * Registers an object with the runtime
     *
     * @param  obj  the object to register
     */
    public void registerObject(CorfuDBObject obj)
    {
        synchronized(objectmap)
        {
            if(objectmap.containsKey(obj.getID()))
            {
                System.out.println("object ID already registered!");
                throw new RuntimeException();
            }
            System.out.println("registering object ID " + obj.getID());
            objectmap.put(obj.getID(), obj);
        }
    }

    CorfuDBObject getObject(long objectid)
    {
        if(!objectmap.containsKey(objectid)) throw new RuntimeException("object not registered!");
        return objectmap.get(objectid);
    }


    /**
     * Creates a SimpleRuntime with an underlying SMR engine. Registers itself
     * as the SMR engine's learner.
     *
     * @param  tsmre  the object to register
     */
    public SimpleRuntime(SMREngine tsmre)
    {
        smre = tsmre;
        smre.registerLearner(this);
        objectmap = new HashMap<Long, CorfuDBObject>();

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
        Set<Long> streams = new HashSet<Long>();
        streams.add(cob.getID());
        smre.propose(update, streams, query);
    }

    public void update_helper(CorfuDBObject cob, Serializable update)
    {
        query_then_update_helper(cob, null, update);
    }


    public void query_helper(CorfuDBObject cob)
    {
        smre.sync();
    }

    public void apply(Object command, Set<Long> streams, long timestamp)
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
                //in the worst case, the object thinks it has an older version that it really does
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

}
