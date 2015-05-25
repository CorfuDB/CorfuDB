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

import org.corfudb.runtime.smr.IStreamFactory;
import org.corfudb.runtime.smr.Pair;
import org.corfudb.runtime.stream.ITimestamp;

import java.io.*;
import java.util.*;


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
    public SimpleRuntime(IStreamFactory fact, UUID tuniquenodeid, String trpchostname, int trpcportnum)
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

    public void AbortTX()
    {
        throw new RuntimeException("SimpleRuntime does not support transactions.");
    }

    public void query_then_update_helper(CorfuDBObject cob, CorfuDBObjectCommand query, CorfuDBObjectCommand update)
    {
        query_then_update_helper(cob, query, update, null);
    }

    public void query_then_update_helper(CorfuDBObject cob, CorfuDBObjectCommand query, CorfuDBObjectCommand update, Serializable key) {
        Set<UUID> streams = new HashSet<UUID>();
        streams.add(cob.getID());
        SMREngine smre = getEngine(cob.getID());
        if (smre == null) // we are not playing this stream
        {
            Pair<String, Integer> remotenode = rrmap.getRemoteRuntime(cob.getID());
            if (remotenode == null) // we can't locate a remote runtime to read from either
                throw new RuntimeException("Cant find object in system");
            throw new RuntimeException("remote objects do not support query_then_update_helper");
        }
        try {
            getEngine(cob.getID()).propose(update, streams, query);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void update_helper(CorfuDBObject cob, CorfuDBObjectCommand update)
    {
        update_helper(cob, update, null);
    }

    public void update_helper(CorfuDBObject cob, CorfuDBObjectCommand update, Serializable key)
    {
        query_then_update_helper(cob, null, update);
    }

    public boolean query_helper(CorfuDBObject cob)
    {
        return query_helper(cob, null);
    }

    public boolean query_helper(CorfuDBObject cob, Serializable key)
    {
        return query_helper(cob, key, null);
    }


    public boolean query_helper(CorfuDBObject cob, Serializable key, CorfuDBObjectCommand command)
    {
        SMREngine smre = getEngine(cob.getID());
        if(smre==null) //not playing stream
        {
            rpcRemoteRuntime(cob, command);
        }
        else {
            try {
                smre.sync(ITimestamp.getInvalidTimestamp(), command);
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    public void deliver(Object command, UUID curstream, ITimestamp timestamp)
    {
        //we don't have to lock the object --- there's one thread per SMREngine,
        //and exactly one SMREngine per stream/object
        applyCommandToObject(curstream, (CorfuDBObjectCommand)command, timestamp);
    }

}

