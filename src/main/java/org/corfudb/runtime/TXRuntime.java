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
import java.util.*;

/**
 * This class is a transactional runtime, implementing the AbstractRuntime interface
 * plus BeginTX/EndTX calls. It extends SimpleRuntime and overloads apply, update_helper and query_helper.
 *
 */
public class TXRuntime extends SimpleRuntime
{

    final ThreadLocal<TxInt> curtx = new ThreadLocal<TxInt>();

    //used to communicate decisions from the query_helper thread to waiting endtx calls
    final Map<Long, Boolean> decisionmap;

    public TXRuntime(SMREngine smre)
    {
        super(smre);
        decisionmap = new HashMap<Long, Boolean>();
    }

    void BeginTX()
    {
        if (curtx.get() != null) //there's already an executing tx
            throw new RuntimeException("tx already executing!"); //should we do something different to support nested txes?
        curtx.set(new TxInt());
    }


    boolean EndTX()
    {
        System.out.println("EndTX");
        long txpos = -1;
        //append the transaction intention
        //txpos = curbundle.append(BufferStack.serialize(curtx.get()), curtx.get().get_streams());
        //txpos = super.query_then_update_helper(null, null, curtx.get(), curtx.get().get_streams());
        txpos = smre.propose(curtx.get(), curtx.get().get_streams());
        //now that we appended the intention, we need to play the bundle until the append point
        smre.sync(txpos);
        //at this point there should be a decision
        //if not, for now we throw an error (but with decision records we'll keep syncing
        //until we find the decision)
        System.out.println("appended endtx at position " + txpos);
        synchronized (decisionmap)
        {
            if (decisionmap.containsKey(txpos))
            {
                boolean dec = decisionmap.get(txpos);
                decisionmap.remove(txpos);
                curtx.set(null);
                return dec;
            }
            else
                throw new RuntimeException("decision not found!");
        }
    }

    public void query_helper(CorfuDBObject cob)
    {
        if(curtx.get()==null) //non-transactional, pass through
        {
            super.query_helper(cob);
        }
        else
        {
            curtx.get().mark_read(cob.getID(), cob.getTimestamp());
        }
    }

    public void query_then_update_helper(CorfuDBObject cob, Object query, Serializable update, Set<Long> streams)
    {
        if(curtx.get()==null) //not in a transactional context, append immediately to the streambundle as a singleton tx
        {
            //what about the weird case where the application proposes a TxInt? Can we assume
            //this won't happen since TxInt is not a public class?
            if(update instanceof TxInt) throw new RuntimeException("app cant update_helper a txint");
            //return super.query_then_update_helper(cob, query, update, streams);
            smre.propose(update, streams, query);
        }
        else //in a transactional context, buffer for now
        {
            if(query !=null)
            {
                //mark the read set
                query_helper(cob);
                //apply the precommand to the object
                apply(query, streams, SMREngine.TIMESTAMP_INVALID);
            }
            curtx.get().buffer_update(update, streams.iterator().next());
        }

    }

    public void update_helper(CorfuDBObject cob, Serializable update, Set<Long> streams)
    {
        query_then_update_helper(cob, null, update, streams);
    }

    public void apply(Object command, Set<Long> streams, long timestamp)
    {
        if (command instanceof TxInt)
        {
            TxInt T = (TxInt)command;
            if(process_txint(T, timestamp))
            {
                Iterator<Pair<Serializable, Long>> it = T.bufferedupdates.iterator();
                while(it.hasNext())
                {
                    Pair<Serializable, Long> P = it.next();
                    Set<Long> tstreams = new HashSet<Long>();
                    tstreams.add(P.second);
                    //todo: do we have to do 2-phase locking?
                    //since all updates are funnelled through the apply thread
                    //the only bad thing that can happen is that reads see a non-transactional state
                    //but this can happen anyway, and in this case the transaction will abort
                    super.apply(P.first, tstreams, timestamp);
                }
            }
        }
        else
            super.apply(command, streams, timestamp);
    }

    boolean process_txint(TxInt newtx, long timestamp)
    {
        synchronized(decisionmap)
        {
            System.out.println("decided position " + timestamp);
            decisionmap.put(timestamp, true);
            return true;
        }
    }
}

class TxInt implements Serializable //todo: custom serialization
{
    List<Pair<Serializable, Long>> bufferedupdates;
    Set<Long> streamset;
    Set<Pair<Long, Long>> readset;
    TxInt()
    {
        bufferedupdates = new LinkedList<Pair<Serializable, Long>>();
        readset = new HashSet<Pair<Long, Long>>();
        streamset = new HashSet<Long>();
    }
    void buffer_update(Serializable bs, long stream)
    {
        bufferedupdates.add(new Pair<Serializable, Long>(bs, stream));
        streamset.add(stream);
    }
    void mark_read(long object, long version)
    {
        readset.add(new Pair(object, version));
    }
    Set<Long> get_streams()
    {
        return streamset;
    }
    Set<Pair<Long, Long>> get_readset()
    {
        return readset;
    }
    List<Pair<Serializable, Long>> get_bufferedupdates()
    {
        return bufferedupdates;
    }
}
