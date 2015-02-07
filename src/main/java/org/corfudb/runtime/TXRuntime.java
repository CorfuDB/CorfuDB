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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is a transactional runtime implementing the AbstractRuntime interface.
 * It extends SimpleRuntime and overloads apply, update_helper and query_helper.
 *
 */
public class TXRuntime extends BaseRuntime
{

    static Logger dbglog = LoggerFactory.getLogger(TXRuntime.class);

    final boolean trackstats = true;
    AtomicLong ctr_numcommits = new AtomicLong();
    AtomicLong ctr_numaborts = new AtomicLong();
    AtomicLong ctr_numundecided = new AtomicLong();
    AtomicLong ctr_numappliestx = new AtomicLong();
    AtomicLong ctr_numapplieslin = new AtomicLong();
    AtomicLong ctr_numapplieslocal = new AtomicLong();

    final ThreadLocal<TxInt> curtx = new ThreadLocal<TxInt>();

    //used to communicate decisions from the query_helper thread to waiting endtx calls
    final Map<Long, Boolean> decisionmap;

    public TXRuntime(StreamFactory fact, long uniquenodeid, String rpchostname, int rpcport)
    {
        super(fact, uniquenodeid, rpchostname, rpcport);
        decisionmap = new HashMap<Long, Boolean>();
    }

    public void BeginTX()
    {
        if (curtx.get() != null) //there's already an executing tx
            throw new RuntimeException("tx already executing!"); //should we do something different to support nested txes?
        curtx.set(new TxInt());
    }


    public boolean EndTX()
    {
        dbglog.debug("EndTX");
        long txpos = -1;
        //append the transaction intention
        if(curtx.get()==null) throw new RuntimeException("no current transaction!");
        if(curtx.get().get_updatestreams().size()==0)
        {
            if(curtx.get().get_readset().size()==0) // empty transaction
            {
                curtx.set(null);
                return true;
            }
            //read-only transaction
            else
            {
                //todo: this doesn't work anymore!
//                boolean ret = validate(curtx.get());
//                curtx.set(null);
//                if (ret) return true;
                curtx.set(null);
                return true;
            }
//            throw new RuntimeException("empty transaction!"); //todo: do something more sensible here
        }
        SMREngine smre = getEngine();
        if(smre==null) throw new RuntimeException("no engine found for appending tx!");
        txpos = smre.propose(curtx.get(), curtx.get().get_updatestreams());
        dbglog.debug("appended endtx at position {}; now syncing...", txpos);
        //now that we appended the intention, we need to play the stream until the append point
        //todo: do this more efficiently so that each sync doesn't need to establish
        //a brand new linearization point by independently checking the tail of the underlying stream
        Iterator<Triple<Long, Long, Serializable>> it = curtx.get().get_readset().iterator();
        while(it.hasNext())
        {
            long streamid = it.next().first;
            smre = getEngine(streamid);
            if(smre!=null)
            {
                smre.sync(txpos); //this results in a number of calls to apply, as each intervening intention is processed
                dbglog.debug("synced stream ", streamid);
            }
            else
            {
                //throw new RuntimeException("no engine found for read stream!");
                //remote read
            }
        }

        dbglog.debug("EndTX checking for decision for intention at {}...", txpos);
        //at this point there should be a decision
        //if not, for now we throw an error (but with decision records we'll keep syncing
        //until we find the decision)
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
            {
                //catastrophic error for now, while we debug
                System.out.println("dec not found for " + txpos);
                System.out.println(this);
                System.exit(0);
                throw new RuntimeException("decision not found!");
            }
        }
    }

    public void query_helper(CorfuDBObject cob)
    {
        query_helper(cob, null);
    }

    public void query_then_update_helper(CorfuDBObject cob, CorfuDBObjectCommand query, CorfuDBObjectCommand update)
    {
        query_then_update_helper(cob, query, update, null);
    }

    public void update_helper(CorfuDBObject cob, CorfuDBObjectCommand update)
    {
        update_helper(cob, update, null);
    }

    public void query_helper(CorfuDBObject cob, Serializable key)
    {
        query_helper(cob, key, null);
    }

    public void query_helper(CorfuDBObject cob, Serializable key, CorfuDBObjectCommand command)
    {
        if(curtx.get()==null) //non-transactional
        {
            SMREngine smre = getEngine(cob.getID());
            if(smre==null) //not playing stream
            {
                rpcRemoteRuntime(cob, command);
            }
            else
                smre.sync(SMREngine.TIMESTAMP_INVALID, command);
        }
        else
        {
            SMREngine smre = getEngine(cob.getID());
            if(smre!=null) //we're playing the object
            {
                curtx.get().mark_read(cob.getID(), cob.getTimestamp(), key);
                //do what here??? apply the command through the apply thread
                smre.sync(SMREngine.TIMESTAMP_INVALID, command);
            }
            else //it's a remote object
            {
                rpcRemoteRuntime(cob, command);
                //todo: we need to get the timestamp back from the remote node!!!
                //todo: for now just using the local object timestamp as a dummy value...
                curtx.get().mark_read(cob.getID(), cob.getTimestamp(), key);
            }
        }
    }

    public void query_then_update_helper(CorfuDBObject cob, CorfuDBObjectCommand query, CorfuDBObjectCommand update, Serializable key)
    {
        Set<Long> streams = new HashSet<Long>();
        streams.add(cob.getID());
        if(curtx.get()==null) //not in a transactional context, append immediately to the stream
        {
            //return super.query_then_update_helper(cob, query, update, streams);
            getEngine(cob.getID()).propose(update, streams, query); //todo: check return value of getEngine?
        }
        else //in a transactional context, buffer for now
        {
            if(query !=null)
            {
                query_helper(cob, key, query);
                //apply the precommand to the object
//                deliver(query, cob.getID(), streams, SMREngine.TIMESTAMP_INVALID);
            }
            curtx.get().buffer_update(update, streams.iterator().next());
        }

    }

    public void update_helper(CorfuDBObject cob, CorfuDBObjectCommand update, Serializable key)
    {
        query_then_update_helper(cob, null, update);
    }

    public void deliver(Object command, long curstream, Set<Long> streams, long timestamp)
    {
        dbglog.debug("deliver {}", timestamp);

        if (command instanceof TxInt) //is the command a transaction or a linearizable singleton?
        {
            if(trackstats)
            {
                ctr_numappliestx.incrementAndGet();
            }

            TxInt T = (TxInt)command;

            //should the transaction commit or abort?
            int decision = validate(T, curstream, timestamp);

            if(trackstats)
            {
                if(decision==VAL_COMMIT) ctr_numcommits.incrementAndGet();
                else if(decision==VAL_ABORT) ctr_numaborts.incrementAndGet();
                else if(decision==VAL_UNDECIDED) ctr_numundecided.incrementAndGet();
            }

            //update the decision map
            if(decision==VAL_COMMIT || decision==VAL_ABORT)
            {
                synchronized (decisionmap)
                {
                    dbglog.debug("decided position {}", timestamp);
                    decisionmap.put(timestamp, decision==VAL_COMMIT);
                }
            }

            //if decision is a commit, apply the changes
            if(decision==VAL_COMMIT)
            {
                Iterator<Pair<Serializable, Long>> it = T.get_bufferedupdates().iterator();
                while(it.hasNext())
                {
                    Pair<Serializable, Long> P = it.next();
//                    Set<Long> tstreams = new HashSet<Long>();
//                    tstreams.add(P.second);
                    //this apply upcall can be simultaneously called by different SMR threads!
                    //it has to be threadsafe! different threads can simultaneously
                    //try to enter the object's apply upcall...
                    //at the very least we need simple object locking.
                    //todo: do we have to do 2-phase locking?
                    //since all updates are funnelled through the apply thread
                    //the only bad thing that can happen is that reads see an inconsistent state
                    //but this can happen anyway, and in this case the transaction will abort
                    //todo: think about providing tx opacity across the board
                    CorfuDBObject cob = getObject(P.second);
                    if(cob==null) throw new RuntimeException("not a registered object!");
                    cob.lock(true);
                    //super.apply(P.first, curstream, tstreams, timestamp); //let SimpleRuntime do the object multiplexing
                    cob.applyToObject(P.first);
                    cob.setTimestamp(timestamp);
                    cob.unlock(true);
                }
            }
        }
        else
        {
            if(trackstats)
            {
                if(timestamp!=SMREngine.TIMESTAMP_INVALID)
                    ctr_numapplieslin.incrementAndGet();
                else
                    ctr_numapplieslocal.incrementAndGet();
            }
            applyCommandToObject(curstream, command, timestamp);
        }

        dbglog.debug("done with deliver {}", timestamp);
    }

    //the first bitset indicates whether the decision has been made or not
    //the second bitset indicates whether the decision is a commit or an abort
    Map<Long, Pair<BitSet, BitSet>> decisionbits = new HashMap();
    Lock decbitslock = new ReentrantLock();

    final int VAL_ABORT = 0;
    final int VAL_COMMIT = 1;
    final int VAL_UNDECIDED = 2;

    //this code needs to be threadsafe since multiple SMRs can call into it simultaneously
    //even though each SMR is single-threaded.
    int validate(TxInt newtx, long curstream, long timestamp)
    {
  //      System.out.println("validate " + timestamp + " in stream " + curstream);
        // to enforce strict serializability, we use a simple rule:
        // has anything the transaction read changed since it was read?
        // if not, the transaction commits since the state it viewed is
        // essentially the same state it would have seen had it acquired
        // locks pessimistically.

        if(timestamp==SMREngine.TIMESTAMP_INVALID) throw new RuntimeException("validation timestamp cannot be invalid!");

        decbitslock.lock();
        Pair<BitSet, BitSet> P = null;
        if(decisionbits.containsKey(timestamp))
            P = decisionbits.get(timestamp);
        else
        {
            P = new Pair<BitSet, BitSet>(new BitSet(), new BitSet());
            decisionbits.put(timestamp, P);
        }
        decbitslock.unlock();

        BitSet decided = P.first;
        BitSet committed = P.second;


        if(decided.cardinality()==committed.cardinality())
        {
            boolean partialabort = false;
            Iterator<Triple<Long, Long, Serializable>> readsit = newtx.get_readset().iterator();
            while (readsit.hasNext())
            {
                Triple<Long, Long, Serializable> curread = readsit.next();
                if (curread.first != curstream) continue; //validate only the current stream
                //is the current version of the object at a later timestamp than the version read by the transaction?
                if (getObject(curread.first).getTimestamp(curread.third) > curread.second)
                {
                    partialabort = true;
                    break;
                }
            }
            int streampos = newtx.get_readstreams().get(curstream);
            synchronized(P) //using P to guard decided/committed bitsets
            {
                decided.set(streampos);
                if (!partialabort) committed.set(streampos);
            }
        }

        int ret;
        synchronized(P) //using P to guard decided/committed bitsets
        {
            if (decided.cardinality() != committed.cardinality())
            {
                //transaction is an abort
                ret = VAL_ABORT;
            }
            else if (decided.cardinality() == newtx.get_readstreams().size()) //all streams have been decided
            {
                //transaction is a commit
                ret = VAL_COMMIT;
            }
            else
            {
                //not enough information yet
                ret = VAL_UNDECIDED;
                //dbglog.warn("undec " + timestamp + " " + decided.cardinality() + " " + committed.cardinality() + " " + newtx.get_readset().size() + " " + newtx.get_readstreams().size() + " " + curstream);
            }
        }
        dbglog.debug("DECISION = {}", ret);
        return ret;
    }

    public String toString()
    {
        String x = "TXRuntime: " + ctr_numappliestx.get() + " tx applies; "
                + ctr_numcommits.get() + " commits; "
                + ctr_numaborts.get() + " aborts; "
                + ctr_numundecided.get() + " undecided; "
                + ctr_numapplieslin.get() + " lin applies; "
                + ctr_numapplieslocal.get() + " local applies;";
        return x;
    }

}

class TxInt implements Serializable //todo: custom serialization
{
    private List<Pair<Serializable, Long>> bufferedupdates;
    private Set<Long> updatestreamset;
    private Set<Triple<Long, Long, Serializable>> readset;
    private Map<Long, Integer> readstreammap; //maps from a stream id to a number denoting the insertion order of that id
    TxInt()
    {
        bufferedupdates = new LinkedList<Pair<Serializable, Long>>();
        readset = new HashSet();
        updatestreamset = new HashSet<Long>();
        readstreammap = new HashMap();
    }
    void buffer_update(Serializable bs, long stream)
    {
        bufferedupdates.add(new Pair<Serializable, Long>(bs, stream));
        updatestreamset.add(stream);
    }
    void mark_read(long object, long version, Serializable key)
    {
        readset.add(new Triple(object, version, key));
        if(!readstreammap.containsKey(object))
            readstreammap.put(object, readstreammap.size());
    }
    Set<Long> get_updatestreams()
    {
        return updatestreamset;
    }
    Map<Long, Integer> get_readstreams()
    {
        return readstreammap;
    }
    Set<Triple<Long, Long, Serializable>> get_readset()
    {
        return readset;
    }
    List<Pair<Serializable, Long>> get_bufferedupdates()
    {
        return bufferedupdates;
    }
}
