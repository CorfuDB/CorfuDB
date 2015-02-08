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

/**
 * This class is a transactional runtime implementing the AbstractRuntime interface.
 * It extends SimpleRuntime and overloads apply, update_helper and query_helper.
 *
 */
public class TXRuntime extends BaseRuntime
{

    static Logger dbglog = LoggerFactory.getLogger(TXRuntime.class);


    final ThreadLocal<TxInt> curtx = new ThreadLocal<TxInt>();

    //used to communicate decisions from the query_helper thread to waiting endtx calls
    final Map<Long, Boolean> decisionmap;

    Map<Long, TXEngine> txenginemap = new HashMap<Long, TXEngine>();

    public TXEngine getTXEngine(long streamid)
    {
        synchronized(txenginemap)
        {
            return txenginemap.get(streamid);
        }
    }

    public void registerObject(CorfuDBObject cob)
    {
        registerObject(cob, false);
    }

    public void registerObject(CorfuDBObject cob, boolean remote)
    {
        super.registerObject(cob, remote);
        if(!remote)
        {
            synchronized(txenginemap)
            {
                txenginemap.put(cob.getID(), new TXEngine(cob, getEngine(cob.getID()), this));
            }
        }
    }


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

        //check if it's read-only or empty
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


        //append the intention
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
            //for each stream in the readset, if we're playing it, sync
            long streamid = it.next().first;
            smre = getEngine(streamid);
            if(smre!=null)
            {
                smre.sync(txpos); //this results in a number of calls to deliver, as each intervening intention is processed
                dbglog.debug("synced stream ", streamid);
            }
            else
            {
                //throw new RuntimeException("no engine found for read stream!");
                //remote read
            }
        }
        final int maxtries = Integer.MAX_VALUE;
        int numtries = 0;

        while(true)
        {
            numtries++;
            if(numtries>maxtries)
            {
                throw new RuntimeException("something wrong -- too many decision check retries!");
            }
            dbglog.debug("EndTX checking for decision for intention at {}...", txpos);
            synchronized (decisionmap)
            {
                if (decisionmap.containsKey(txpos))
                {
                    boolean dec = decisionmap.get(txpos);
                    decisionmap.remove(txpos);
                    curtx.set(null);
                    return dec;
                }
//                else
//                {
//                    //catastrophic error for now, while we debug
//                    System.out.println("dec not found for " + txpos);
//                    System.out.println(this);
//                    System.exit(0);
//                    throw new RuntimeException("decision not found!");
//                }
            }
            //decision not found --- sync all the streams again
            //ideally, two kinds of nodes need to know about the final tx decision:
            //-- any node that's playing an update stream, since it has to know whether to update its state or not
            //-- the originating node of the tx
            //for the first category, it's sufficient to put decisions on the update streams
            //but for the second category, the originating node may only be playing the read streams
            //hence, we put decisions on all streams
            //todo: put decisions on read streams only if required (how do we determine this?)
            Iterator<Long> it2 = curtx.get().get_updatestreams().iterator();
            while(it2.hasNext())
            {
                long streamid = it2.next();
                SMREngine smre2 = getEngine(streamid);
                if(smre2!=null)
                    smre2.sync();
            }
            it2 = curtx.get().get_readstreams().keySet().iterator();
            while(it2.hasNext())
            {
                long streamid = it2.next();
                SMREngine smre2 = getEngine(streamid);
                if(smre2!=null)
                    smre2.sync();
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


    public void deliver(Object command, long curstream, long timestamp)
    {
        throw new RuntimeException("this should never get called! SMR commands" +
                " are handled by individual TXEngines...");
    }

    Object partialinfolock = new Object();
    Map<Long, BitSet> partialdecisions = new HashMap();
    Map<Long, TxInt> pendingtxes = new HashMap();


    public void addPending(long timestamp, TxInt txint)
    {
        synchronized(partialinfolock)
        {
            if(!pendingtxes.containsKey(timestamp))
            {
                pendingtxes.put(timestamp, txint);
                partialdecisions.put(timestamp, new BitSet());
            }

        }
    }

    public TxInt getPending(long timestamp)
    {
        synchronized(partialinfolock)
        {
            return pendingtxes.get(timestamp);
        }
    }

    public Pair<Boolean, Boolean> updateDecision(long timestamp, long stream, boolean partialdecision)
    {
        boolean decided = false;
        boolean commit = false;
        if(partialdecision)
        {
            TxInt txint;
            synchronized (partialinfolock)
            {
                txint = pendingtxes.get(timestamp);
                if (txint == null)
                    throw new RuntimeException("pending tx not found -- was addPending called?");
                BitSet bs = partialdecisions.get(timestamp);
                if (bs == null)
                    throw new RuntimeException("bitset not found -- was addPending called?");
                bs.set(txint.get_readstreams().get(stream));
                if(bs.cardinality()==txint.get_readstreams().size())
                {
                    decided = true;
                    commit = true;
                }
            }
        }
        else
        {
            decided = true;
            commit = false;
        }
        if(decided)
        {
            synchronized (decisionmap)
            {
                dbglog.debug("decided position {}", timestamp);
                decisionmap.put(timestamp, commit);
            }
        }
        return new Pair(decided, commit);
    }

    final boolean trackstats = true;
    AtomicLong ctr_numcommits = new AtomicLong();
    AtomicLong ctr_numaborts = new AtomicLong();
    AtomicLong ctr_numtxint = new AtomicLong();
    AtomicLong ctr_numtxdec = new AtomicLong();
    AtomicLong ctr_numapplieslin = new AtomicLong();
    AtomicLong ctr_numapplieslocal = new AtomicLong();


    public String toString()
    {
        String x = "TXRuntime: " + ctr_numtxint.get() + " txints; "
                + ctr_numtxdec.get() + " decrecs; "
                + ctr_numcommits.get() + " commits; "
                + ctr_numaborts.get() + " aborts; "
                + ctr_numapplieslin.get() + " lin applies; "
                + ctr_numapplieslocal.get() + " local applies;";
        return x;
    }

}

class TXEngine implements SMRLearner
{
    static Logger dbglog = LoggerFactory.getLogger(TXEngine.class);

    SMREngine smre;
    CorfuDBObject cob;
    TXRuntime txr;



    public TXEngine(CorfuDBObject tcob, SMREngine tsmre, TXRuntime ttxr)
    {
        smre = tsmre;
        cob = tcob;
        txr = ttxr;
        smre.registerLearner(this); //overwrites any existing learner (i.e., TXRuntime)
    }

    @Override
    public void deliver(Object command, long curstream, long timestamp)
    {
        dbglog.debug("deliver {}", timestamp);

        if (command instanceof TxInt) //is the command a transaction or a linearizable singleton?
        {
            process_tx_intention(command, curstream, timestamp);
        }
        else if(command instanceof TxDec)
        {
            process_tx_decision(command, curstream, timestamp);
        }
        else
        {
            process_lin_singleton(command, curstream, timestamp);
        }

        dbglog.debug("done with deliver {}", timestamp);

    }

    //called by deliver
    public void process_tx_intention(Object command, long curstream, long timestamp)
    {


        if (txr.trackstats)
        {
            txr.ctr_numtxint.incrementAndGet();
        }

        TxInt T = (TxInt) command;

        txr.addPending(timestamp, T);

        //generate partial decision
        validate(T, curstream, timestamp);
    }


    public void process_tx_decision(Object command, long curstream, long timestamp)
    {
        if (txr.trackstats)
        {
            txr.ctr_numtxdec.incrementAndGet();
        }


//        if(trackstats)
//        {
//            if(decision==VAL_COMMIT) ctr_numcommits.incrementAndGet();
//            else if(decision==VAL_ABORT) ctr_numaborts.incrementAndGet();
//            else if(decision==VAL_UNDECIDED) ctr_numundecided.incrementAndGet();
//        }

//        //update the decision map
//        if(decision==VAL_COMMIT || decision==VAL_ABORT)
//        {
//            txr.updateDecision(timestamp, decision==VAL_COMMIT);
//        }


        TxDec decrec = (TxDec)command;

        Pair<Boolean, Boolean> P = txr.updateDecision(decrec.txint_timestamp, decrec.stream, decrec.decision);

        if(txr.trackstats)
        {
            if(P.first)
            {
                if(P.second) txr.ctr_numcommits.incrementAndGet();
                else txr.ctr_numaborts.incrementAndGet();
            }
        }



        //if decision is a commit, apply the changes
        if(P.first && P.second)
        {
            Iterator<Pair<Serializable, Long>> it = txr.getPending(decrec.txint_timestamp).get_bufferedupdates().iterator();
            while(it.hasNext())
            {
                Pair<Serializable, Long> P2 = it.next();
                //no need to lock since each object can only be modified by its underlying TXEngine, which
                //in turn is only entered by the underlying SMREngine thread.
                //todo: do we have to do 2-phase locking?
                //the only bad thing that can happen is that reads see an inconsistent state
                //but this can happen anyway, and in this case the transaction will abort
                //todo: think about providing tx opacity across the board
                if(P2.second!=curstream) continue;
                CorfuDBObject cob = txr.getObject(P2.second);
                if(cob==null) throw new RuntimeException("not a registered object!");
                //cob.lock(true);
                cob.applyToObject(P2.first);
                cob.setTimestamp(timestamp);
                //cob.unlock(true);
            }
        }
    }

    public void process_lin_singleton(Object command, long curstream, long timestamp)
    {
        if(txr.trackstats)
        {
            if(timestamp!=SMREngine.TIMESTAMP_INVALID)
                txr.ctr_numapplieslin.incrementAndGet();
            else
                txr.ctr_numapplieslocal.incrementAndGet();
        }
        txr.applyCommandToObject(curstream, command, timestamp);
    }

    //this code does not need to be threadsafe; each TXEngine is
    //coupled with exactly one SMREngine object, hence only
    //the single SMREngine thread will upcall into this function
    void validate(TxInt newtx, long curstream, long timestamp)
    {
        //      System.out.println("validate " + timestamp + " in stream " + curstream);
        // to enforce strict serializability, we use a simple rule:
        // has anything the transaction read changed since it was read?
        // if not, the transaction commits since the state it viewed is
        // essentially the same state it would have seen had it acquired
        // locks pessimistically.

        if(timestamp==SMREngine.TIMESTAMP_INVALID) throw new RuntimeException("validation timestamp cannot be invalid!");



        boolean partialabort = false;
        Iterator<Triple<Long, Long, Serializable>> readsit = newtx.get_readset().iterator();
        while (readsit.hasNext())
        {
            Triple<Long, Long, Serializable> curread = readsit.next();
            if (curread.first != curstream) continue; //validate only the current stream
            //is the current version of the object at a later timestamp than the version read by the transaction?
            if (txr.getObject(curread.first).getTimestamp(curread.third) > curread.second)
            {
                partialabort = true;
                break;
            }
        }
        TxDec decrec = new TxDec(timestamp, curstream, !partialabort);
        smre.propose(decrec, newtx.get_allstreams());

    }


}

class TxDec implements Serializable
{
    long stream;
    boolean decision; //true means commit, false means abort
    long txint_timestamp;
    public TxDec(long t_txint_timestamp, long t_stream, boolean t_dec)
    {
        txint_timestamp = t_txint_timestamp;
        stream = t_stream;
        decision = t_dec;
    }

}


class TxInt implements Serializable //todo: custom serialization
{
    private List<Pair<Serializable, Long>> bufferedupdates;
    private Set<Long> updatestreamset;
    private Set<Triple<Long, Long, Serializable>> readset;
    private Map<Long, Integer> readstreammap; //maps from a stream id to a number denoting the insertion order of that id
    private Set<Long> allstreamset; //todo: custom serialization so this doesnt get written out
    TxInt()
    {
        bufferedupdates = new LinkedList<Pair<Serializable, Long>>();
        readset = new HashSet();
        updatestreamset = new HashSet<Long>();
        readstreammap = new HashMap();
        allstreamset = new HashSet();
    }
    void buffer_update(Serializable bs, long stream)
    {
        bufferedupdates.add(new Pair<Serializable, Long>(bs, stream));
        updatestreamset.add(stream);
        allstreamset.add(stream);
    }
    void mark_read(long object, long version, Serializable key)
    {
        readset.add(new Triple(object, version, key));
        if(!readstreammap.containsKey(object))
            readstreammap.put(object, readstreammap.size());
        allstreamset.add(object);
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
    Set<Long> get_allstreams()
    {
        return allstreamset;
    }
}
