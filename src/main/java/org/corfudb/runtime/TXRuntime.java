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
        dbglog.debug("EndTX: " + curtx.get());
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
        //todo: remove the egregious copy
        txpos = smre.propose(curtx.get(), new HashSet(curtx.get().get_updatestreams().keySet()));
        dbglog.debug("appended endtx at position {}; now syncing...", txpos);


        //now that we appended the intention, we need to play the stream until the append point
        //todo: do this more efficiently so that each sync doesn't need to establish
        //a brand new linearization point by independently checking the tail of the underlying stream

        Iterator<TxIntReadSetEntry> it = curtx.get().get_readset().iterator();
        while(it.hasNext())
        {
            //for each stream in the readset, if we're playing it, sync
            long streamid = it.next().objectid;
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
        final int timeoutms = 10000;
        long startms = System.currentTimeMillis();
        while(true)
        {
            if(System.currentTimeMillis()-startms>timeoutms)
            {
                throw new RuntimeException("timeout on waiting for final decision for " + txpos);
            }
            dbglog.debug("EndTX checking for decision for intention at {}...", txpos);
            boolean ret = false;
            boolean dec = false;
            synchronized (decisionmap)
            {
                if (decisionmap.containsKey(txpos))
                {
                    //at this point we know the decision
                    //but it's possible that not all TXEngines have
                    //processed and applied the decision yet
                    //which means the next TX can see an inconsistent
                    //state and abort.
                    //for now, we simply trigger another sync
                    //to ensure that all TXEngines catch up
                    ret = true;
                    //return dec;
                }
            }
            //decision not found --- sync all the streams again
            //ideally, two kinds of nodes need to know about the final tx decision:
            //-- any node that's playing an update stream, since it has to know whether to update its state or not
            //-- the originating node of the tx
            //for the first category, it's sufficient to put decisions on the update streams
            //but for the second category, the originating node may only be playing the read streams
            //hence, we put decisions on all streams
            //todo: put decisions on read streams only if required (how do we determine this?)
            Iterator<Long> it2 = curtx.get().get_allstreams().keySet().iterator();
            while(it2.hasNext())
            {
                long streamid = it2.next();
                SMREngine smre2 = getEngine(streamid);
                if(smre2!=null)
                    smre2.sync();
            }
            if(ret)
            {
                synchronized(decisionmap)
                {
                    dec = decisionmap.get(txpos);
                    decisionmap.remove(txpos);
                }
                curtx.set(null);
                return dec;
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
//                System.out.println("read " + cob.getID() + " at " + cob.getTimestamp());
                curtx.get().mark_read(cob.getID(), cob.getTimestamp(), key);
                //do what here??? apply the command through the apply thread
                //todo: right now this causes an unnecessary sync
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
            SMREngine smre = getEngine(cob.getID());
            if(smre==null) throw new RuntimeException("updates not allowed on remote objects!");
            smre.propose(update, streams, query);
        }
        else //in a transactional context, buffer for now
        {
            if(query !=null)
            {
                query_helper(cob, key, query);
                //apply the precommand to the object
//                deliver(query, cob.getID(), streams, SMREngine.TIMESTAMP_INVALID);
            }
            curtx.get().buffer_update(update, streams.iterator().next(), key);
        }

    }

    public void update_helper(CorfuDBObject cob, CorfuDBObjectCommand update, Serializable key)
    {
        query_then_update_helper(cob, null, update, key);
    }


    public void deliver(Object command, long curstream, long timestamp)
    {
        throw new RuntimeException("this should never get called! SMR commands" +
                " are handled by individual TXEngines...");
    }


    //State shared by TXEngines:

    //todo: GC for partialdecisions

    Object partialinfolock = new Object();
    Map<Long, BitSet> partialdecisions = new HashMap();

    public void initDecisionState(long timestamp)
    {
        synchronized(partialinfolock)
        {
            if(!partialdecisions.containsKey(timestamp))
            {
                partialdecisions.put(timestamp, new BitSet());
            }
        }
    }

    public Pair<Boolean, Boolean> updateDecisionState(long timestamp, int streambitpos, int totalstreams, boolean partialdecision)
    {
        boolean decided = false;
        boolean commit = false;
        if(partialdecision)
        {
            synchronized (partialinfolock)
            {
                BitSet bs = partialdecisions.get(timestamp);
                if (bs == null)
                    throw new RuntimeException("bitset not found -- was initDecisionState called?");
                bs.set(streambitpos);
                if(bs.cardinality()==totalstreams)
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
        return new Pair(decided, commit);
        //System.out.println(timestamp + " " + streambitpos + " " + totalstreams + " " + partialdecision + " " + decided + " " + commit);
    }


    public void updateFinalDecision(long timestamp, boolean commit)
    {
        synchronized (decisionmap)
        {
            dbglog.debug("decided position {}", timestamp);
            decisionmap.put(timestamp, commit);
        }
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

    Map<Long, TxInt> pendingtxes = new HashMap();


    public void addPending(long timestamp, TxInt txint)
    {
        if(!pendingtxes.containsKey(timestamp))
        {
            pendingtxes.put(timestamp, txint);
        }
    }

    public TxInt getPending(long timestamp)
    {
        return pendingtxes.get(timestamp);
    }

    public void removePending(long timestamp)
    {
        pendingtxes.remove(timestamp);
    }

    //called by deliver
    public void process_tx_intention(Object command, long curstream, long timestamp)
    {
        dbglog.debug("process_tx_int " + curstream + "." + timestamp);

        if (txr.trackstats)
        {
            txr.ctr_numtxint.incrementAndGet();
        }

        TxInt T = (TxInt) command;

        txr.initDecisionState(timestamp);

        //generate partial decision
        // to enforce strict serializability, we use a simple rule:
        // has anything the transaction read changed since it was read?
        // if not, the transaction commits since the state it viewed is
        // essentially the same state it would have seen had it acquired
        // locks pessimistically.

        if(timestamp==SMREngine.TIMESTAMP_INVALID) throw new RuntimeException("validation timestamp cannot be invalid!");

        boolean partialabort = false;
        //we see the intention if we play a stream that's either in the read set or the write set
        //we only have to validate and generate a partial decision if the stream is in the read set
        //but we also generate a 'true' decision if the stream is in the write set
        //to handle the blind writes case
        //if stream appends are reliable, we can get rid of generating the txdec if we are only in the write set
        if(T.get_readstreams().get(curstream)!=null)
        {
            Iterator<TxIntReadSetEntry> readsit = T.get_readset().iterator();
            while (readsit.hasNext())
            {
                TxIntReadSetEntry curread = readsit.next();
                if (curread.objectid != curstream) continue; //validate only the current stream
                //is the current version of the object at a later timestamp than the version read by the transaction?
                if (txr.getObject(curread.objectid).getTimestamp(curread.key) > curread.readtimestamp)
                {
//                    System.out.println("partial decision is an abort: " + curread.objectid + ":" + curread.key + ":" + curread.readtimestamp + ":" + txr.getObject(curread.objectid).getTimestamp(curread.key));
                    partialabort = true;
                    break;
                }
            }

            if (!partialabort)
            {
                //we now need to check if the transaction conflicts with any of the pending transactions
                //to this object; if so, for now we abort immediately. in the future, we need to maintain
                //a dependency graph
                Iterator<TxInt> it = pendingtxes.values().iterator();
                while (it.hasNext())
                {
                    TxInt T2 = it.next();
                    //does T2 write something that T reads?
                    if (T.readsSomethingWrittenBy(T2))
                    {
                        partialabort = true;
                        break;
                    }
                }
            }
        }
        TxDec decrec = new TxDec(timestamp, curstream, !partialabort);
        //todo: remove this egregious copy
        smre.propose(decrec, new HashSet<Long>(T.get_allstreams().keySet()));

        //if stream appends are reliable, we can commit blind writes at this point

        //at this point, the transaction hasn't committed; we need to wait until we encounter
        //partial decisions (including the one we just inserted) to appear in the stream
        //so we stick this into a dependency graph of blocking transactions for now
        addPending(timestamp, T);

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



        TxDec decrec = (TxDec)command;

        dbglog.debug("process_tx_dec " + curstream + "." + timestamp + " for txint at " + curstream + "." + decrec.txint_timestamp);


        TxInt T = getPending(decrec.txint_timestamp);
        if(T==null) //already been decided and applied by this TXEngine
        {
            return;
        }



        //we index over all streams here; if stream appends are reliable, we can switch this to only readstreams
        Pair<Boolean, Boolean> P = txr.updateDecisionState(decrec.txint_timestamp, T.get_allstreams().get(decrec.stream), T.get_allstreams().size(), decrec.decision);

        if(txr.trackstats)
        {
            if(P.first)
            {
                if(P.second) txr.ctr_numcommits.incrementAndGet();
                else txr.ctr_numaborts.incrementAndGet();
            }
        }


        if(P.first) //final decision has been made
        {
            if(P.second) //... and is a commit
            {
                Iterator<TxIntWriteSetEntry> it = getPending(decrec.txint_timestamp).get_bufferedupdates().iterator();
                while (it.hasNext())
                {
                    TxIntWriteSetEntry P2 = it.next();
                    //no need to lock since each object can only be modified by its underlying TXEngine, which
                    //in turn is only entered by the underlying SMREngine thread.
                    //todo: do we have to do 2-phase locking?
                    //the only bad thing that can happen is that reads see an inconsistent state
                    //but this can happen anyway, and in this case the transaction will abort
                    //todo: think about providing tx opacity across the board
                    if (P2.objectid != curstream) continue;
                    CorfuDBObject cob = txr.getObject(P2.objectid);
                    if (cob == null) throw new RuntimeException("not a registered object!");
                    //cob.lock(true);
                    cob.applyToObject(P2.command);
                    cob.setTimestamp(decrec.txint_timestamp); //use the intention's timestamp
                    //cob.unlock(true);
//                    System.out.println("object " + cob.getID() + " timestamp set to " + cob.getTimestamp());
                }
            }
            removePending(decrec.txint_timestamp);
            //todo: if it's an abort, we can notify the app earlier
//            if(txr.updateApplyStatus(decrec.txint_timestamp, T.get_updatestreams().get(decrec.stream), T.get_updatestreams().size()))
                txr.updateFinalDecision(decrec.txint_timestamp, P.second); //notify the application

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

class TxIntReadSetEntry implements Serializable
{
    public long objectid;
    public long readtimestamp;
    public Serializable key;
    public TxIntReadSetEntry(long tobjid, long ttimestamp, Serializable tkey)
    {
        objectid = tobjid;
        readtimestamp = ttimestamp;
        key = tkey;
    }
}

class TxIntWriteSetEntry implements Serializable
{
    Serializable command;
    long objectid;
    Serializable key;
    public TxIntWriteSetEntry(Serializable tcommand, long tobjid, Serializable tkey)
    {
        command = tcommand;
        objectid = tobjid;
        key = tkey;
    }
}

class TxInt implements Serializable //todo: custom serialization
{
    //command, object, key
    private List<TxIntWriteSetEntry> bufferedupdates;
    private Map<Long, Integer> updatestreammap;

    //object, version, key
    private Set<TxIntReadSetEntry> readset;
    private Map<Long, Integer> readstreammap; //maps from a stream id to a number denoting the insertion order of that id
    private Map<Long, Integer> allstreammap; //todo: custom serialization so this doesnt get written out
    TxInt()
    {
        bufferedupdates = new LinkedList();
        readset = new HashSet();
        updatestreammap = new HashMap();
        readstreammap = new HashMap();
        allstreammap = new HashMap();
    }
    void buffer_update(Serializable bs, long stream, Serializable key)
    {
        bufferedupdates.add(new TxIntWriteSetEntry(bs, stream, key));
        updatestreammap.put(stream, updatestreammap.size());
        if(!allstreammap.containsKey(stream))
            allstreammap.put(stream, allstreammap.size());
    }
    void mark_read(long object, long version, Serializable key)
    {
        readset.add(new TxIntReadSetEntry(object, version, key));
        if(!readstreammap.containsKey(object))
            readstreammap.put(object, readstreammap.size());
        if(!allstreammap.containsKey(object))
        {
            allstreammap.put(object, allstreammap.size());
        }

    }
    Map<Long, Integer> get_updatestreams()
    {
        return updatestreammap;
    }
    Map<Long, Integer> get_readstreams()
    {
        return readstreammap;
    }
    Set<TxIntReadSetEntry> get_readset()
    {
        return readset;
    }
    List<TxIntWriteSetEntry> get_bufferedupdates()
    {
        return bufferedupdates;
    }
    Map<Long, Integer> get_allstreams()
    {
        return allstreammap;
    }

    public boolean readsSomethingWrittenBy(TxInt T2)
    {
        Iterator<TxIntReadSetEntry> it = get_readset().iterator();
        while(it.hasNext())
        {
            TxIntReadSetEntry Trip1 = it.next();
            Iterator<TxIntWriteSetEntry> it2 = T2.get_bufferedupdates().iterator();
            while(it2.hasNext())
            {
                TxIntWriteSetEntry Trip2 = it2.next();
                //objects match?
                if(Trip1.objectid==Trip2.objectid)
                {
                    //keys match?
                    if(Trip1.key==null || Trip2.key==null || Trip1.key.equals(Trip2.key))
                    {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public String toString()
    {
        return "TXINT: [[[WriteSet: " + bufferedupdates.toString() + "]]]\n"
                + "[[[ReadSet: " + readset.toString() + "]]]";
    }

}
