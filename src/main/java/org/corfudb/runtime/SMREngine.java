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

import org.corfudb.client.ITimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class is an SMR engine. It's unaware of CorfuDB objects (or transactions).
 * It accepts new commands, totally orders them with respect to other clients in the
 * distributed system, and funnels them in order to its registered learner. The learner
 * is typically a CorfuDB runtime (which in turn supports objects and/or transactions)
 * but can also be any conventional SMR-based application.
 */
public class SMREngine
{
    private static Logger dbglog = LoggerFactory.getLogger(SMREngine.class);

    SMRLearner smrlearner;

    //used to coordinate between querying threads and the query_helper thread
    final Object queuelock;

    //pair of queues that get rotated between the playback thread and the sync threads
    List<SyncObjectWrapper> curqueue;
    List<SyncObjectWrapper> procqueue;

    Set<Long> defaultstreamset;
    Stream curstream;

    //list of pending commands -- when we encounter entries in the log
    //we check this list to see if the entry was appended by us.
    //in that case we retrieve the original version of the object and pass
    //it back to the application. this allows context to flow between
    //the proposing thread and the apply upcall in the learner.
    //Each command consists of the primary command, which is serialized into the
    //total order, and a secondary command that is not inserted into the total order
    //but executes locally just before the primary command
    HashMap<Long, Pair<Serializable, Object>> pendingcommands = new HashMap<Long, Pair<Serializable, Object>>();
    Lock pendinglock = new ReentrantLock();

    public void registerLearner(SMRLearner tlearner)
    {
        smrlearner = tlearner;
    }

    long uniquenodeid;

    public SMREngine(Stream sb, long tuniquenodeid)
    {
        // System.out.println("Creating new SMR engine on stream " + sb.getStreamID() + " with node id " + tuniquenodeid);
        curstream = sb;
        uniquenodeid = tuniquenodeid;
        SMRCommandWrapper.initialize(uniquenodeid);

        queuelock = new Object();
        curqueue = new LinkedList();
        procqueue = new LinkedList();

        defaultstreamset = new HashSet();
        defaultstreamset.add(sb.getStreamID());


        //start the playback thread
        new Thread(new Runnable()
        {
            public void run()
            {
                while(true)
                {
                    playback();
                }
            }
        }).start();


    }

    public ITimestamp propose(Serializable update, Set<Long> streams, Object precommand)
    {
        return propose(update, streams, precommand, false);
    }

    /**
     * Proposes a new command to the SMR total order.
     *
     * @param update The primary command to be appended to the total order
     * @param streams The streams to which the command should be appended
     * @param precommand A secondary command to be played immediately before the primary command;
     *                   this command is not inserted into the total order,
     *                   but is played at the same point as the primary order.
     *                   It's typically used to insert a read operation that must execute atomically
     *                   with a subsequent write operation.
     * @return
     */
    public ITimestamp propose(Serializable update, Set<Long> streams, Object precommand, boolean sync)
    {
        SMRCommandWrapper cmd = new SMRCommandWrapper(update, streams);
        pendinglock.lock();
        pendingcommands.put(cmd.uniqueid.second, new Pair(update, precommand));
//        System.out.println("putting " + cmd + " as a pending local command");
        pendinglock.unlock();
        ITimestamp pos = curstream.append(cmd, streams);
        if (precommand != null || sync)
        //we play until the append point --- this may be sub-optimal in some cases where we want to append
        //but not play immediately (or ever)
            sync(pos);
        return pos;
    }

    public ITimestamp propose(Serializable update)
    {
        return propose(update, defaultstreamset);
    }

    public ITimestamp propose(Serializable update, Set<Long> streams)
    {
        return propose(update, streams, null);
    }


    class SyncObjectWrapper
    {
        boolean preapply = false;
        Object synccommand;
        public SyncObjectWrapper(Object t)
        {
            this(t, false);
        }
        public SyncObjectWrapper(Object t, boolean tpreapply)
        {
            synccommand = t;
            preapply = tpreapply;
        }
    }

    /** returns once log has been played by playback thread
     * until syncpos, inclusive.
     * the command can be applied anytime after syncpos has been
     * reached. syncpos is merely a hint to reduce the latency
     * of the sync call.
     * todo: right now syncpos is ignored unless it's TIMESTAMP_INVALID
     *
     * if syncpos == timestamp_invalid, the command should be applied
     * immediately before/without syncing;
     * if syncpos == timestamp_max, the command should be applied after
     * syncing to the current tail.
     */
    public void sync(ITimestamp syncpos, Object command)
    {
        final SyncObjectWrapper syncobj = new SyncObjectWrapper(command, (syncpos.equals(ITimestamp.getInvalidTimestamp())));
        synchronized (syncobj)
        {
            synchronized(queuelock)
            {
                curqueue.add(syncobj);
                if(curqueue.size()==1) //first item, may need to wake up playback thread
                    queuelock.notify();
            }
            try
            {
                syncobj.wait();
            }
            catch (InterruptedException ie)
            {
                throw new RuntimeException(ie);
            }
        }
    }

    public void sync(ITimestamp syncpos)
    {
        sync(syncpos, null);
    }

    public void sync()
    {
        sync(ITimestamp.getMaxTimestamp(), null);
    }

    //runs in a single thread
    void playback()
    {
        List<SyncObjectWrapper> tqueue;
        synchronized(queuelock)
        {
            while(curqueue.size()==0)
            {
                try
                {
                    queuelock.wait();
                }
                catch(InterruptedException e)
                {
                    //do nothing
                }
            }
            //to ensure linearizability, any pending queries have to wait for the conclusion of
            //a checkTail that started *after* they were issued. accordingly, when playback starts up,
            //it rotates out the current queue of pending requests to stop new requests from entering it
            tqueue = procqueue;
            procqueue = curqueue;
            curqueue = tqueue;
        }
//        System.out.println("playback woken up...");

        if(procqueue.size()==0) throw new RuntimeException("queue cannot be empty at this point!");

//        System.out.println("playback continuing...");

        //check for pre-applies
        //todo: make this more efficient
        Iterator<SyncObjectWrapper> it2 = procqueue.iterator();
        while(it2.hasNext())
        {
            SyncObjectWrapper sw = it2.next();
            if(sw.preapply)
            {
                if(sw.synccommand!=null)
                {
                    smrlearner.deliver(sw.synccommand, curstream.getStreamID(), ITimestamp.getInvalidTimestamp());
                }
                synchronized(sw)
                {
                    sw.notifyAll();
                }
                it2.remove();
            }
        }
        if(procqueue.size()==0) return;

        //check the current tail of the stream, and then read the stream until that position
        ITimestamp curtail = curstream.checkTail();


        dbglog.debug("picked up sync batch of size {}; syncing until {}", procqueue.size(), curtail);

        StreamEntry update = curstream.readNext(curtail);
        while(update!=null)
        {
//            System.out.println("SMREngine got message in stream " + curstream.getStreamID() + " with learner " +
//                            smrlearner + " of class " + smrlearner.getClass());
            SMRCommandWrapper cmdw = (SMRCommandWrapper)update.getPayload();
            //if this command was generated by us, swap out the version we read back with the local version
            //this allows return values to be transmitted via the local command object
            pendinglock.lock();
            Pair<Serializable, Object> localcmds = null;
            //did we generate this command, and is it pending?
//            System.out.println(uniquenodeid + " is checking for local command on " + cmdw.uniqueid);
//            System.out.println(cmdw.uniqueid.first==uniquenodeid);
///            System.out.println(pendingcommands);
            if(cmdw.uniqueid.first==uniquenodeid && pendingcommands.containsKey(cmdw.uniqueid.second))
                localcmds = pendingcommands.remove(cmdw.uniqueid.second);
            pendinglock.unlock();
            if(smrlearner==null) throw new RuntimeException("smr learner not set!");
            if(localcmds!=null)
            {
                if (localcmds.second != null)
                {
//                    System.out.println("deliver local command precommand " + localcmds.second);
                    smrlearner.deliver(localcmds.second, curstream.getStreamID(), ITimestamp.getInvalidTimestamp());
                }
//                System.out.println("deliver local command " + localcmds.first);
                smrlearner.deliver(localcmds.first, curstream.getStreamID(), update.getLogpos());
            }
            else
            {
//                System.out.println("deliver non-local command " + cmdw.cmd);
                smrlearner.deliver(cmdw.cmd, curstream.getStreamID(), update.getLogpos());
            }
            update = curstream.readNext(curtail);
        }

//        dbglog.debug("done with applying sync batch... wake up syncing threads...");

        //wake up all waiting query threads; they will now see a state that incorporates all updates
        //that finished before they started
        //todo -- it's dumb to create a set every time for the trivial case
        Set<Long> curstreamlist = new HashSet<Long>(); curstreamlist.add(curstream.getStreamID());
        Iterator<SyncObjectWrapper> it = procqueue.iterator();
        while(it.hasNext())
        {
            SyncObjectWrapper syncobj = it.next();
            if(syncobj.synccommand!=null)
            {
                smrlearner.deliver(syncobj.synccommand, curstream.getStreamID(), ITimestamp.getInvalidTimestamp());
            }
            synchronized(syncobj)
            {
                syncobj.notifyAll();
            }
        }
        procqueue.clear();
    }

}

/**
 * Interface implemented by SMR learners. SMREngine uses this interface to
 * send new commands to its registered learner.
 */
interface SMRLearner
{
    /**
     * An upcall that must be implemented by learners to obtain new commands
     * from an SMREngine.
     * todo: clean up timestamp semantics --- currently all local commands have invalid_timestamp; is this okay?
     * @param command
     * @param curstream
     * @param timestamp
     */
    void deliver(Object command, long curstream, ITimestamp timestamp);
}


