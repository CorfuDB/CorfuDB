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
    List<Object> curqueue;
    List<Object> procqueue;

    Stream curstream;

    static final long TIMESTAMP_INVALID = -1;
    Lock pendinglock = new ReentrantLock();
    HashMap<Long, Pair<Serializable, Object>> pendingcommands = new HashMap<Long, Pair<Serializable, Object>>();

    public void registerLearner(SMRLearner tlearner)
    {
        smrlearner = tlearner;
    }

    public SMREngine(Stream sb)
    {
        curstream = sb;

        queuelock = new Object();
        curqueue = new LinkedList();
        procqueue = new LinkedList();

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


    public long propose(Serializable update, Set<Long> streams, Object precommand)
    {
        SMRCommandWrapper cmd = new SMRCommandWrapper(update, streams);
        pendinglock.lock();
        pendingcommands.put(cmd.uniqueid, new Pair(update, precommand));
        pendinglock.unlock();
        long pos = curstream.append(cmd, streams);
        if(precommand!=null) //block until precommand is played
            sync(pos);
        return pos;
    }

    public long propose(Serializable update, Set<Long> streams)
    {
        return propose(update, streams, null);
    }


    /** returns once log has been played by playback thread
     * until syncpos.
     * todo: right now syncpos is ignored
     */
    public void sync(long syncpos)
    {
        final Object syncobj = new Object();
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

    public void sync()
    {
        sync(-1);
    }

    //runs in a single thread
    void playback()
    {
        List<Object> tqueue;
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

        if(procqueue.size()==0) throw new RuntimeException("queue cannot be empty at this point!");

        //check the current tail of the stream, and then read the stream until that position
        long curtail = curstream.checkTail();

        dbglog.debug("picked up sync batch of size {}; syncing until {}", procqueue.size(), curtail);

        StreamEntry update = curstream.readNext();
        while(update!=null)
        {
            SMRCommandWrapper cmdw = (SMRCommandWrapper)update.getPayload();
            //if this command was generated by us, swap out the version we read back with the local version
            //this allows return values to be transmitted via the local command object
            pendinglock.lock();
            Pair<Serializable, Object> localcmds = null;
            if(pendingcommands.containsKey(cmdw.uniqueid))
                localcmds = pendingcommands.remove(cmdw.uniqueid);
            pendinglock.unlock();
            if(smrlearner==null) throw new RuntimeException("smr learner not set!");
            if(localcmds!=null)
            {
                if (localcmds.second != null)
                {
                    smrlearner.apply(localcmds.second, curstream.getStreamID(), cmdw.streams, TIMESTAMP_INVALID);
                }
                smrlearner.apply(localcmds.first, curstream.getStreamID(), cmdw.streams, update.getLogpos());
            }
            else
                smrlearner.apply(cmdw.cmd, curstream.getStreamID(), cmdw.streams, update.getLogpos());
            update = curstream.readNext(curtail);
        }

        dbglog.debug("done with applying sync batch... wake up syncing threads...");

        //wake up all waiting query threads; they will now see a state that incorporates all updates
        //that finished before they started
        Iterator it = procqueue.iterator();
        while(it.hasNext())
        {
            Object syncobj = it.next();
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
    void apply(Object command, long curstream, Set<Long> allstreams, long timestamp);
}

/**
 * This is used by SMREngine to wrap commands before serializing them in the stream,
 * so that it can add its own header information.
 *
 */
class SMRCommandWrapper implements Serializable
{
    static long ctr=0;
    long uniqueid;
    Serializable cmd;
    Set<Long> streams;
    public SMRCommandWrapper(Serializable tcmd, Set<Long> tstreams)
    {
        cmd = tcmd;
        //todo: for now, uniqueid is just a local counter; this won't work with multiple clients!
        uniqueid = ctr++;
        streams = tstreams;
    }
}

