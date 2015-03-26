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

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.ITimestamp;
import org.corfudb.client.OutOfSpaceException;
import org.corfudb.client.abstractions.Bundle;
import org.corfudb.client.entries.CorfuDBStreamEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;

class StreamEntryImpl implements StreamEntry
{
    private ITimestamp logpos; //this doesn't have to be serialized, but leaving it in for debug purposes
    private Object payload;
    private Set<Long> streams;

    public ITimestamp getLogpos()
    {
        return logpos;
    }

    public Object getPayload()
    {
        return payload;
    }

    @Override
    public boolean isInStream(long streamid)
    {
        return getStreams().contains(streamid);
    }

    public Set<Long> getStreams()
    {
        return streams;
    }

    public StreamEntryImpl(Object tbs, ITimestamp position, Set<Long> tstreams)
    {
        logpos = position;
        payload = tbs;
        streams = tstreams;
    }
}



class StreamFactoryImpl implements StreamFactory
{
    WriteOnceAddressSpace was;
    StreamingSequencer ss;
    public StreamFactoryImpl(WriteOnceAddressSpace twas, StreamingSequencer tss)
    {
        was = twas;
        ss = tss;
    }
    public Stream newStream(long streamid)
    {
        return new StreamImpl(streamid, ss, was);
    }

}

class HopAdapterStreamFactoryImpl implements StreamFactory
{
    CorfuDBClient cdb;
    public HopAdapterStreamFactoryImpl(CorfuDBClient tcdb)
    {
        cdb = tcdb;
    }

    @Override
    public Stream newStream(long streamid)
    {
        return new HopAdapterStreamImpl(cdb, streamid);
    }
}

class HopAdapterStreamEntryImpl implements StreamEntry
{
    CorfuDBStreamEntry cde;
    public HopAdapterStreamEntryImpl(CorfuDBStreamEntry tcde)
    {
        cde = tcde;
    }

    @Override
    public ITimestamp getLogpos()
    {
        return cde.getTimestamp();
    }

    @Override
    public Object getPayload()
    {
        return cde.payload;
    }

    @Override
    public boolean isInStream(long streamid)
    {
        return cde.containsStream(new UUID(streamid, 0));
    }
}

class HopAdapterStreamImpl implements Stream
{
    private static Logger dbglog = LoggerFactory.getLogger(HopAdapterStreamImpl.class);
    org.corfudb.client.abstractions.IStream hopstream;
    long streamid;

    static class globalThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory
    {
        AtomicInteger counter = new AtomicInteger();
        @Override
        public ForkJoinWorkerThread newThread(ForkJoinPool pool)
        {
            ForkJoinWorkerThread thread = new globalThread(pool);
            thread.setName("GlobalStreamThread-" + counter.getAndIncrement());
            return thread;
        }
    }

    static class globalThread extends ForkJoinWorkerThread {
        public globalThread(ForkJoinPool pool) {
            super(pool);
        }
    }


    static Thread.UncaughtExceptionHandler globalThreadExceptionHandler = (Thread t, Throwable e) ->  {
                        dbglog.warn("Global thread " + t.getName() + "terminated due to exception", e); };
    static ExecutorService globalThreadPool = new ForkJoinPool(8, new globalThreadFactory(), globalThreadExceptionHandler, true);


    public HopAdapterStreamImpl(CorfuDBClient cdb, long tstreamid)
    {
        streamid = tstreamid;
        hopstream = new org.corfudb.client.abstractions.Stream(cdb, new UUID(streamid, 0), 2, 10000, globalThreadPool, true);
    }

    @Override
    public ITimestamp append(Serializable s, Set<Long> streams)
    {
        dbglog.debug("appending to streams " + streams + " from stream " + streamid);
        if(streams.size()==1)
        {
            try
            {
                ITimestamp T = hopstream.append(s);
                return T;
            }
            catch (OutOfSpaceException oe)
            {
                System.out.println(oe);
                throw new RuntimeException(oe);
            }
        }
        else
        {
            List<UUID> streamuuids = new LinkedList<>();
            Iterator<Long> it = streams.iterator();
            while(it.hasNext())
            {
                long x = it.next();
                if(streamid==x) continue;
                streamuuids.add(new UUID(x, 0));
            }
            try
            {
                Bundle B = new Bundle(hopstream, streamuuids, s, false);
                ITimestamp T = B.apply();
                //ITimestamp T = hopstream.pullStream(streamuuids, s, 1);
                return T;
            }
            catch (IOException e)
            {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public StreamEntry readNext()
    {
        dbglog.debug("readNext...");
        try
        {
            CorfuDBStreamEntry cde = hopstream.readNextEntry();
            dbglog.debug("done with readNext.");
            return new HopAdapterStreamEntryImpl(cde);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public StreamEntry readNext(ITimestamp stoppos)
    {
        dbglog.debug("readNext {}", stoppos);
        CorfuDBStreamEntry cde = hopstream.peek();

        dbglog.debug("peeked " + ((cde==null)?null:cde.getTimestamp()));
        if(cde==null)
        {
            ITimestamp local = hopstream.check(true,true);
            if (local != null) {
            dbglog.debug("local ts = {} ", local);
            if (local.compareTo(stoppos) < 0)
            {
                dbglog.debug("calling readNext() on entry not yet read, local {}, stoppos {} compare{}", local, stoppos, local.compareTo(stoppos));
                return readNext();
            }}
            else
            {
                //stream not read, all positions are greater.
                return readNext();
            }
            return null;
        }
        if(cde.getTimestamp().compareTo(stoppos)<=0)
        {
            dbglog.debug("calling readNext()");
            return readNext();
        }
        return null;
    }

    @Override
    public ITimestamp checkTail()
    {
        ITimestamp curtail = hopstream.check();
        dbglog.debug("curtail = {}", curtail);
        return curtail;
    }

    @Override
    public void prefixTrim(ITimestamp trimpos)
    {
        throw new RuntimeException("unimplemented");
    }

    @Override
    public long getStreamID()
    {
        return streamid;
    }
}

class StreamImpl implements Stream
{
    static Logger dbglog = LoggerFactory.getLogger(StreamImpl.class);
    static final boolean cacheStreamEntries = true;


    long streamid;

    StreamingSequencer seq;
    WriteOnceAddressSpace addrspace;
    ConcurrentHashMap<Long, StreamEntry> m_cache;
    Lock biglock;
    long curpos;
    long curtail;

    public long getStreamID()
    {
        return streamid;
    }

    StreamImpl(long tstreamid, StreamingSequencer tss, WriteOnceAddressSpace tlas)
    {
        streamid = tstreamid;
        seq = tss;
        addrspace = tlas;
        biglock = new ReentrantLock();
        m_cache = new ConcurrentHashMap();
    }

    @Override
    public ITimestamp append(Serializable payload, Set<Long> streams)
    {
        long ret = seq.get_slot(streams);
        Timestamp T = new Timestamp(addrspace.getID(), ret, 0, this.getStreamID()); //todo: fill in the right epoch
        dbglog.debug("reserved slot {}", ret);
        StreamEntry S = new StreamEntryImpl(payload, T, streams);
        addrspace.write(ret, BufferStack.serialize(S));
        dbglog.debug("wrote slot {}", ret);
        return T;
    }

    @Override
    public StreamEntry readNext()
    {
        return readNext(null);
    }

    @Override
    public StreamEntry readNext(ITimestamp istoppos)
    {
        //this is a hacky implementation that doesn't take multi-log hopping (epochs, logids) into account
        Timestamp stoppos = (Timestamp)istoppos;
        if(stoppos!=null && stoppos.logid!=addrspace.getID()) throw new RuntimeException("readnext using timestamp of different log!");
        StreamEntry ret = null;
        while(true)
        {
            long readpos;
            biglock.lock();
            try {
                if (!(curpos < curtail && (stoppos == null || curpos < stoppos.pos)))
                    return null;
                readpos = curpos++;
                ret = m_cache.getOrDefault(readpos, null);
            } finally {
                biglock.unlock();
            }
            if(ret == null) {
                BufferStack bs = addrspace.read(readpos);
                ret = (StreamEntry) bs.deserialize();
                biglock.lock();
                try {
                    m_cache.put(readpos, ret);
                } finally {
                    biglock.unlock();
                }
            }
            if(ret.isInStream(this.getStreamID()))
                break;
            dbglog.debug("skipping...");
        }
        return ret;
    }

    @Override
    public ITimestamp checkTail()
    {
        long tcurtail = seq.check_tail();
        biglock.lock();
        if(tcurtail>curtail) curtail = tcurtail;
        biglock.unlock();
        return new Timestamp(addrspace.getID(), tcurtail, 0, this.getStreamID()); //todo: populate epoch
    }

    @Override
    public void prefixTrim(ITimestamp trimpos)
    {
        throw new RuntimeException("unimplemented");
    }
}
