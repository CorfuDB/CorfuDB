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
package org.corfudb.runtime.smr;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.log.ITimestamp;
import org.corfudb.runtime.OutOfSpaceException;
import org.corfudb.runtime.log.Bundle;
import org.corfudb.runtime.log.IStream;
import org.corfudb.runtime.entries.CorfuDBStreamEntry;
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

import java.util.concurrent.atomic.AtomicLong;
import java.util.ArrayList;
import org.corfudb.runtime.view.Serializer;

class StreamEntryImpl implements org.corfudb.runtime.smr.StreamEntry
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



class IStreamFactoryImpl implements IStreamFactory
{
    org.corfudb.runtime.view.IWriteOnceAddressSpace was;
    org.corfudb.runtime.smr.StreamingSequencer ss;
    public IStreamFactoryImpl(org.corfudb.runtime.view.IWriteOnceAddressSpace twas, org.corfudb.runtime.smr.StreamingSequencer tss)
    {
        was = twas;
        ss = tss;
    }
    public org.corfudb.runtime.smr.Stream newStream(long streamid)
    {
        return new StreamImpl(streamid, ss, was);
    }

}

class HopAdapterIStreamFactoryImpl implements IStreamFactory
{
    CorfuDBRuntime cdb;
    public HopAdapterIStreamFactoryImpl(CorfuDBRuntime tcdb)
    {
        cdb = tcdb;
    }

    @Override
    public org.corfudb.runtime.smr.Stream newStream(long streamid)
    {
        return new HopAdapterStreamImpl(cdb, streamid);
    }
}

class MemoryStreamFactoryImpl implements IStreamFactory
{
    boolean serialize;
    boolean compress;

    public MemoryStreamFactoryImpl(boolean serialize, boolean compress)
    {
        this.serialize = serialize;
        this.compress = compress;
    }

    @Override
    public org.corfudb.runtime.smr.Stream newStream(long streamid)
    {
        return new MemoryStreamImpl(streamid, serialize, compress);
    }

}


class HopAdapterStreamEntryImpl implements org.corfudb.runtime.smr.StreamEntry
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

class HopAdapterStreamImpl implements org.corfudb.runtime.smr.Stream
{
    private static Logger dbglog = LoggerFactory.getLogger(HopAdapterStreamImpl.class);
    IStream hopstream;
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


    public HopAdapterStreamImpl(CorfuDBRuntime cdb, long tstreamid)
    {
        streamid = tstreamid;
        hopstream = new org.corfudb.runtime.log.Stream(cdb, new UUID(streamid, 0), 2, 10000, globalThreadPool, true);
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
    public org.corfudb.runtime.smr.StreamEntry readNext()
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
    public org.corfudb.runtime.smr.StreamEntry readNext(ITimestamp stoppos)
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

class MemoryStreamImpl implements org.corfudb.runtime.smr.Stream
{
    static Logger dbglog = LoggerFactory.getLogger(MemoryStreamImpl.class);

    static class MemoryLog {
        ConcurrentHashMap<Long, Serializable> addressSpace;
        AtomicLong sequencer;
        public MemoryLog()
        {
            addressSpace = new ConcurrentHashMap<Long, Serializable>();
            sequencer = new AtomicLong();
        }

        public Long getNextSequence()
        {
            return sequencer.getAndIncrement();
        }

        public boolean write(Long address, Serializable payload, boolean serialize, boolean compress)
        {
            try {
            Serializable out_payload = (compress ? Serializer.serialize_compressed(payload) :
                                                      serialize ? Serializer.serialize(payload) :
                                                      payload);

            Serializable prev = addressSpace.putIfAbsent(address, out_payload);
            return prev == null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public Long append(Serializable payload, boolean serialize, boolean compress)
        {
            while (true) {
                try {
                long sequence = sequencer.getAndIncrement();
                Serializable out_payload = (compress ? Serializer.serialize_compressed(payload) :
                                          serialize ? Serializer.serialize(payload) :
                                          payload);

                Serializable prev = addressSpace.putIfAbsent(sequence, out_payload);
                if (prev == null) {
                    return sequence;
                }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public Serializable read(Long address, boolean serialize, boolean compress)
        {
            try {
            Serializable payload = addressSpace.get(address);
            if(!compress && !serialize) { return payload; }
            byte[] b_payload = (byte[]) payload;
            Serializable ret = (compress ? (Serializable) Serializer.deserialize_compressed(b_payload) :
                                          serialize ? (Serializable) Serializer.deserialize(b_payload) :
                                          payload);
            return ret;
            } catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        public Long getTail()
        {
            return sequencer.get();
        }
    }

    static MemoryLog mlog = new MemoryLog();

    static ConcurrentHashMap<Long, ArrayList<Long>> streamList = new ConcurrentHashMap<Long, ArrayList<Long>>();

    static class MemoryStreamEntry implements Serializable, org.corfudb.runtime.smr.StreamEntry {
        public Serializable payload;
        public Long streamID;
        public MemoryStreamEntry(Long streamID, Serializable payload)
        {
            this.streamID = streamID;
            this.payload = payload;
        }

        @Override
        public ITimestamp getLogpos()
        {
            return new MemoryTimestamp(streamID);
        }

        @Override
        public Object getPayload()
        {
            return payload;
        }

        @Override
        public boolean isInStream(long streamid)
        {
            return streamID == streamid;
        }
    }

    static class MemoryStream {
        Long streamID;
        AtomicInteger logicalAddress;
        boolean serialize;
        boolean compress;


        public MemoryStream(Long streamid)
        {
            this(streamid, false, false);
        }

        public MemoryStream(Long streamid, boolean serialize, boolean compress)
        {
            this.streamID = streamid;
            streamList.putIfAbsent(streamid, new ArrayList<Long>());
            logicalAddress = new AtomicInteger();
            this.serialize = serialize;
            this.compress = compress;
        }

        public Long append(Serializable payload)
        {
            return staticAppend(streamID, payload, serialize, compress);
        }

        public static Long staticAppend(Long streamID, Serializable payload, boolean serialize, boolean compress)
        {
            while (true)
            {
                Long address = mlog.getNextSequence();
                if (mlog.write(address, new MemoryStreamEntry(streamID, payload), serialize, compress))
                {
                    ArrayList<Long> list = streamList.get(streamID);
                    synchronized(list) {
                        list.add(address);
                    }
                    return address;
                }
            }
        }

        public MemoryStreamEntry readNext(Long address) {
            if (address != null && getTail() < address) {
                return null;
            }

            ArrayList<Long> list = streamList.get(streamID);
            synchronized(list) {
                if (logicalAddress.get() >= list.size())
                {
                    return null;
                }
            }
             int logicalCurrent = logicalAddress.getAndIncrement();
             while (true)
             {
                try {
                    synchronized(list) {
                        Long nextAddress = list.get(logicalCurrent);
                        return (MemoryStreamEntry)mlog.read(nextAddress, serialize, compress);
                    }
                }
                catch (Exception e) {
                }
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ie) {}
            }
        }

        public Long getTail() {
            ArrayList<Long> list = streamList.get(streamID);
            synchronized(list) {
                return list.get(list.size()-1);
            }
        }
    }

    static class MemoryTimestamp implements ITimestamp
    {
        public Long ts;
        public MemoryTimestamp(Long ts)
        {
            this.ts = ts;
        }

        @Override
        public String toString()
        {
            return ts.toString();
        }

        @Override
        public int compareTo(ITimestamp timestamp)
        {
            //always less than max
            if (ITimestamp.isMax(timestamp)) { return -1; }
            //always greater than min
            if (ITimestamp.isMin(timestamp)) { return 1; }
            //always invalid
            if (ITimestamp.isInvalid(timestamp)) { throw new ClassCastException("Comparison of invalid timestamp!"); }

            if (timestamp instanceof MemoryTimestamp)
            {
                MemoryTimestamp t = (MemoryTimestamp) timestamp;
                return ts.compareTo(t.ts);
            }
            throw new ClassCastException("I don't know how to compare these timestamps, (maybe you need to override comapreTo<ITimestamp> in your timestamp implementation?) [ts1=" + this.toString() + "] [ts2=" + timestamp.toString()+"]");
        }

        @Override
        public boolean equals(Object t)
        {
            if (!(t instanceof MemoryTimestamp)) { return false; }
            return compareTo((MemoryTimestamp)t) == 0;
        }

        @Override
        public int hashCode()
        {
            return ts.intValue();
        }

    }

    long streamid;
    MemoryStream ms;
    boolean serialize;
    boolean compress;

    public long getStreamID()
    {
        return streamid;
    }

    MemoryStreamImpl(long tstreamid, boolean serialize, boolean compress)
    {
        streamid = tstreamid;
        this.serialize = serialize;
        this.compress = compress;
        ms = new MemoryStream(streamid, serialize, compress);
    }

    @Override
    public ITimestamp append(Serializable payload, Set<Long> streams)
    {
        for (Long s : streams)
        {
            MemoryStream.staticAppend(s, payload, serialize, compress);
        }
        return new MemoryTimestamp(ms.append(payload));
    }

    @Override
    public org.corfudb.runtime.smr.StreamEntry readNext()
    {
        return readNext(null);
    }

    @Override
    public org.corfudb.runtime.smr.StreamEntry readNext(ITimestamp istoppos)
    {
        MemoryTimestamp ts = (MemoryTimestamp) istoppos;
        MemoryStreamEntry mse = ms.readNext(ts.ts);
        return mse;
    }

    @Override
    public ITimestamp checkTail()
    {
        return new MemoryTimestamp(ms.getTail());
    }

    @Override
    public void prefixTrim(ITimestamp trimpos)
    {
        throw new RuntimeException("unimplemented");
    }
}

class StreamImpl implements org.corfudb.runtime.smr.Stream
{
    static Logger dbglog = LoggerFactory.getLogger(StreamImpl.class);

    long streamid;
    org.corfudb.runtime.smr.StreamingSequencer seq;
    org.corfudb.runtime.view.IWriteOnceAddressSpace addrspace;
    ConcurrentHashMap<Long, org.corfudb.runtime.smr.StreamEntry> m_cache;
    Lock biglock;
    long curpos;
    long curtail;

    public long getStreamID()
    {
        return streamid;
    }

    StreamImpl(long tstreamid, org.corfudb.runtime.smr.StreamingSequencer tss, org.corfudb.runtime.view.IWriteOnceAddressSpace tlas)
    {
        streamid = tstreamid;
        seq = tss;
        addrspace = tlas;
        biglock = new ReentrantLock();
    }

    @Override
    public ITimestamp append(Serializable payload, Set<Long> streams)
    {
        long ret = seq.get_slot(streams);
        Timestamp T = new Timestamp(0, ret, 0, this.getStreamID()); //todo: fill in the right epoch
        dbglog.debug("reserved slot {}", ret);
        org.corfudb.runtime.smr.StreamEntry S = new StreamEntryImpl(payload, T, streams);
        try
        {
            addrspace.write(ret,Serializer.serialize_compressed(S));
        }
        catch (Exception ex)
        {
            throw new RuntimeException(ex);
        }

        dbglog.debug("wrote slot {}", ret);
        return T;
    }

    @Override
    public org.corfudb.runtime.smr.StreamEntry readNext()
    {
        return readNext(null);
    }

    @Override
    public org.corfudb.runtime.smr.StreamEntry readNext(ITimestamp istoppos)
    {
        //this is a hacky implementation that doesn't take multi-log hopping (epochs, logids) into account
        Timestamp stoppos = (Timestamp)istoppos;
        //if(stoppos!=null && stoppos.logid!=addrspace.getID()) throw new RuntimeException("readnext using timestamp of different log!");
        org.corfudb.runtime.smr.StreamEntry ret = null;
        while(true)
        {
            long readpos;
            biglock.lock();
            try {
                if (!(curpos < curtail && (stoppos == null || curpos < stoppos.pos)))
                    return null;
                readpos = curpos++;
            } finally {
                biglock.unlock();
            }
            try {
                ret = (org.corfudb.runtime.smr.StreamEntry) Serializer.deserialize_compressed(addrspace.read(readpos));
            }
            catch (Exception ex)
            {
                throw new RuntimeException(ex);
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
        return new Timestamp(0, tcurtail, 0, this.getStreamID()); //todo: populate epoch
    }

    @Override
    public void prefixTrim(ITimestamp trimpos)
    {
        throw new RuntimeException("unimplemented");
    }
}
