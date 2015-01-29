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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A Stream is a set of intertwined streams residing in a single
 * underlying log (i.e., a global ordering exists across all the streams).
 * The Stream allows appends to arbitrary streams (even those not in the bundle).
 * It enables playback of entries in the union of all the streams in strict log order.
 */
interface Stream
{
    long append(BufferStack bs, Set<Long> streams);

    /**
     * reads the next entry in the stream bundle
     *
     * @return       the next log entry
     */
    StreamEntry readNext();

    /**
     * reads the next entry in the stream bundle that has a position strictly lower than stoppos.
     * stoppos is required so that the runtime can check the current tail of the log using checkTail() and
     * then play the log until that tail position and no further, in order to get linearizable
     * semantics with a minimum number of reads.
     *
     * @param  stoppos  the stopping position for the read
     * @return          the next entry in the stream bundle
     */
    StreamEntry readNext(long stoppos);

    /**
     * returns the current tail position of the stream bundle (this is exclusive, so a checkTail
     * on an empty stream returns 0). this also synchronizes local stream metadata with the underlying
     * log and establishes a linearization point for subsequent readNexts; any subsequent readnext will
     * reflect entries that were appended before the checkTail was issued.
     *
     * @return          the current tail of the stream
     */
    long checkTail();

    /**
     * trims all entries in the stream bundle until the passed in position (exclusive). the space
     * may not be reclaimed immediately if the underlying address space only supports a prefix trim.
     *
     * @param   trimpos the position strictly before which all entries belonging to the bundle's
     *                  streams are trimmed
     */
    void prefixTrim(long trimpos);
}





/**
 * Used by Stream to wrap read values, so that some metadata
 * (e.g., the position of the entry in the underlying log) can be returned
 * along with the payload.
 */
class StreamEntry
{
    private long logpos;
    private BufferStack payload;

    public long getLogpos()
    {
        return logpos;
    }

    public BufferStack getPayload()
    {
        return payload;
    }

    public StreamEntry(BufferStack tbs, long position)
    {
        logpos = position;
        payload = tbs;
    }
}


class StreamImpl implements Stream
{
    long streamid;

    StreamingSequencer seq;
    WriteOnceAddressSpace addrspace;

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

    }

    @Override
    public long append(BufferStack bs, Set<Long> streams)
    {
        long ret = seq.get_slot(streams);
        addrspace.write(ret, bs);
        return ret;
    }

    @Override
    public StreamEntry readNext()
    {
            return readNext(0);
    }

    @Override
    public StreamEntry readNext(long stoppos)
    {
        biglock.lock();
        if(!(curpos<curtail && (stoppos==0 || curpos<stoppos)))
        {
            biglock.unlock();
            return null;
        }
        long readpos = curpos++;
        biglock.unlock();
        BufferStack ret = addrspace.read(readpos);
        return new StreamEntry(ret, readpos);
    }

    @Override
    public long checkTail()
    {
        long tcurtail = seq.check_tail();
        biglock.lock();
        if(tcurtail>curtail) curtail = tcurtail;
        biglock.unlock();
        return tcurtail;
    }

    @Override
    public void prefixTrim(long trimpos)
    {
        throw new RuntimeException("unimplemented");
    }
}



/**
 * Simple implementation of the Stream interface. Implements
 * streaming by playing back all entries and discarding those
 * that belong to other streams.
 */
class StreamBundleImpl implements Stream
{

    List<Long> mystreams;

    WriteOnceAddressSpace las;
    StreamingSequencer ss;

    Lock biglock;
    long curpos;
    long curtail;


    public StreamBundleImpl(List<Long> streamids, StreamingSequencer tss, WriteOnceAddressSpace tlas)
    {
        las = tlas;
        ss = tss;

        biglock = new ReentrantLock();

        mystreams = streamids;
    }

    public long append(BufferStack bs, Set<Long> streamids)
    {
        long ret = ss.get_slot(streamids);
        las.write(ret, bs);
        return ret;

    }

    public long checkTail()
    {
        long tcurtail = ss.check_tail();
        biglock.lock();
        if(tcurtail>curtail) curtail = tcurtail;
        biglock.unlock();
        return tcurtail;

    }

    @Override
    public void prefixTrim(long trimpos)
    {
        //todo: do something smarter where we track the individual trim points of all streams
        las.prefixTrim(trimpos);
    }

    public StreamEntry readNext()
    {
        return readNext(0);
    }

    public StreamEntry readNext(long stoppos)
    {
        biglock.lock();
        if(!(curpos<curtail && (stoppos==0 || curpos<stoppos)))
        {
            biglock.unlock();
            return null;
        }
        long readpos = curpos++;
        biglock.unlock();
        BufferStack ret = las.read(readpos);
        return new StreamEntry(ret, readpos);
    }
}