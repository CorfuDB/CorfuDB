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
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A Stream is a subsequence of entries residing in a single
 * underlying log.
 * It enables playback of entries in the stream in strict log order.
 * Entries can belong to multiple streams.
 * The Stream object allows appends to arbitrary other streams.
 */
interface Stream
{
    long append(Serializable s, Set<Long> streams);

    /**
     * reads the next entry in the stream
     *
     * @return       the next log entry
     */
    StreamEntry readNext();

    /**
     * reads the next entry in the stream that has a position strictly lower than stoppos.
     * stoppos is required so that the runtime can check the current tail of the log using checkTail() and
     * then play the log until that tail position and no further, in order to get linearizable
     * semantics with a minimum number of reads.
     *
     * @param  stoppos  the stopping position for the read
     * @return          the next entry in the stream
     */
    StreamEntry readNext(long stoppos);

    /**
     * returns the current tail position of the stream (this is exclusive, so a checkTail
     * on an empty stream returns 0). this also synchronizes local stream metadata with the underlying
     * log and establishes a linearization point for subsequent readNexts; any subsequent readnext will
     * reflect entries that were appended before the checkTail was issued.
     *
     * @return          the current tail of the stream
     */
    long checkTail();

    /**
     * trims all entries in the stream until the passed in position (exclusive); so
     * trimming at timestamp T trims all entries with a timestamp lower than T. the space
     * may not be reclaimed immediately if the underlying address space only supports a prefix trim.
     *
     * @param   trimpos the position strictly before which all entries belonging to the
     *                  stream are trimmed
     */
    void prefixTrim(long trimpos);

    /**
     * returns this stream's ID
     *
     * @return this stream's ID
     */
    long getStreamID();
}





/**
 * Used by Stream to wrap read values, so that some metadata
 * (e.g., the position of the entry in the underlying log) can be returned
 * along with the payload.
 */
class StreamEntry implements Serializable
{
    private long logpos; //this doesn't have to be serialized, but leaving it in for debug purposes
    private Object payload;
    private Set<Long> streams;

    public long getLogpos()
    {
        return logpos;
    }

    public Object getPayload()
    {
        return payload;
    }

    public Set<Long> getStreams()
    {
        return streams;
    }

    public StreamEntry(Object tbs, long position, Set<Long> tstreams)
    {
        logpos = position;
        payload = tbs;
        streams = tstreams;
    }
}


interface StreamFactory
{
    public Stream newStream(long streamid);
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

class StreamImpl implements Stream
{
    static Logger dbglog = LoggerFactory.getLogger(StreamImpl.class);

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
    public long append(Serializable payload, Set<Long> streams)
    {
        long ret = seq.get_slot(streams);
        dbglog.debug("reserved slot {}", ret);
        StreamEntry S = new StreamEntry(payload, ret, streams);
        addrspace.write(ret, BufferStack.serialize(S));
        dbglog.debug("wrote slot {}", ret);
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
        StreamEntry ret = null;
        while(true)
        {
            biglock.lock();
            if (!(curpos < curtail && (stoppos == 0 || curpos < stoppos)))
            {
                biglock.unlock();
                return null;
            }
            long readpos = curpos++;
            biglock.unlock();
            BufferStack bs = addrspace.read(readpos);
            ret = (StreamEntry) bs.deserialize();
            if(ret.getStreams().contains(this.getStreamID()))
                break;
            dbglog.debug("skipping...");
        }
        return ret;
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