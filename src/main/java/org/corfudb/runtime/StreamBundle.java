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

import org.corfudb.sharedlog.ClientLib;
import org.corfudb.sharedlog.CorfuException;
import org.corfudb.sharedlog.ExtntWrap;
import org.corfudb.sharedlog.UnwrittenCorfuException;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * A StreamBundle is a set of intertwined streams residing in a single
 * underlying log (i.e., a global ordering exists across all the streams).
 * The StreamBundle allows appends to arbitrary streams (even those not in the bundle).
 * It enables playback of entries in the union of all the streams in strict log order.
 */
interface StreamBundle
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
 * This is an interface to a stream-aware sequencer.
 */
interface StreamingSequencer
{
    long get_slot(Set<Long> streams);
    long check_tail();
}

/**
 * A trivial implementation of a stream-aware sequencer that passes commands through
 * to the default stream-unaware sequencer.
 */
class CorfuStreamingSequencer implements StreamingSequencer
{
    ClientLib cl;
    public CorfuStreamingSequencer(ClientLib tcl)
    {
        cl = tcl;
    }
    public long get_slot(Set<Long> streams)
    {
        long ret;
        try
        {
            ret = cl.grabtokens(1);
        }
        catch(CorfuException ce)
        {
            throw new RuntimeException(ce);
        }
        return ret;
    }
    public long check_tail()
    {
        try
        {
            return cl.querytail();
        }
        catch(CorfuException ce)
        {
            throw new RuntimeException(ce);
        }
    }
}

/**
 * This is the write-once address space providing storage for the shared log.
 */
interface WriteOnceAddressSpace
{
    /**
     * Writes an entry at a particular position. Throws an exception if
     * the entry is already written to.
     *
     * @param pos
     * @param bs
     */
    void write(long pos, BufferStack bs); //todo: throw exception

    /**
     * Reads the entry at a particular position. Throws exceptions if the entry
     * is unwritten or trimmed.
     *
     * @param pos
     */
    BufferStack read(long pos); //todo: throw exception

    /**
     * Trims the prefix of the address space before the passed in position.
     *
     * @param pos position before which all entries are trimmed
     */
    void prefixTrim(long pos);
}

/**
 * Implements the write-once address space over the default Corfu shared log implementation.
 */
class CorfuLogAddressSpace implements WriteOnceAddressSpace
{
    ClientLib cl;

    public CorfuLogAddressSpace(ClientLib tcl)
    {
        cl = tcl;
    }

    public void write(long pos, BufferStack bs)
    {
        try
        {
            //convert to a linked list of extent-sized bytebuffers, which is what the logging layer wants
            if(bs.numBytes()>cl.grainsize())
                throw new RuntimeException("entry too big at " + bs.numBytes() + " bytes; multi-entry writes not yet implemented");
            LinkedList<ByteBuffer> buflist = new LinkedList<ByteBuffer>();
            byte[] payload = new byte[cl.grainsize()];
            bs.flatten(payload);
            buflist.add(ByteBuffer.wrap(payload));
            cl.writeExtnt(pos, buflist);
        }
        catch(CorfuException ce)
        {
            throw new RuntimeException(ce);
        }
    }

    public BufferStack read(long pos)
    {
        System.out.println("Reading..." + pos);
        byte[] ret = null;
        while(true)
        {
            try
            {
                ExtntWrap ew = cl.readExtnt(pos);
                //for now, copy to a byte array and return
                System.out.println("read back " + ew.getCtntSize() + " bytes");
                ret = new byte[4096 * 10]; //hack --- fix this
                ByteBuffer bb = ByteBuffer.wrap(ret);
                java.util.Iterator<ByteBuffer> it = ew.getCtntIterator();
                while (it.hasNext())
                {
                    ByteBuffer btemp = it.next();
                    bb.put(btemp);
                }
                break;
            }
            catch (UnwrittenCorfuException uce)
            {
                //encountered a hole -- try again
            }
            catch (CorfuException e)
            {
                throw new RuntimeException(e);
            }
        }
        return new BufferStack(ret);

    }

    @Override
    public void prefixTrim(long pos)
    {
        try
        {
            cl.trim(pos);
        }
        catch (CorfuException e)
        {
            throw new RuntimeException(e);
        }
    }
}

/**
 * Used by StreamBundle to wrap read values, so that some metadata
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

/**
 * Simple implementation of the StreamBundle interface. Implements
 * streaming by playing back all entries and discarding those
 * that belong to other streams.
 */
class StreamBundleImpl implements StreamBundle
{
    List<Long> mystreams;

    WriteOnceAddressSpace las;
    StreamingSequencer ss;

    long curpos;
    long curtail;


    public StreamBundleImpl(List<Long> streamids, StreamingSequencer tss, WriteOnceAddressSpace tlas)
    {
        las = tlas;
        ss = tss;
        mystreams = streamids;
    }

    //todo we are currently synchronizing on 'this' because ClientLib crashes on concurrent access;
    //once ClientLib is fixed, we need to clean up StreamBundle's locking
    public synchronized long append(BufferStack bs, Set<Long> streamids)
    {
        long ret = ss.get_slot(streamids);
        las.write(ret, bs);
        return ret;

    }

    public synchronized long checkTail() //for now, using 'this' to synchronize curtail
    {
//		System.out.println("Checking tail...");
        curtail = ss.check_tail();
//		System.out.println("tail is " + curtail);
        return curtail;

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

    public synchronized StreamEntry readNext(long stoppos) //for now, using 'this' to synchronize curpos/curtail
    {
        if(!(curpos<curtail && (stoppos==0 || curpos<stoppos)))
        {
            return null;
        }
        long readpos = curpos++;
        BufferStack ret = las.read(readpos);
        return new StreamEntry(ret, readpos);
    }
}