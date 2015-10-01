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
package org.corfudb.runtime.stream.legacy;


import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.HoleEncounteredException;
import org.corfudb.runtime.OutOfSpaceException;
import org.corfudb.runtime.TrimmedException;
import org.corfudb.runtime.entries.IStreamEntry;
import org.corfudb.runtime.entries.legacy.HopAdapterStreamEntry;
import org.corfudb.runtime.stream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ExecutorService;


class HopAdapterStream implements IAdapterStream
{
    private static Logger dbglog = LoggerFactory.getLogger(HopAdapterStream.class);
    IStream hopstream;
    UUID streamid;
    CorfuDBRuntime rt;

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


    public HopAdapterStream(CorfuDBRuntime cdb, UUID tstreamid)
    {
        rt = cdb;
        streamid = tstreamid;
        hopstream = (IStream) new org.corfudb.runtime.stream.Stream(cdb, streamid, 2, 10000, globalThreadPool, true);
    }

    public ITimestamp append(Serializable s, Set<UUID> streams) throws OutOfSpaceException, IOException
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
            catch (IOException ioexc)
            {
                System.out.println(ioexc);
                throw new RuntimeException(ioexc);
            }
        }
        else
        {
            List<UUID> streamuuids = new LinkedList<>();
            Iterator<UUID> it = streams.iterator();
            while(it.hasNext())
            {
                UUID x = it.next();
                if(streamid==x) continue;
                streamuuids.add(x);
            }
            try
            {
                ArrayList<UUID> listuuids = new ArrayList();
                listuuids.addAll(listuuids);
                OldBundle B = new OldBundle((IHopStream)hopstream, listuuids, s, false);
                ITimestamp T = B.apply();
                return T;
            }
            catch (IOException e)
            {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    public ITimestamp append(Serializable s) throws OutOfSpaceException, IOException {
        try {
            ITimestamp T = hopstream.append(s);
            return T;
        } catch (OutOfSpaceException oe) {
            System.out.println(oe);
            throw new RuntimeException(oe);
        } catch (IOException ioexc) {
            System.out.println(ioexc);
            throw new RuntimeException(ioexc);
        }
    }

    /**
     * Append an object to the stream. This operation may or may not be successful. For example,
     * a move operation may occur, and the append will not be part of the stream.
     *
     * @param data A serializable object to append to the stream.
     * @return A timestamp, which reflects the physical position and the epoch the data was written in.
     */
    @Override
    public ITimestamp append(Object data) throws OutOfSpaceException, IOException {
        return null;
    }

    @Override
    public IStreamEntry readNextEntry() throws HoleEncounteredException, TrimmedException, IOException
    {
        dbglog.debug("readNext...");
        try
        {
            IStreamEntry cde = hopstream.readNextEntry();
            dbglog.debug("done with readNext.");
            return new HopAdapterStreamEntry(cde);
        }
        catch (IOException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        catch (HoleEncounteredException e)
        {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public IStreamEntry readEntry(ITimestamp stoppos) throws HoleEncounteredException, TrimmedException, IOException
    {
        dbglog.debug("readNext {}", stoppos);
        IStreamEntry cde = hopstream.readEntry(hopstream.getCurrentPosition());

        dbglog.debug("peeked " + ((cde==null)?null:cde.getTimestamp()));
        if(cde==null)
        {
            ITimestamp local = hopstream.check();
            if (local != null) {
            dbglog.debug("local ts = {} ", local);
            if (local.compareTo(stoppos) < 0)
            {
                dbglog.debug("calling readNext() on entry not yet read, local {}, stoppos {} compare{}", local, stoppos, local.compareTo(stoppos));
                return readNextEntry();
            }}
            else
            {
                //stream not read, all positions are greater.
                return readNextEntry();
            }
            return null;
        }
        if(cde.getTimestamp().compareTo(stoppos)<=0)
        {
            dbglog.debug("calling readNext()");
            return readNextEntry();
        }
        return null;
    }

    public ITimestamp checkTail()
    {
        ITimestamp curtail = hopstream.check();
        dbglog.debug("curtail = {}", curtail);
        return curtail;
    }

    public void prefixTrim(ITimestamp trimpos)
    {
        throw new RuntimeException("unimplemented");
    }

    /**
     * Given a timestamp, get the timestamp in the stream
     *
     * @param ts The timestamp to increment.
     * @return The next timestamp in the stream, or null, if there are no next timestamps in the stream.
     */
    @Override
    public ITimestamp getNextTimestamp(ITimestamp ts) {
        return new SimpleTimestamp(((SimpleTimestamp)ts).address + 1);
    }

    /**
     * Given a timestamp, get a proceeding timestamp in the stream.
     *
     * @param ts The timestamp to decrement.
     * @return The previous timestamp in the stream, or null, if there are no previous timestamps in the stream.
     */
    @Override
    public ITimestamp getPreviousTimestamp(ITimestamp ts) {
        return new SimpleTimestamp(((SimpleTimestamp)ts).address - 1);
    }

    /**
     * Gets the current position the stream has read to (which may not point to an entry in the
     * stream).
     *
     * @return A timestamp, which reflects the most recently read address in the stream.
     */
    @Override
    public ITimestamp getCurrentPosition() {
        return hopstream.getCurrentPosition();
    }

    /**
     * Requests a trim on this stream. This function informs the configuration master that the
     * position on this stream is trimmable, and moves the start position of this stream to the
     * new position.
     *
     * @param address
     */
    @Override
    public void trim(ITimestamp address) {

    }


    /**
     * Close the stream. This method must be called to free resources.
     */
    @Override
    public void close() {

    }

    /**
     * Get the ID of the stream.
     *
     * @return The ID of the stream.
     */
    @Override
    public UUID getStreamID() {
        return streamid;
    }

    /**
     * Get the ID of the stream.
     *
     * @return The ID of the stream.
     */
    @Override
    public long getIntegerStreamID() {
        return streamid.getMostSignificantBits();
    }

    /**
     * Returns a fresh or cached timestamp, which can serve as a linearization point. This function
     * may return a non-linearizable (invalid) timestamp which may never occur in the ordering
     * due to a move/epoch change.
     *
     * @param cached Whether or not the timestamp returned is cached.
     * @return A timestamp, which reflects the most recently allocated timestamp in the stream,
     * or currently read, depending on whether cached is set or not.
     */
    @Override
    public ITimestamp check(boolean cached) {
        return hopstream.check(cached);
    }
}

