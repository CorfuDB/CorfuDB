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
import org.corfudb.runtime.entries.legacy.BasicStreamEntry;
import org.corfudb.runtime.stream.*;
import org.corfudb.runtime.view.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BasicStream implements IStream
{
    static Logger dbglog = LoggerFactory.getLogger(BasicStream.class);

    UUID streamid;
    CorfuDBRuntime rt;
    ISequencer seq;
    IWriteOnceAddressSpace addrspace;
    ConcurrentHashMap<Long, IStreamEntry> m_cache;
    Lock biglock;
    long curpos;
    long curtail;

    public BasicStream(UUID tstreamid, ISequencer tss, IWriteOnceAddressSpace tlas, CorfuDBRuntime _rt)
    {
        streamid = tstreamid;
        seq = tss;
        addrspace = tlas;
        biglock = new ReentrantLock();
        rt = _rt;
    }

    public BasicStream(UUID streamID, CorfuDBRuntime runtime) {
        this(streamID, new StreamingSequencer(runtime), new WriteOnceAddressSpace(runtime), runtime);
    }

    public ITimestamp append(Serializable payload, Set<UUID> streams) throws OutOfSpaceException, IOException {
        long address = seq.getNext();
        SimpleTimestamp T = new SimpleTimestamp(address);
        dbglog.debug("reserved slot {}", address);
        IStreamEntry S = new BasicStreamEntry(payload, T, streams);
        addrspace.write(address,(Serializable) S);
        dbglog.debug("wrote slot {}", address);
        return T;
    }

    public ITimestamp append(Serializable payload) throws OutOfSpaceException, IOException {
        long address = seq.getNext();
        SimpleTimestamp T = new SimpleTimestamp(address);
        dbglog.debug("reserved slot {}", address);
        IStreamEntry S = new BasicStreamEntry(payload, T, streamid);
        addrspace.write(address,(Serializable) S);
        dbglog.debug("wrote slot {}", address);
        return T;
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
        return readEntry(null);
    }

    @Override
    public IStreamEntry readEntry(ITimestamp istoppos) throws HoleEncounteredException, TrimmedException, IOException
    {
        //this is a hacky implementation that doesn't take multi-log hopping (epochs, logids) into account
        SimpleTimestamp stoppos = (SimpleTimestamp)istoppos;
        IStreamEntry ret = null;
        while(true)
        {
            long readpos;
            biglock.lock();
            try {
                if (!(curpos < curtail && (stoppos == null || curpos < stoppos.address)))
                    return null;
                readpos = curpos++;
            } finally {
                biglock.unlock();
            }
            try {
                ret = (IStreamEntry) addrspace.readObject(readpos);
            } catch(ClassNotFoundException cnfe) {
                throw new RuntimeException(cnfe);
            }
            if(ret.containsStream(this.getStreamID()))
                break;
            dbglog.debug("skipping...");
        }
        return ret;
    }

//    public ITimestamp checkTail()
//    {
//        long tcurtail = seq.getCurrent(streamid);
//        biglock.lock();
//        if(tcurtail>curtail) curtail = tcurtail;
//        biglock.unlock();
//        return new Timestamp(streamid, 0, tcurtail, 0); //todo: populate epoch
//    }

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
        return new SimpleTimestamp(curpos-1);
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
        return new SimpleTimestamp(curpos-1);
    }
}
