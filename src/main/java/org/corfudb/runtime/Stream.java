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
public interface Stream
{

    /**
     * Appends the entry to multiple streams. Results in a single write to a single underlying
     * address space that appears in multiple streams. Does not guarantee success --- i.e.,
     * if the client subsequently plays the streams forward, it may not encounter the entry
     * in one or all of the streams.
     * //todo: should this strictly be in the Stream object, since it impacts multiple streams?
     *
     * @param s entry to append
     * @param streams set of streams to append the entry to
     * @return Timestamp of the appended entry
     */
    Timestamp append(Serializable s, Set<Long> streams);

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
    StreamEntry readNext(Timestamp stoppos);

    /**
     * returns the current tail position of the stream (this is exclusive, so a checkTail
     * on an empty stream returns 0). this also synchronizes local stream metadata with the underlying
     * log and establishes a linearization point for subsequent readNexts; any subsequent readnext will
     * reflect entries that were appended before the checkTail was issued.
     *
     * @return          the current tail of the stream
     */
    Timestamp checkTail();

    /**
     * trims all entries in the stream until the passed in position (exclusive); so
     * trimming at timestamp T trims all entries with a timestamp lower than T. the space
     * may not be reclaimed immediately if the underlying address space only supports a prefix trim.
     *
     * @param   trimpos the position strictly before which all entries belonging to the
     *                  stream are trimmed
     */
    void prefixTrim(Timestamp trimpos);

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

interface StreamEntry extends Serializable
{
    public Timestamp getLogpos();
    public Object getPayload();
    public Set<Long> getStreams();
}

class Timestamp implements Comparable, Serializable
{
    long logid; //todo: currently we seem to identify logs with strings... which makes for terribly inefficient timestamps
    long pos;
    long epoch;
    public Timestamp(long tlogid, long tpos, long tepoch)
    {
        logid = tlogid;
        pos = tpos;
        epoch = tepoch;
    }

    @Override
    public int compareTo(Object o)
    {
        Timestamp T2 = (Timestamp)o;
        //todo: is it okay to (ab)use ClassCastException?
        if(this.logid!=T2.logid) throw new ClassCastException("timestamps are not comparable since they are on different logs");
        int x;
        if(this.epoch<T2.epoch || (this.epoch==T2.epoch && this.pos<T2.pos))
            x = -1;
        else
            x = 1;
        return x;
    }

    public String toString()
    {
        return "[" + logid + "." + epoch + "." + pos + "]";
    }
}


