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
    ITimestamp append(Serializable s, Set<Long> streams);

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
    StreamEntry readNext(ITimestamp stoppos);

    /**
     * returns the current tail position of the stream (this is exclusive, so a checkTail
     * on an empty stream returns 0). this also synchronizes local stream metadata with the underlying
     * log and establishes a linearization point for subsequent readNexts; any subsequent readnext will
     * reflect entries that were appended before the checkTail was issued.
     *
     * @return          the current tail of the stream
     */
    ITimestamp checkTail();

    /**
     * trims all entries in the stream until the passed in position (exclusive); so
     * trimming at timestamp T trims all entries with a timestamp lower than T. the space
     * may not be reclaimed immediately if the underlying address space only supports a prefix trim.
     *
     * @param   trimpos the position strictly before which all entries belonging to the
     *                  stream are trimmed
     */
    void prefixTrim(ITimestamp trimpos);

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
    public ITimestamp getLogpos();
    public Object getPayload();
    public boolean isInStream(long streamid);
}

interface ITimestampConstants
{
    ITimestamp getInvalidTimestamp();
    ITimestamp getMaxTimestamp();
    ITimestamp getMinTimestamp();
}

class TimestampConstants implements ITimestampConstants
{
    static ITimestampConstants instance = new TimestampConstants();

    public static ITimestampConstants singleton()
    {
        return instance;
    }

    Timestamp invalidts = new Timestamp(Timestamp.special_isinvalid);
    Timestamp maxts = new Timestamp(Timestamp.special_ismax);
    Timestamp mints = new Timestamp(Timestamp.special_ismin);

    @Override
    public ITimestamp getInvalidTimestamp()
    {
        return invalidts;
    }

    @Override
    public ITimestamp getMaxTimestamp()
    {
        return maxts;
    }

    @Override
    public ITimestamp getMinTimestamp()
    {
        return mints;
    }
}

class Timestamp implements ITimestamp, Serializable
{
    public static final int special_none = -1;
    public static final int special_ismax = 0;
    public static final int special_ismin = 1;
    public static final int special_isinvalid = 2;

    int special_value = special_none;

    long streamid;
    long logid; //todo: currently we seem to identify logs with strings... which makes for terribly inefficient timestamps
    long pos;
    long epoch;
    public Timestamp(long tlogid, long tpos, long tepoch, long tstreamid)
    {
        logid = tlogid;
        pos = tpos;
        epoch = tepoch;
        streamid = tstreamid;
    }
    public Timestamp(int tspecialvalue)
    {
        special_value = tspecialvalue;
    }

    public boolean equals(Object o)
    {
        Timestamp tT = (Timestamp)o;
        if(tT.special_value!=this.special_value) return false;
        if(compareTo(tT)==0) return true;
        return false;
    }


    @Override
    public int compareTo(ITimestamp iTimestamp)
    {
        Timestamp T2 = (Timestamp)iTimestamp;
        //check the special bits
        if(special_value!=special_none && special_value==T2.special_value) return 0;

        if(special_value == special_ismax || T2.special_value==special_ismin)
            return 1;
        else if(special_value == special_ismin || T2.special_value==special_ismax)
            return -1;
        else if(special_value == special_isinvalid || T2.special_value==special_isinvalid)
            throw new ClassCastException("trying to compare invalid timestamp");


        int x = 0;
        //todo: is it okay to (ab)use ClassCastException?
        if(this.logid!=T2.logid && this.streamid!=T2.streamid)
            throw new ClassCastException("timestamps are not comparable since they are on different logs");
        //same stream
        else if (this.streamid == T2.streamid)
        {
            if (this.epoch == T2.epoch && this.pos == T2.pos)
                x = 0;
            else if (this.epoch < T2.epoch || (this.epoch == T2.epoch && this.pos < T2.pos)) //on the same epoch, logpos must be comparable since they're on the same log
                x = -1;
            else
                x = 1;
        }
        else
        {
            throw new ClassCastException("different streams!");
        }
        //same log, different streams
/*        else if(this.logid==T2.logid && this.streamid!=T2.streamid)
        {
            if(this.pos==T2.pos)
                x = 0;
            else if(this.pos<T2.pos)
                x = -1;
            else
                x = 1;
        }*/
        return x;

    }

    public String toString()
    {
        if(special_value==special_isinvalid) return "T[INV]";
        if(special_value==special_ismax) return "T[MAX]";
        if(special_value==special_ismin) return "T[MIN]";
        return "T[" + streamid + "," + logid + "," + epoch + "," + pos + "]";
    }

    public int hashCode()
    {
        return (int)(streamid+logid+pos+epoch);
    }
}