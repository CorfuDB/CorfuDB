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

package org.corfudb.runtime.log;

import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.UUID;
import java.lang.StringBuilder;

public class Timestamp implements ITimestamp, Serializable {
    private static final Logger log = LoggerFactory.getLogger(Timestamp.class);

    public Long pos;
    public UUID primaryStream;

    public Long physicalPos;
    public UUID logID;

    public Long copyAddress;
    public UUID copyStreamID;

    public UUID containingStream;
    public Map<UUID, Long> epochMap;
    public boolean isAttachedTS = false;
    public boolean isHole = false;
    public static final long serialVersionUID = 0l;

    public Timestamp(UUID streamID, long epoch, long pos, long physicalPos)
    {
        epochMap = new HashMap<UUID, Long>();
        epochMap.put(streamID, epoch);
        this.pos = pos;
        this.physicalPos = physicalPos;
    }

    public Timestamp(UUID streamID, long epoch)
    {
        epochMap = new HashMap<UUID, Long>();
        epochMap.put(streamID, epoch);
    }

    public Timestamp(Map<UUID, Long> epochMap)
    {
        this.epochMap = new HashMap<UUID, Long>(epochMap);
    }

    public Timestamp(Map<UUID, Long> epochMap, Long pos, Long physicalPos, UUID primaryStream)
    {
        this.epochMap = new HashMap<UUID, Long>(epochMap);
        this.pos = pos;
        this.physicalPos = physicalPos;
        this.primaryStream = primaryStream;
    }

    public void setCopyAddress(Long copyAddress, UUID copyStreamID)
    {
        this.copyAddress = copyAddress;
        this.copyStreamID = copyStreamID;
    }

    public void setHole(boolean ishole)
    {
        isHole = ishole;
    }
    public void setContainingStream(UUID containingStream)
    {
        this.containingStream = containingStream;
    }

    public Long getEpoch(UUID stream)
    {
        try {
            return epochMap.get(stream);
        }
        catch (NullPointerException npe)
        {
            log.debug("Attempted to get epoch for stream {}, but it is not present in timestamp [ts={}]", stream.toString(), toString());
            return null;
        }
    }

    public void setTransientInfo(UUID log, UUID primaryStream, long pos, long physicalPos)
    {
        this.primaryStream = primaryStream;
        this.pos = pos;
        this.physicalPos = physicalPos;
        this.logID = log;
    }

    public void setLogicalPos(long pos, UUID primaryStream)
    {
        this.primaryStream = primaryStream;
        this.pos = pos;
    }

    public void setPhysicalPos(long pos)
    {
        this.physicalPos = pos;
    }

    public long getPhysicalPos()
    {
        return this.physicalPos;
    }
    public void setLogId(UUID log)
    {
        this.logID = log;
    }

    public boolean checkEpoch(Map<UUID, Long> epochMap)
    {
        boolean isContained = false;
        for (UUID id : this.epochMap.keySet())
        {
            if (epochMap.containsKey(id))
            {
                isContained = true;
                long ourEpoch = this.epochMap.get(id);
                long theirEpoch = epochMap.get(id);
                if ((ourEpoch != -1) && (theirEpoch != -1))
                {
                    if (ourEpoch != theirEpoch)
                    {
                        return false;
                    }
                }
            }
        }
        return isContained;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (UUID id : epochMap.keySet())
        {
            if (first) { first = false; }
            else { sb.append(", "); }
            sb.append(id).append(": ").append(epochMap.get(id)).append(".").append(physicalPos == null ? "?" : physicalPos);
            if (pos != null && id.equals(primaryStream))
            {
                sb.append("(").append(pos).append(")");
            }
        }
        return sb.toString();
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

        if (timestamp instanceof Timestamp)
        {
            Timestamp t = (Timestamp) timestamp;
            if (t.isHole) {return -1;}
            // It's the same stream and we have logical addresses.
            if (primaryStream != null && primaryStream.equals(t.primaryStream) && pos != null && t.pos != null)
            {
                return (int) (pos - t.pos);
            }
            // It's the same containing stream, in which case physical addresses can be used
            // TODO: only need to check the epoch of the containing stream.
            else if (containingStream != null && containingStream.equals(t.containingStream) && physicalPos != null && t.physicalPos != null && checkEpoch(t.epochMap))
            {
                return (int) (physicalPos - t.physicalPos);
            }
            else if (primaryStream != null && primaryStream.equals(t.primaryStream) && copyStreamID != null && (copyStreamID.equals(t.primaryStream) || copyStreamID.equals(t.copyStreamID)))
            {
                long ourAddress = copyAddress;
                long theirAddress = t.physicalPos;
                return (int) (ourAddress - theirAddress);
            }
            // It's the same stream but the containing stream is different.
            // We can't really say anything.
            else if (primaryStream != null && primaryStream.equals(t.primaryStream) && containingStream != null && !containingStream.equals(t.containingStream))
            {
                log.info("You're comparing a timestamp from a different containing stream! {} {}", containingStream, t.containingStream);
                return -1;
            }
            else if (physicalPos != null && t.physicalPos != null && checkEpoch(t.epochMap) )
            {
                return (int) (physicalPos - t.physicalPos);
            }
            throw new ClassCastException("Uncomparable timestamp objects! [t1=" + this.toString() + "] [t2=" + t.toString() + "]");
        }
        throw new ClassCastException("I don't know how to compare these timestamps, (maybe you need to override comapreTo<ITimestamp> in your timestamp implementation?) [ts1=" + this.toString() + "] [ts2=" + timestamp.toString()+"]");
    }

    @Override
    public boolean equals(Object t)
    {
        if (!(t instanceof Timestamp)) { return false; }
        return compareTo((Timestamp)t) == 0;
    }

    @Override
    public int hashCode()
    {
        return physicalPos.intValue();
    }
}

