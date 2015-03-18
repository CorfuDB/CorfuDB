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

package org.corfudb.client;

import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.OutOfSpaceException;

import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.UUID;
import java.lang.StringBuilder;

public class Timestamp implements Comparable<Timestamp>, Serializable {
    private static final Logger log = LoggerFactory.getLogger(Timestamp.class);

    public transient Long pos;
    public transient UUID primaryStream;

    public transient Long physicalPos;
    public transient UUID logID;
    public Map<UUID, Long> epochMap;

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
        this.pos = null;
        this.physicalPos = null;
    }

    public Timestamp(Map<UUID, Long> epochMap)
    {
        this.epochMap = epochMap;
    }

    public Timestamp(Map<UUID, Long> epochMap, long pos, long physicalPos)
    {
        this.epochMap = epochMap;
        this.pos = pos;
        this.physicalPos = physicalPos;
    }


    public long getEpoch(UUID stream)
    {
        return epochMap.get(stream);
    }

    public void setLogicalPos(long pos, UUID primaryStream)
    {
        this.primaryStream = primaryStream;
        this.pos = pos;
    }

    public void setPhysicalPos(long pos)
    {
        this.physicalPos = physicalPos;
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
                if (!this.epochMap.get(id).equals(epochMap.get(id)))
                {
                    return false;
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
        }
        return sb.toString();
    }

    public int compareTo(Timestamp t)
    {
        if (primaryStream != null && primaryStream.equals(t.primaryStream))
        {
            return (int) (pos - t.pos);
        }

        for (UUID id : this.epochMap.keySet())
        {
            if (t.epochMap.containsKey(id))
            {
                return (int) (this.epochMap.get(id) - t.epochMap.get(id));
            }
        }
        throw new ClassCastException("Uncomparable timestamp objects!");
    }
}

