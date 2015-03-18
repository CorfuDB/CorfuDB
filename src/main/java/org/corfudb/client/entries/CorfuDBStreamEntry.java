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

package org.corfudb.client.entries;

import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.Timestamp;

import java.util.Map;
import java.util.ArrayList;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.io.Serializable;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

public class CorfuDBStreamEntry implements Serializable, Comparable<CorfuDBStreamEntry>
{
    private static final long serialVersionUID = 0L;
    public Timestamp ts;
    public byte[] payload;

    public CorfuDBStreamEntry() {}
    public CorfuDBStreamEntry(UUID streamID, Serializable payloadObject, long epoch)
        throws IOException
    {
        try (ByteArrayOutputStream bs = new ByteArrayOutputStream())
        {
            try (ObjectOutput out = new ObjectOutputStream(bs))
            {
                out.writeObject(payloadObject);
                payload = bs.toByteArray();
            }
        }

        this.ts = new Timestamp(streamID, epoch);
    }

    public Object deserializePayload()
        throws IOException, ClassNotFoundException
    {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(payload))
        {
            try (ObjectInputStream ois = new ObjectInputStream(bis))
            {
                return ois.readObject();
            }
        }
    }


    public CorfuDBStreamEntry(UUID streamID, byte[] payload, long epoch)
        throws IOException
    {
        this.payload = payload;
        this.ts = new Timestamp(streamID, epoch);
    }

    public CorfuDBStreamEntry(UUID streamID, long epoch)
    {
        this.ts = new Timestamp(streamID, epoch);
    }

    public CorfuDBStreamEntry(Map<UUID, Long> epochMap, Serializable payloadObject)
    throws IOException
    {
        try (ByteArrayOutputStream bs = new ByteArrayOutputStream())
        {
            try (ObjectOutput out = new ObjectOutputStream(bs))
            {
                out.writeObject(payloadObject);
                payload = bs.toByteArray();
            }
        }

        this.ts = new Timestamp(epochMap);
    }

    public CorfuDBStreamEntry(Map<UUID, Long> epochMap, byte[] payload)
    {
        this.payload = payload;
        this.ts = new Timestamp(epochMap);
    }

    public CorfuDBStreamEntry(Map<UUID, Long> epochMap)
    {
        this.ts = new Timestamp(epochMap);
    }

    public byte[] getPayload() {
        return payload;
    }

    public int compareTo(CorfuDBStreamEntry cse)
    {
        return cse.ts.compareTo(cse.getTimestamp());
    }

    public void setTimestamp(Timestamp ts)
    {
        this.ts = ts;
    }

    public Timestamp getTimestamp()
    {
        return ts;
    }

    public boolean containsStream(UUID stream)
    {
        return ts.epochMap.containsKey(stream);
    }

    public boolean checkEpoch(Map<UUID, Long> epochMap)
    {
        boolean isContained = false;
        for (UUID id : ts.epochMap.keySet())
        {
            if (epochMap.containsKey(id))
            {
                isContained = true;
                if (!ts.epochMap.get(id).equals(epochMap.get(id)))
                {
                    return false;
                }
            }
        }
        return isContained;
    }

}
