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
    public UUID streamID;
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

        this.streamID = streamID;
        this.ts = new Timestamp(epoch);
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
        this.streamID = streamID;
        this.ts = new Timestamp(epoch);
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

    public UUID getStreamID() {
        return this.streamID;
    }

}
