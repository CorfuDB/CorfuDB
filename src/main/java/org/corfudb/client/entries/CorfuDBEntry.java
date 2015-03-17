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
import org.corfudb.client.OutOfSpaceException;

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

public class CorfuDBEntry implements Serializable, Comparable<CorfuDBEntry>
{
    private static final long serialVersionUID = 0L;
    public transient final long physPos;
    public byte[] payload;

    public CorfuDBEntry()
    {
        this.physPos = -1;
    }
    public CorfuDBEntry(long physPos)
    {
        this.physPos = physPos;
    }

    public CorfuDBEntry(long physPos, byte[] payload)
    {
        this.physPos = physPos;
        this.payload = payload;
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

    public long getPhysicalPosition()
    {
        return this.physPos;
    }

    public int compareTo(CorfuDBEntry o)
    {
        return (int) (physPos - o.getPhysicalPosition());
    }

}
