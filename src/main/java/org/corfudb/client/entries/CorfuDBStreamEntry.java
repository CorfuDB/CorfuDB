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

/**
 * This class represents an entry inside a stream.
 */
public class CorfuDBStreamEntry implements IPayload, Serializable, Comparable<CorfuDBStreamEntry>
{
    private static final long serialVersionUID = 0L;
    /** The timestamp of the entry */
    public Timestamp ts;
    /** The payload of the entry, which may be null. */
    public byte[] payload;
    /** Whether or not this entry is a copy */
    public boolean isCopy;
    /** If it is a copy, what the original address was */
    public long originalAddress;
    /** A transient variable to hold the actual physical position */
    transient long realPhysicalPos;
    /** If it is a copy, what the original stream was */
    UUID originalStream;
    transient Object deserializedPayload;

    public Object getDeserializedPayload()
    {
        return deserializedPayload;
    }

    public void setDeserializedPayload(Object o)
    {
        deserializedPayload = o;
    }

    /** Hidden default constructor */
    private CorfuDBStreamEntry() {}

    /**
     * Create a new CorfuDBStreamEntry with a serializable object, residing in a single stream.
     *
     * @param streamID      The ID of the stream the entry is in.
     * @param payloadObject A serializable object containing the payload to insert.
     * @param epoch         The epoch of the entry.
     */
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

    /**
     * Create a new CorfuDBStreamEntry with a byte array payload, residing in a single stream.
     *
     * @param streamID      The ID of the stream the entry is in.
     * @param payload       A byte array representing the payload to inert.
     * @param epoch         The epoch of the entry.
     */
    public CorfuDBStreamEntry(UUID streamID, byte[] payload, long epoch)
        throws IOException
    {
        this.payload = payload;
        this.ts = new Timestamp(streamID, epoch);
    }

    /**
     * Create a new CorfuDBStreamEntry with no payload, residing in a single stream.
     *
     * @param streamID      The ID of the stream the entry is in.
     * @param epoch         The epoch of the entry.
     */
    public CorfuDBStreamEntry(UUID streamID, long epoch)
    {
        this.ts = new Timestamp(streamID, epoch);
    }

    /**
     * Create a new CorfuDBStreamEntry with a serializable payload, residing in multiple streams.
     *
     * @param epochMap      A map containing all the streams and epochs this entry is to be inserted in.
     * @param payloadObject The serializable object to insert.
     */
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

    /**
     * Create a new CorfuDBStreamEntry with a byte array payload, residing in multiple streams.
     *
     * @param epochMap      A map containing all the streams and epochs this entry is to be inserted in.
     * @param payload       A byte array representing the payload to insert.
     */
    public CorfuDBStreamEntry(Map<UUID, Long> epochMap, byte[] payload)
    {
        this.payload = payload;
        this.ts = new Timestamp(epochMap);
    }

    /**
     * Create a new CorfuDBStreamEntry with no payload, residing in multiple streams.
     *
     * @param epochMap      A map containing all the streams and epochs this entry is to be inserted in.
     */
    public CorfuDBStreamEntry(Map<UUID, Long> epochMap)
    {
        this.ts = new Timestamp(epochMap);
    }

    /**
     * Get the payload attached to this entry as a byte array.
     */
    public byte[] getPayload() {
        return payload;
    }

    /**
     * Restore the original address.
     */
    public void restoreOriginalPhysical(UUID primaryStream)
    {
        if (isCopy)
        {
            this.realPhysicalPos = ts.getPhysicalPos();
            ts.setPhysicalPos(originalAddress);
            ts.setContainingStream(originalStream);
            ts.setCopyAddress(this.realPhysicalPos, primaryStream);
        }
    }

    /**
     * Compare this entry to another object, and return based on order.
     *
     * @param cse           The entry to compare against.
     *
     * @return              0 if the entries are at the same position (and therefore the same entry),
     *                      >0 if this entry is ahead of the provided entry,
     *                      <0 if this entry is behind the provided entry.
     */
    @Override
    public int compareTo(CorfuDBStreamEntry cse)
    {
        return cse.ts.compareTo(cse.getTimestamp());
    }

    /**
     * Set the timestamp for this entry.
     *
     * @param ts            The timestamp to set.
     */
    public void setTimestamp(Timestamp ts)
    {
        this.ts = ts;
    }

    /**
     * Retrieve the timestamp for this entry.
     *
     * @return              The timestamp for this entry.
     */
    public Timestamp getTimestamp()
    {
        return ts;
    }

    /**
     * Returns whether or not the entry is in a given stream.
     *
     * @param id            The UUID of the stream to check.
     *
     * @return              True, if the entry is in the stream, false otherwise.
     */
    public boolean containsStream(UUID stream)
    {
        return ts.epochMap.containsKey(stream);
    }

    /**
     * Returns whether or not the entry is in a given epochmap.
     *
     * @param epochMap      A map describing the epoch to check against.
     *
     * @return              True, if the entry is in the epoch, false otherwise.
     */
    public boolean checkEpoch(Map<UUID, Long> epochMap)
    {
        return ts.checkEpoch(epochMap);
    }

}
