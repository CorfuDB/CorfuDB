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
import java.util.HashMap;
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

import org.corfudb.client.Timestamp;
import org.corfudb.client.OverwriteException;
import org.corfudb.client.abstractions.Stream;
import org.corfudb.client.view.StreamingSequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.CorfuDBClient;

import org.corfudb.client.view.Serializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.Input;
/**
 * This class represents entries which have a payload.
 */
public interface IPayload {
    static final Logger log = LoggerFactory.getLogger(IPayload.class);

    /**
     * Get the payload attached to this entry as a byte array.
     */
    byte[] getPayload();

    /**
     * Set the payload attached to this entry as a byte array.
     */
    void setPayload(byte[] payload);

    /**
     * Get the cached version of the entry, if it exists.
     */
    Object getDeserializedPayload();

    /**
     * Set the cached version of the entry.
     */
    void setDeserializedPayload(Object o);

    /**
     * Return the deserialized version of the payload.
     *
     * @return      A deserialized object representing the payload.
     */
    default Object deserializePayload()
        throws IOException, ClassNotFoundException
    {
        Object o = getDeserializedPayload();
        if (o != null) { return getDeserializedPayload(); }
        Kryo k = Serializer.kryos.get();
        try (ByteArrayInputStream bis = new ByteArrayInputStream(getPayload()))
        {
            try (Input i = new Input(bis, 16384))
            {
                o = k.readClassAndObject(i);
                setDeserializedPayload(o);
                return o;
            }
        }
    }

    default void serializePayload (Object o)
        throws IOException
    {
        Kryo k = Serializer.kryos.get();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream())
        {
            try (Output output = new Output(baos, 16384))
            {
                k.writeClassAndObject(output, o);
                output.flush();
                setPayload(baos.toByteArray());
            }
        }
    }

}
