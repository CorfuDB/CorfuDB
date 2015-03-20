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
import org.corfudb.client.OverwriteException;

/**
 * This is a bundle entry interface, which is encountered inside the move entries of remotely bundled
 * logs. It enables remote logs to easily write results to remotes.
 */
public interface IBundleEntry {

    /**
     * Writes a payload in the remote slot, and collects the result of the bundle into the local
     * stream in 1 RTT.
     *
     * @param payload       The payload to insert into the slot.
     *
     * @return              The timestamp for the remote append operation, or null, if there was no remote slot.
     */
    public Timestamp writeSlot(byte[] payload)
    throws OverwriteException, IOException;

    /**
     * Writes a serializable payload in the remote slot, and collects the result of the bundle into the local
     * stream in 1 RTT.
     *
     * @param payloadObject       The payload to insert into the slot.
     *
     * @return                      The timestamp for the remote append operation, or null, if there was no remote slot.
     */
    public Timestamp writeSlot(Serializable payloadObject)
    throws OverwriteException, IOException;
}
