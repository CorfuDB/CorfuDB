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

package org.corfudb.runtime.entries;

import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.log.Timestamp;

import java.util.Map;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.io.Serializable;

import java.io.IOException;

import org.corfudb.runtime.OverwriteException;
import org.corfudb.runtime.log.Stream;
import org.corfudb.runtime.view.StreamingSequencer;
import org.corfudb.runtime.UnwrittenException;

import org.corfudb.runtime.view.IWriteOnceAddressSpace;
import org.corfudb.runtime.view.CachedWriteOnceAddressSpace;

import java.util.concurrent.CompletableFuture;
/**
 * This class implements a bundle entry, which is encountered inside the move entries of remotely bundled
 * logs. It enables remote logs to easily write results to remotes.
 */
public class BundleEntry extends CorfuDBStreamMoveEntry implements org.corfudb.runtime.entries.IBundleEntry {

    private final Logger log = LoggerFactory.getLogger(BundleEntry.class);

    long physicalPos;
    long numSlots;
    Map<UUID, Long> epochMap;

    transient Stream s;
    transient CorfuDBRuntime cdbc;
    transient IWriteOnceAddressSpace woas;
    transient StreamingSequencer ss;

    /**
    * This constructor should only be called by the bundle code.
    *
    * @param payload        The payload to insert into the bundle.
    * @param epochMap       A mapping of the current epochs which will be at the remote stream start entry.
    * @param remoteLog      The remote log the bundle is attached to.
    * @param numSlots       t
    * @param physicalPos    The physical position of the remote slot, if allocated, or -1, if there is no remote slot.
    */
    public BundleEntry(Map<UUID, Long> epochMap, UUID destinationLog, UUID destinationStream, long destinationPos, long destinationEpoch,
                        Serializable payload, int numSlots, long physicalPos)
        throws IOException
    {
        super(epochMap, destinationLog, destinationStream, destinationPos, (int) numSlots+1, destinationEpoch, payload);
        this.originalAddress = destinationPos;
        this.isCopy = true;
        this.physicalPos = physicalPos;
        this.originalStream = destinationStream;
        this.epochMap = new HashMap<UUID, Long>(epochMap);
        this.numSlots = numSlots;
    }

    /**
     * This function is called by the stream upcall. It sets the transient information for the bundle.
     *
     * @param s             The stream that read this entry.
     * @param woas          The write once address space of the stream.
     * @param ss            The streaming sequencer for the stream.
     * @param cdbc          The corfudbclient of the stream.
     */
    public void setTransientInfo(Stream s, IWriteOnceAddressSpace woas, StreamingSequencer ss, CorfuDBRuntime cdbc)
    {
        this.s = s;
        this.woas = woas;
        this.ss = ss;
        this.cdbc = cdbc;
    }

    /**
     * Writes a payload in the remote slot, and collects the result of the bundle into the local
     * stream in 1 RTT.
     *
     * @param payload       The payload to insert into the slot.
     *
     * @return              The timestamp for the remote append operation, or null, if there was no remote slot.
     */
    public Timestamp writeSlot(byte[] payload)
    throws OverwriteException, IOException
    {
        return writeSlot((Serializable) payload);
    }

    /**
     * Writes a serializable payload in the remote slot, and collects the result of the bundle into the local
     * stream in 1 RTT.
     *
     * @param payloadObject       The payload to insert into the slot.
     *
     * @return                      The timestamp for the remote append operation, or null, if there was no remote slot.
     */
    public Timestamp writeSlot(Serializable payloadObject)
    throws OverwriteException, IOException
    {
        if (physicalPos == -1)
        {
            return null;
        }

        //  Write the payload to the remote (TODO: talk to the configuration master instead)
        final IWriteOnceAddressSpace remote_woas = new CachedWriteOnceAddressSpace(cdbc, destinationLog);
        final CorfuDBStreamEntry cdbse = new CorfuDBStreamEntry(epochMap, payloadObject);
        remote_woas.write(physicalPos, cdbse);

        // Read all payloads from
        // Read the remote payload and write it to the local slots
        for (int i = 0; i < numSlots; i++)
        {
            final int slotNum = i;
            CompletableFuture<Void> cf = CompletableFuture.runAsync( () -> {
            while (true)
            {
                try {
                    long posDest = destinationPos + slotNum + 1;
                    CorfuDBStreamEntry cdbse2 = (CorfuDBStreamEntry) remote_woas.readObject(posDest);
                    cdbse2.originalAddress = posDest;
                    cdbse2.isCopy = true;
                    woas.write(realPhysicalPos + slotNum + 1, cdbse2);
                    break;
                }
                catch (UnwrittenException ue) {}
                catch (OverwriteException oe) {break;}
                catch (ClassNotFoundException cnfe) {break;}
                catch (IOException e) { break;}
            }
            }, s.executor);
        }
        return new Timestamp(epochMap, null, physicalPos, s.streamID);
    }

}
