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

package org.corfudb.runtime.stream;

import org.corfudb.runtime.OutOfSpaceException;

import java.util.List;

import java.util.UUID;

import org.corfudb.runtime.RemoteException;

import java.io.IOException;
import java.io.Serializable;

/**
 * This class creates bundles, which abstracts away pulling multiple logs and inserting a stream
 * into a single operation.
 */
public class OldBundle {

    IHopStream stream;
    List<UUID> remoteStreams;
    Serializable payload;
    boolean allocateSlots;

    public OldBundle(IHopStream s, List<UUID> remoteStreams, byte[] payload, boolean allocateSlots)
    throws IOException
    {
        this.stream = s;
        this.remoteStreams = remoteStreams;
        this.payload = payload;
        this.allocateSlots = allocateSlots;
    }

    public OldBundle(IHopStream s, List<UUID> remoteStreams, Serializable payload, boolean allocateSlots)
    throws IOException
    {
        this.stream = s;
        this.remoteStreams = remoteStreams;
        this.payload = payload;
        this.allocateSlots = allocateSlots;
    }

    public Timestamp apply()
    throws RemoteException, OutOfSpaceException, IOException
    {
        return stream.pullStreamAsBundle(remoteStreams, payload, allocateSlots ? remoteStreams.size() : 0);
    }
}

