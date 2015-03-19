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

package org.corfudb.client.abstractions;

import org.corfudb.client.view.StreamingSequencer;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.configmasters.IConfigMaster;
import org.corfudb.client.IServerProtocol;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.CorfuDBView;
import org.corfudb.client.entries.CorfuDBEntry;
import org.corfudb.client.entries.CorfuDBStreamEntry;
import org.corfudb.client.entries.CorfuDBStreamMoveEntry;
import org.corfudb.client.entries.CorfuDBStreamStartEntry;
import org.corfudb.client.OutOfSpaceException;
import org.corfudb.client.LinearizationException;

import java.util.Map;
import java.util.ArrayList;
import java.util.Queue;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.HashMap;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.TimeUnit;
import org.corfudb.client.Timestamp;
import org.corfudb.client.UnwrittenException;
import org.corfudb.client.TrimmedException;
import org.corfudb.client.RemoteException;
import org.corfudb.client.OutOfSpaceException;
import java.lang.ClassNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.stream.Collectors;
import org.corfudb.client.gossip.StreamEpochGossipEntry;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import java.util.function.Supplier;
import org.corfudb.client.StreamData;
import java.io.Serializable;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectOutput;
import java.io.IOException;

/**
 * This class creates bundles, which abstracts away pulling multiple logs and inserting a stream
 * into a single operation.
 */
class Bundle {

    IStream stream;
    List<UUID> remoteStreams;
    byte[] payload;
    boolean allocateSlots;

    public Bundle(IStream s, List<UUID> remoteStreams, byte[] payload, boolean allocateSlots)
    {
        this.stream = s;
        this.remoteStreams = remoteStreams;
        this.payload = payload;
        this.allocateSlots = allocateSlots;
    }

    public Bundle(IStream s, List<UUID> remoteStreams, Serializable payload, boolean allocateSlots)
    throws IOException
    {
        this.stream = s;
        this.remoteStreams = remoteStreams;
        try (ByteArrayOutputStream bs = new ByteArrayOutputStream())
        {
            try (ObjectOutput out = new ObjectOutputStream(bs))
            {
                out.writeObject(payload);
                this.payload = bs.toByteArray();
            }
        }

        this.allocateSlots = allocateSlots;
    }

    public Timestamp apply()
    throws RemoteException, OutOfSpaceException, IOException
    {
        return stream.pullStreamAsBundle(remoteStreams, payload, allocateSlots ? remoteStreams.size() : 0);
    }

}

