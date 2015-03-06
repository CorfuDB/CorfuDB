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

import org.corfudb.client.view.Sequencer;
import org.corfudb.client.view.WriteOnceAddressSpace;
import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.entries.CorfuDBEntry;
import org.corfudb.client.entries.CorfuDBStreamEntry;
import org.corfudb.client.OutOfSpaceException;

import java.util.Map;
import java.util.ArrayList;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.corfudb.client.Timestamp;
import org.corfudb.client.UnwrittenException;
import org.corfudb.client.TrimmedException;
import java.lang.ClassNotFoundException;
import java.io.IOException;
import java.io.Serializable;

public class Stream implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(Stream.class);

    CorfuDBClient cdbc;
    UUID streamID;

    Sequencer sequencer;
    WriteOnceAddressSpace woas;

    ExecutorService executor;

    PriorityBlockingQueue<CorfuDBEntry> logQ;
    AtomicLong dispatchedReads;
    AtomicLong logPointer;

    PriorityBlockingQueue<CorfuDBStreamEntry> streamQ;
    AtomicLong streamPointer;

    public Stream(CorfuDBClient cdbc, UUID uuid) {
        this(cdbc, uuid, Runtime.getRuntime().availableProcessors());
    }

    public Stream(CorfuDBClient cdbc, UUID uuid, int numThreads) {
        this.cdbc  = cdbc;
        this.streamID = uuid;
        sequencer = new Sequencer(cdbc);
        woas = new WriteOnceAddressSpace(cdbc);
        executor = Executors.newFixedThreadPool(numThreads);
        logQ = new PriorityBlockingQueue<CorfuDBEntry>();
        streamQ = new PriorityBlockingQueue<CorfuDBStreamEntry>();
        dispatchedReads = new AtomicLong();
        streamPointer = new AtomicLong();
        logPointer = new AtomicLong();
        getLogTailAndDispatch();
        readIntoStream();
    }

    private CompletableFuture<Void> readIntoStream()
    {
        return CompletableFuture.runAsync(() -> {
            while (true) {
                try {
                    CorfuDBEntry c = logQ.take();
                    if (c.getPhysicalPosition() == logPointer.get())
                    {
                        logPointer.getAndIncrement();
                        CorfuDBStreamEntry cdse = (CorfuDBStreamEntry) c.deserializePayload();
                        if (cdse.getStreamID().equals(streamID))
                        {
                            cdse.setTimestamp(new Timestamp(0, streamPointer.getAndIncrement()));
                            streamQ.offer(cdse);
                        }
                    }
                    else
                    {
                        logQ.offer(c);
                    }
                } catch (InterruptedException ie)
                {

                }
                catch (IOException ioe)
                {}
                catch (ClassNotFoundException cnfe)
                {

                }
            }
        }, executor);
    }

    private CompletableFuture<Void> dispatchRead(long logPos)
    {
        return CompletableFuture.runAsync(() -> {
            try {
                byte[] data = woas.read(logPos);
                CorfuDBEntry cde = new CorfuDBEntry(logPos, data);
                logQ.offer(cde);
            } catch (UnwrittenException ue)
            {
                //retry
                dispatchRead(logPos);
            } catch (TrimmedException te)
            {
                //tell the main code this entry was trimmed
            }
        },executor);
    }

    private CompletableFuture<Long> getLogTailAndDispatch()
    {
        CompletableFuture<Long> future = CompletableFuture.supplyAsync(() -> {
            return sequencer.getCurrent();
        }, executor);
            future.thenAcceptAsync( (tail) -> {
                long toDispatch;
                while ((toDispatch = dispatchedReads.getAndIncrement()) <= tail)
                {
                    final long logPos = toDispatch;
                    dispatchRead(logPos);
                }
                try { Thread.sleep(1000); } catch (InterruptedException ie) {}
                getLogTailAndDispatch();
            });
        return future;
    }

    public long append(byte[] data)
        throws OutOfSpaceException
    {
        while (true)
        {
            try {
                long token = sequencer.getNext();
                CorfuDBStreamEntry cdse = new CorfuDBStreamEntry(streamID, data);
                woas.write(token, (Serializable) cdse);
                return token;
            } catch(Exception e) {
                log.warn("Issue appending to log, getting new sequence number...", e);
            }
        }
    }

    public long append(Serializable data)
        throws OutOfSpaceException
    {
        while (true)
        {
            try {
                long token = sequencer.getNext();
                CorfuDBStreamEntry cdse = new CorfuDBStreamEntry(streamID, data);
                woas.write(token, (Serializable) cdse);
                return token;
            } catch(Exception e) {
                log.warn("Issue appending to log, getting new sequence number...", e);
            }
        }
    }

    public byte[] readNext()
    throws Exception
    {
        return streamQ.take().getPayload();
    }

    public Object readNextObject()
    throws Exception
    {
        return streamQ.take().deserializePayload();
    }

    public long check()
    {
        return sequencer.getCurrent();
    }

    public void trim(long address)
    {
    }

    public void close()
    {
        if (executor != null)
        {
            executor.shutdownNow();
        }
    }


}
