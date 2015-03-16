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
import org.corfudb.client.configmasters.IConfigMaster.streamInfo;
import org.corfudb.client.IServerProtocol;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.entries.CorfuDBEntry;
import org.corfudb.client.entries.CorfuDBStreamEntry;
import org.corfudb.client.entries.CorfuDBStreamMoveEntry;
import org.corfudb.client.entries.CorfuDBStreamStartEntry;
import org.corfudb.client.OutOfSpaceException;

import java.util.Map;
import java.util.ArrayList;
import java.util.Queue;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

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
import java.lang.ClassNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.stream.Collectors;

/**
 *  A hop-aware stream implementation. The stream must be closed after the application is done using it
 *  to free resources, or enclosed in a try-resource block.
 */
public class Stream implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(Stream.class);

    CorfuDBClient cdbc;
    UUID streamID;

    StreamingSequencer sequencer;
    WriteOnceAddressSpace woas;

    ExecutorService executor;
    boolean prefetch = true;


    PriorityBlockingQueue<CorfuDBEntry> logQ;
    AtomicLong dispatchedReads;
    AtomicLong logPointer;

    boolean closed = false;
    boolean killExecutor = false;
    PriorityBlockingQueue<CorfuDBStreamEntry> streamQ;
    CompletableFuture<Void> currentDispatch;
    AtomicLong streamPointer;
    int queueMax;

    public Stream(CorfuDBClient cdbc, UUID uuid) {
        this(cdbc, uuid, 4, 10, Runtime.getRuntime().availableProcessors(), true);
    }

    public Stream(CorfuDBClient cdbc, UUID uuid, int queueSize, int allocationSize, boolean prefetch) {
        this(cdbc,uuid,queueSize, allocationSize, Runtime.getRuntime().availableProcessors(), prefetch);
    }

    public Stream(CorfuDBClient cdbc, UUID uuid, int queueSize, int allocationSize, int numThreads, boolean prefetch) {
        this(cdbc, uuid, queueSize, allocationSize, Executors.newFixedThreadPool(numThreads), prefetch);
        killExecutor = true;
    }

    public Stream(CorfuDBClient cdbc, UUID uuid, int queueSize,  int allocationSize, ExecutorService executor, boolean prefetch)
    {
        this.cdbc  = cdbc;
        this.streamID = uuid;
        sequencer = new StreamingSequencer(cdbc);
        woas = new WriteOnceAddressSpace(cdbc);
        logQ = new PriorityBlockingQueue<CorfuDBEntry>();
        streamQ = new PriorityBlockingQueue<CorfuDBStreamEntry>();
        dispatchedReads = new AtomicLong();
        streamPointer = new AtomicLong();
        logPointer = new AtomicLong();
        queueMax = queueSize;
        this.prefetch = prefetch;
        this.executor = executor;
        //now, the stream starts reading from the beginning...
        IConfigMaster cm = (IConfigMaster) cdbc.getView().getConfigMasters().get(0);
        //does the stream exist? create if it does not
        streamInfo si = cm.getStream(streamID);
        //it doesn't, so try to create
        while (si == null)
        {
            try {
                long sequenceNo = sequencer.getNext(uuid);
                CorfuDBStreamStartEntry cdsse = new CorfuDBStreamStartEntry(streamID);
                woas.write(sequenceNo, cdsse);
                cm.addStream(cdbc.getView().getUUID(), streamID, sequenceNo);
                sequencer.setAllocationSize(streamID, allocationSize);
                si = cm.getStream(streamID);
            } catch (IOException ie)
            {
                log.debug("Warning, couldn't get streaminfo, retrying...", ie);
            }
        }
        dispatchedReads.set(si.startPos);
        if (prefetch)
        {
            currentDispatch = getStreamTailAndDispatch(queueSize);
        }
    }

    enum ReadResultType {
        SUCCESS,
        UNWRITTEN,
        TRIMMED
    }

    class ReadResult {
        public long pos;
        public ReadResultType resultType;
        public CorfuDBEntry payload;

        public ReadResult(long pos){
            this.pos = pos;
        }
    }

    private CompletableFuture<ReadResult> dispatchDetailRead(long logPos)
    {
        return CompletableFuture.supplyAsync(() -> {
           // log.debug("dispatch " + streamID.toString() + " " + logPos);
            ReadResult r = new ReadResult(logPos);
            try {
                byte[] data = woas.read(logPos);
                CorfuDBEntry cde = new CorfuDBEntry(logPos, data);
                //success
                r.resultType = ReadResultType.SUCCESS;
                r.payload = cde;
            } catch (UnwrittenException ue)
            {
                //retry
                r.resultType = ReadResultType.UNWRITTEN;
            } catch (TrimmedException te)
            {
                //tell the main code this entry was trimmed
                r.resultType = ReadResultType.TRIMMED;
            }
            return r;
        },executor);
    }

    @SuppressWarnings("rawtypes")
    private <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> futures) {
        CompletableFuture<Void> allDoneFuture =
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
        return allDoneFuture.thenApplyAsync(v ->
                futures.stream().
                        map(future -> future.join()).
                        collect(Collectors.<T>toList()), executor
        );
    }
    @SuppressWarnings("rawtypes")
    private CompletableFuture<Void> getStreamTailAndDispatch(long numReads)
    {
        if (closed) { return null; }
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                //dispatch N reads and wait to check if any of them are moves
                long toDispatch = dispatchedReads.get();
                List<CompletableFuture<ReadResult>> futureList = new ArrayList<CompletableFuture<ReadResult>>();
                for (long i = toDispatch; i < toDispatch + numReads; i++)
                {
                     futureList.add(dispatchDetailRead(i));
                }
                CompletableFuture<List<ReadResult>> readfutures = sequence(futureList);
                readfutures.thenAcceptAsync( (results) -> {
                    long numReadable = 0;
                    long highWatermark = toDispatch;
                    for (ReadResult r : results)
                    {
                        if (r.resultType == ReadResultType.SUCCESS)
                        {
                            if (closed) { return; }
                            highWatermark = r.pos + 1;
                            try {
                                Object payload = r.payload.deserializePayload();
                                if (payload instanceof CorfuDBStreamMoveEntry)
                                {
                                    CorfuDBStreamMoveEntry cdbsme = (CorfuDBStreamMoveEntry) payload;
                                    //flush what we've read (unless the stream starts at the next allocation,)
                                    //it's all bound to be invalid...
                                    dispatchedReads.set(cdbsme.destinationPos);
                                    currentDispatch = getStreamTailAndDispatch(1);
                                    return;
                                }
                                else if (payload instanceof CorfuDBStreamStartEntry)
                                {
                                }
                                else if (payload instanceof CorfuDBStreamEntry)
                                {
                                    CorfuDBStreamEntry cdbse = (CorfuDBStreamEntry) payload;
                                    if (cdbse.getStreamID().equals(streamID))
                                    {
                                        cdbse.setTimestamp(new Timestamp(0, streamPointer.getAndIncrement()));
                                        numReadable++;
                                        streamQ.offer(cdbse);
                                    }
                                }
                            }
                            catch (NullPointerException npe)
                            {

                            }
                            catch (Exception e)
                            {
                                log.error("Exception reading payload", e);
                            }
                        }
                    }
                    dispatchedReads.set(highWatermark);
                    //check if the queue is full
                    while (streamQ.size() >= queueMax)
                    {
                        if (!prefetch)
                        {
                            return;
                        }
                        synchronized (streamQ)
                        {
                            try {
                                streamQ.wait();
                            }
                            catch (InterruptedException ie) {}
                        }
                    }
                    //increase number of reads if all entries are valid
                    if (numReadable == results.size()) {
                        numReadable = numReadable * 2; }
                    if (closed) {return;}
                    currentDispatch = getStreamTailAndDispatch(Math.max(numReadable, 1));
                }, executor);
            }, executor);
        return future;
    }

    public long append(byte[] data)
        throws OutOfSpaceException
    {
        while (true)
        {
            try {
                long token = sequencer.getNext(streamID);
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
                long token = sequencer.getNext(streamID);
                CorfuDBStreamEntry cdse = new CorfuDBStreamEntry(streamID, data);
                woas.write(token, (Serializable) cdse);
                return token;
            } catch(Exception e) {
                log.warn("Issue appending to log, getting new sequence number...", e);
            }
        }
    }

    public CorfuDBStreamEntry readNextEntry()
    throws IOException, InterruptedException
    {
        if (closed) { throw new IOException("Reading from closed stream!"); }
        if (!prefetch)
        {
            CorfuDBStreamEntry entry = streamQ.poll();
            while (entry == null)
            {
                synchronized (streamQ)
                {
                    if (currentDispatch == null || currentDispatch.isDone())
                    {
                        currentDispatch = getStreamTailAndDispatch(queueMax);
                    }
                    else
                    {
                    }
                }
                entry = streamQ.poll(100, TimeUnit.MILLISECONDS);
            }
            return entry;
        }
        else
        {
            synchronized(streamQ){
                streamQ.notify();
            }
            return streamQ.take();
        }
    }

    public byte[] readNext()
    throws IOException, InterruptedException
    {
        return readNextEntry().getPayload();
    }

    public Object readNextObject()
    throws IOException, InterruptedException, ClassNotFoundException
    {
        return readNextEntry().deserializePayload();
    }

    public long check()
    {
        return sequencer.getCurrent(streamID);
    }

    public void trim(long address)
    {
    }

    public void close()
    {
        if (executor != null)
        {
            closed = true;
            if (killExecutor)
            {
                executor.shutdownNow();
            }
            synchronized(streamQ)
            {
                streamQ.notify();
            }
            streamQ.clear();
        }
    }


}
