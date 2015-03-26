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

import org.corfudb.client.view.IWriteOnceAddressSpace;
import org.corfudb.client.view.CachedWriteOnceAddressSpace;
import org.corfudb.client.view.ObjectCachedWriteOnceAddressSpace;
import org.corfudb.client.view.StreamingSequencer;
import org.corfudb.client.view.Sequencer;
import org.corfudb.client.configmasters.IConfigMaster;
import org.corfudb.client.IServerProtocol;

import org.corfudb.client.CorfuDBClient;
import org.corfudb.client.CorfuDBView;
import org.corfudb.client.entries.CorfuDBEntry;
import org.corfudb.client.entries.CorfuDBStreamEntry;
import org.corfudb.client.entries.CorfuDBStreamMoveEntry;
import org.corfudb.client.entries.CorfuDBStreamStartEntry;
import org.corfudb.client.entries.BundleEntry;
import org.corfudb.client.OutOfSpaceException;
import org.corfudb.client.LinearizationException;
import org.corfudb.client.OverwriteException;
import org.corfudb.client.gossip.StreamBundleGossip;
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
import org.corfudb.client.gossip.StreamPullGossip;
import org.corfudb.client.entries.CorfuDBStreamHoleEntry;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiFunction;

import java.util.function.Supplier;
import org.corfudb.client.StreamData;
import java.io.Serializable;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectOutput;
import java.io.IOException;


/**
 *  A hop-aware stream implementation. The stream must be closed after the application is done using it
 *  to free resources, or enclosed in a try-resource block.
 */
public class Stream implements AutoCloseable, IStream {

    private final Logger log = LoggerFactory.getLogger(Stream.class);

    CorfuDBClient cdbc;
    public UUID streamID;
    UUID logID;

    StreamingSequencer sequencer;
    IWriteOnceAddressSpace woas;

    public ExecutorService executor;
    boolean prefetch = true;

    AtomicLong dispatchedReads;
    AtomicLong logPointer;
    AtomicLong minorEpoch;
    int backoffCounter;
    static int MAX_BACKOFF = 4;
    ConcurrentHashMap<UUID, Long> epochMap;

    Timestamp latest = null;
    Timestamp latestPrimary = null;

    boolean closed = false;
    boolean killExecutor = false;
    LinkedBlockingQueue<CorfuDBStreamEntry> streamQ;
    CompletableFuture<Void> currentDispatch;
    AtomicLong streamPointer;
    int queueMax;

    Long logpos;

    BlockingDeque<UUID> streamIDstack;

    class ReturnInfo
    {
        public UUID returnLog;
        public long physicalPos;
        public long returnCounter;

        public ReturnInfo(UUID returnLog, long physicalPos, long returnCounter) {
            this.returnLog = returnLog;
            this.physicalPos = physicalPos;
            this.returnCounter = returnCounter;
        }
    }

    static ConcurrentHashMap<UUID, StreamData> remoteStreamMap = new ConcurrentHashMap<UUID, StreamData>();

    BlockingDeque<ReturnInfo> returnStack;
    Supplier<CorfuDBView> getView;
    Supplier<IConfigMaster> getConfigMaster;
    BiFunction<CorfuDBClient, UUID, IWriteOnceAddressSpace> getAddressSpace;

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

    /**
     * Construct a new stream.
     *
     * @param cdbc              The CorfuDB client to construct the stream with.
     * @param uuid              The UUID of the stream to use.
     * @param queueSize         The size of the internal stream queue.
     * @param allocationSize    The allocation size to request from the sequencer, if supported.
     * @param executor          The executor service thread pool to serve threads on.
     * @param prefetch          Whether or not to prefetch data (up to the size of the internal queue)
     */
    public Stream(CorfuDBClient cdbc, UUID uuid, int queueSize,  int allocationSize, ExecutorService executor, boolean prefetch)
    {
        this.cdbc  = cdbc;
        this.streamID = uuid;
        this.logID = cdbc.getView().getUUID();
        sequencer = new StreamingSequencer(cdbc);
        streamQ = new LinkedBlockingQueue<CorfuDBStreamEntry>();
        streamIDstack = new LinkedBlockingDeque<UUID>();
        returnStack = new LinkedBlockingDeque<ReturnInfo>();
        epochMap = new ConcurrentHashMap<UUID, Long>();
        epochMap.put(uuid, 0L);
        //stack our ID onto the stack
        while (!streamIDstack.offerLast(uuid)) {}
        dispatchedReads = new AtomicLong();
        streamPointer = new AtomicLong();
        logPointer = new AtomicLong();
        queueMax = queueSize;
        getView = () -> {
            return this.cdbc.getView();
        };
        getConfigMaster = () -> {
            return (IConfigMaster) this.getView.get().getConfigMasters().get(0);
        };
        getAddressSpace = (client, logid) -> {
            return new ObjectCachedWriteOnceAddressSpace(client, logid);
        };
        backoffCounter = 0;
        woas = getAddressSpace.apply(cdbc, logID);
        this.prefetch = prefetch;
        this.executor = executor;
        this.minorEpoch = new AtomicLong();
        //now, the stream starts reading from the beginning...
        StreamData sd = getConfigMaster.get().getStream(streamID);
        //it doesn't, so try to create
        while (sd == null)
        {
            try {
                long sequenceNo = sequencer.getNext(uuid);
                CorfuDBStreamStartEntry cdsse = new CorfuDBStreamStartEntry(streamID, getCurrentEpoch());
                woas.write(sequenceNo, cdsse);
                getConfigMaster.get().addStream(cdbc.getView().getUUID(), streamID, sequenceNo);
                sequencer.setAllocationSize(streamID, allocationSize);
                sd = getConfigMaster.get().getStream(streamID);
            } catch (IOException ie)
            {
                log.debug("Warning, couldn't get streaminfo, retrying...", ie);
            }
        }
        dispatchedReads.set(sd.startPos);
        if (prefetch)
        {
            currentDispatch = getStreamTailAndDispatch(queueSize);
        }
    }

    private Long getCurrentEpoch()
    {
        return epochMap.get(streamID);
    }

    private void incrementAllEpochs()
    {
        for (UUID stream : epochMap.keySet())
        {
            epochMap.put(stream, epochMap.get(stream)+1);
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
        public Object payload;

        public ReadResult(long pos){
            this.pos = pos;
        }
    }

    private CompletableFuture<ReadResult> dispatchDetailRead(long logPos)
    {
        return CompletableFuture.supplyAsync(() -> {
            ReadResult r = new ReadResult(logPos);
            r.pos = logPos;
            try {
                r.payload = woas.readObject(logPos);
                //success
                r.resultType = ReadResultType.SUCCESS;
            } catch (UnwrittenException ue)
            {
                //retry
                r.resultType = ReadResultType.UNWRITTEN;

            } catch (TrimmedException te)
            {
                //tell the main code this entry was trimmed
                r.resultType = ReadResultType.TRIMMED;
            }
            catch (ClassNotFoundException cnfe)
            {
                log.warn("Unable to read object at logpos {}, class not found!", cnfe);
            } catch (IOException ie)
            {
                log.warn("Unable to read object at logpos {}, ioexception", ie);
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

    enum PayloadReadResult
    {
        VALID,
        INVALID,
        MOVECOMPLETE
    }

    private PayloadReadResult loadPayloadIntoQueue(CorfuDBStreamEntry cdbse, long physicalPos, long logpos)
    {
        PayloadReadResult prr = PayloadReadResult.INVALID;
        if (cdbse.checkEpoch(epochMap))
        {
            cdbse.getTimestamp().setTransientInfo(logID, streamID, logpos, physicalPos);
            cdbse.getTimestamp().setContainingStream(streamIDstack.peekLast());
            cdbse.restoreOriginalPhysical(streamIDstack.peekLast());

            if (cdbse instanceof BundleEntry)
            {
                //a bundle entry actually represents an entry in the remote log (physically).
                BundleEntry be = (BundleEntry) cdbse;
                be.setTransientInfo(this, woas, sequencer, cdbc);
            }
            synchronized (streamPointer) {
                latest = cdbse.getTimestamp();
                //log.info("set latest = {}", cdbse.getTimestamp());
                if (latest == null || latest.epochMap == null)
                {
                    log.warn("uh, null epoch map? at {} ", physicalPos);
                }
                streamPointer.notifyAll();
            }
            prr = PayloadReadResult.VALID;
            while (true) {
               try {
                streamQ.put(cdbse);
                break;
                } catch (InterruptedException e) {}
            }
            if (returnStack.size() != 0)
            {
                ReturnInfo currentReturn = returnStack.peekLast();
                currentReturn.returnCounter = currentReturn.returnCounter - 1;
                if (currentReturn.returnCounter <= 0)
                {
                    log.debug("Temporary move completed, returning to log {} at {}", currentReturn.returnLog, currentReturn.physicalPos);
                    if (!currentReturn.returnLog.equals(logID))
                    {
                        woas = getAddressSpace.apply(cdbc, currentReturn.returnLog);
                        sequencer = new StreamingSequencer(cdbc, currentReturn.returnLog);
                        logID = currentReturn.returnLog;
                    }
                    UUID curStream = null;
                    while (true) {
                        try {
                            curStream = streamIDstack.takeLast();
                            break;
                        } catch (InterruptedException ie) {}
                    }
                    epochMap.remove(curStream);
                    while (true) {
                        try {
                            returnStack.takeLast();
                            break;
                        } catch (InterruptedException ie) {}
                    }
                    dispatchedReads.set(currentReturn.physicalPos);
                    currentDispatch = getStreamTailAndDispatch(1);
                    prr = PayloadReadResult.MOVECOMPLETE;
                }
            }
        }
        else
        {
            log.warn("Ignored log entry from wrong epoch (expected {}, got {}, pos {}, stream {})", getCurrentEpoch(), cdbse.getTimestamp().getEpoch(streamID), physicalPos, streamID);
        }

        return prr;
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
                    int resultsindex = 0;
                    for (ReadResult r : results)
                    {
                        if (r.resultType == ReadResultType.UNWRITTEN)
                        {
                            //got to an unwritten entry - we need to
                            //1 - stop playback of the current stream until the entry is filled
                            //2 - decide whether or not to fill a hole here, if there are valid entries ahead.
                            //for now, we just stop playback, re-read, and hope for the best.
                            if (resultsindex + 1 < results.size())
                            {
                                //is the entry ahead of data?
                                //if so, fill a hole.
                                if (results.get(resultsindex + 1).resultType.equals(ReadResultType.SUCCESS))
                                {
                                    try {
                                    //be nice and wait some time before trying to fill
                                    Thread.sleep(50);
                                    woas.write(r.pos, new CorfuDBStreamHoleEntry(epochMap));
                                    log.warn("Filled detected hole at address {}", r.pos);
                                    }
                                    catch (InterruptedException ie) {}
                                    catch (OverwriteException oe) { log.debug("Tried to fill a hole at {} but it was written to. {}", r.pos); }
                                    catch (IOException ie) { log.warn("IOException attempting to fill hole at {}", r.pos, ie); }
                                }
                            }
                            dispatchedReads.set(highWatermark);
                            backoffCounter++;
                            if (backoffCounter > MAX_BACKOFF)
                            {
                                backoffCounter = MAX_BACKOFF;
                            }
                            try {
                            Thread.sleep((long)Math.pow(2, backoffCounter));}
                            catch (InterruptedException ie) {}
                            getStreamTailAndDispatch(2); //dispatch 2 so we can resolve any holes
                            return;
                        }
                        else if (r.resultType == ReadResultType.SUCCESS)
                        {
                            backoffCounter = 0;
                            if (closed) { return; }
                            highWatermark = r.pos + 1;

                            long logpos = streamPointer.getAndIncrement();
                            try {
                                Object payload = r.payload;
                                if (returnStack.size() == 0 && payload instanceof CorfuDBStreamEntry)
                                {
                                    CorfuDBStreamEntry cdbse= ((CorfuDBStreamEntry) payload);
                                    cdbse.getTimestamp().setTransientInfo(logID, streamID, logpos, r.pos);
                                    cdbse.getTimestamp().setContainingStream(streamIDstack.peekLast());
                                    cdbse.restoreOriginalPhysical(streamIDstack.peekLast());
                                    latestPrimary = cdbse.getTimestamp();
                                }
                                if (payload instanceof BundleEntry)
                                {
                                    BundleEntry be = (BundleEntry) payload;
                                    PayloadReadResult prr = loadPayloadIntoQueue(be, r.pos, logpos);
                                    if (prr == PayloadReadResult.VALID) { numReadable++; }
                                    else if (prr == PayloadReadResult.MOVECOMPLETE) { return; }
                                }
                                else if (payload instanceof CorfuDBStreamMoveEntry)
                                {
                                    CorfuDBStreamMoveEntry cdbsme = (CorfuDBStreamMoveEntry) payload;
                                    if (cdbsme.containsStream(streamID))
                                    {
                                        // This move is just an allocation boundary change. The epoch doesn't change,
                                        // just the read pointer.
                                        if (cdbsme.getTimestamp().getEpoch(streamID) == -1 && cdbsme.destinationStream == null)
                                        {
                                            log.debug("allocation boundary, moving from {} to {} on stream {}", r.pos, cdbsme.destinationPos, streamID);
                                            //flush what we've read (unless the stream starts at the next allocation,)
                                            //it's all bound to be invalid...
                                            dispatchedReads.set(cdbsme.destinationPos);
                                            currentDispatch = getStreamTailAndDispatch(1);
                                            return;
                                        }
                                        // This is an intentional move, either permanent, or temporary.
                                        else
                                        {
                                            if (cdbsme.duration == -1)
                                            {
                                                if (!cdbsme.destinationLog.equals(logID)){
                                                    log.debug("Detected permanent move operation to different log " + cdbsme.destinationLog);
                                                }
                                                else
                                                {
                                                    log.debug("Detected permanent move operation to same log " + cdbsme.destinationLog);
                                                }
                                                //since this is a perma-move, we increment the epoch
                                                incrementAllEpochs();
                                            }

                                            if (cdbsme.destinationStream != null)
                                            {
                                                if (cdbsme.duration == -1)
                                                {
                                                    log.debug("Detected permanent move operation into stream " + cdbsme.destinationStream);
                                                }
                                                else
                                                {
                                                    log.debug("Detected temporary move operation into stream  " + cdbsme.destinationStream + ", destination " + cdbsme.destinationLog);
                                                    returnStack.putLast(new ReturnInfo(logID, r.pos+1, cdbsme.duration));
                                                }
                                                streamIDstack.putLast(cdbsme.destinationStream);
                                                epochMap.put(cdbsme.destinationStream, cdbsme.destinationEpoch);
                                            }
                                            long newEpoch = getCurrentEpoch();
                                            if (cdbsme.duration == -1)
                                            {
                                                getConfigMaster.get().sendGossip(new StreamEpochGossipEntry(streamID, cdbsme.destinationLog, newEpoch, logpos));
                                            }
                                            //since this is on a different log, we change the address space and sequencer
                                            if (!cdbsme.destinationLog.equals(logID))
                                            {
                                                woas = getAddressSpace.apply(cdbc, cdbsme.destinationLog);
                                                sequencer = new StreamingSequencer(cdbc, cdbsme.destinationLog);
                                            }
                                            dispatchedReads.set(cdbsme.destinationPos);
                                            logID = cdbsme.destinationLog;
                                            synchronized(epochMap)
                                            {
                                                epochMap.notifyAll();
                                            }
                                            currentDispatch = getStreamTailAndDispatch(1);
                                            return;
                                        }
                                    }
                                }
                                else if (payload instanceof CorfuDBStreamStartEntry)
                                {
                                    CorfuDBStreamStartEntry cdsse = (CorfuDBStreamStartEntry) payload;
                                    if (cdsse.payload != null)
                                    {
                                        PayloadReadResult prr = loadPayloadIntoQueue(cdsse, r.pos, logpos);
                                        if (prr == PayloadReadResult.VALID) { numReadable++; }
                                        else if (prr == PayloadReadResult.MOVECOMPLETE) { return; }
                                    }
                                }
                                else if (payload instanceof CorfuDBStreamEntry && !(payload instanceof CorfuDBStreamHoleEntry))
                                {
                                    CorfuDBStreamEntry cdbse = (CorfuDBStreamEntry) payload;
                                    PayloadReadResult prr = loadPayloadIntoQueue(cdbse, r.pos, logpos);
                                    if (prr == PayloadReadResult.VALID) { numReadable++; }
                                    else if (prr == PayloadReadResult.MOVECOMPLETE) { return; }
                                }
                            }
                            catch (NullPointerException npe)
                            {
                                log.error("Null pointer exception during playback", npe);
                            }
                            catch (Exception e)
                            {
                                log.error("Exception reading payload", e);
                            }
                        }
                        resultsindex++;
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

    /**
     * Append a byte array to the stream. This operation may or may not be successful. For example,
     * a move operation may occur, and the append will not be part of the stream.
     *
     * @param data      A byte array to append to the stream.
     *
     * @return          A timestamp, which reflects the physical position and the epoch the data was written in.
     */
    public Timestamp append(byte[] data)
        throws OutOfSpaceException
    {
        return append((Serializable)data);
    }

    /**
     * Append an object to the stream. This operation may or may not be successful. For example,
     * a move operation may occur, and the append will not be part of the stream.
     *
     * @param data      A serializable object to append to the stream.
     *
     * @return          A timestamp, which reflects the physical position and the epoch the data was written in.
     */
    public Timestamp append(Serializable data)
        throws OutOfSpaceException
    {
        while (true)
        {
            try {
                UUID containingStream = streamIDstack.peekLast();
                long token = sequencer.getNext(containingStream);
                CorfuDBStreamEntry cdse = new CorfuDBStreamEntry(epochMap, data);
                woas.write(token, (Serializable) cdse);
                Timestamp ts = new Timestamp(epochMap, null, token, streamID);
                ts.setContainingStream(containingStream);
                return ts;
            } catch(Exception e) {
                log.warn("Issue appending to log, getting new sequence number...", e);
            }
        }
    }
    /**
     * Peek at the next entry in the stream as a CorfuDBStreamEntry. This function
     * peeks to see if there is an available entry in the stream to be read.
     *
     * @return      A CorfuDBStreamEntry containing the payload of the next entry in the stream, or null,
     *              if there is no entry available.
     */
    public CorfuDBStreamEntry peek()
    {
        return streamQ.peek();
    }

    /**
     * When the stream is not prefetching, manually requests a dispatch.
     */
    public void doDispatch()
    {
        if(!prefetch)
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
        }
    }

    /**When the stream is not prefetching, manually requests a dispatch, synchronously.
     *
     */
    public void doDispatchSync()
    {
        if(!prefetch)
        {
            synchronized (streamQ)
            {
                if (currentDispatch == null || currentDispatch.isDone())
                {
                    currentDispatch = getStreamTailAndDispatch(queueMax);
                    currentDispatch.join();
                }
                else
                {
                    currentDispatch.join();
                }
            }
        }
    }

    /**
     * Read the next entry in the stream as a CorfuDBStreamEntry. This function
     * retireves the next entry in the stream, blocking if necessary.
     *
     * @return      A CorfuDBStreamEntry containing the payload of the next entry in the stream.
     */
    public CorfuDBStreamEntry readNextEntry()
    throws IOException, InterruptedException
    {
        if (closed) { throw new IOException("Reading from closed stream!"); }
        if (!prefetch)
        {
            CorfuDBStreamEntry entry = streamQ.poll();
            while (entry == null)
            {
                doDispatch();
                entry = streamQ.poll(100, TimeUnit.MILLISECONDS);
            }
            return entry;
        }
        else
        {
            CorfuDBStreamEntry entry = streamQ.take();
            synchronized(streamQ){
                streamQ.notifyAll();
            }
            //log.info("Read next {}", entry.getTimestamp());
            return entry;
        }
    }

    /**
     * Read the next entry in the stream as a byte array. This convenience function
     * retireves the next entry in the stream, blocking if necessary.
     *
     * @return      A byte array containing the payload of the next entry in the stream.
     */
    public byte[] readNext()
    throws IOException, InterruptedException
    {
        return (byte[])readNextEntry().payload;
    }

    /**
     * Read the next entry in the stream as an Object. This convenience function
     * retrieves the next entry in the stream, blocking if necessary.
     *
     * @return      A deserialized object containing the payload of the next entry in the stream.
     */
    public Object readNextObject()
    throws IOException, InterruptedException, ClassNotFoundException
    {
        return readNextEntry().payload;
    }

    /**
     * Returns a fresh timestamp, which can serve as a linearization point. This function
     * may return a non-linearizable (invalid) timestamp which may never occur in the ordering
     * due to a move/epoch change.
     *
     * @return      A timestamp, which reflects the most recently allocated timestamp in the stream.
     */
    public Timestamp check()
    {
        Timestamp ts = new Timestamp(epochMap, null, sequencer.getCurrent(streamID), streamID);
        ts.setContainingStream(streamID);
        return ts;
    }

    /**
     * Returns a fresh or cached timestamp, which can serve as a linearization point. This function
     * may return a non-linearizable (invalid) timestamp which may never occur in the ordering
     * due to a move/epoch change.
     *
     * @param       cached      Whether or not the timestamp returned is cached.
     * @return                  A timestamp, which reflects the most recently allocated timestamp in the stream,
     *                          or currently read, depending on whether cached is set or not.
     */
    public Timestamp check(boolean cached)
    {
        if (cached) { return latest; }
        else { return check(); }
    }

    /**
     * Returns a fresh or cached timestamp, which can serve as a linearization point. This function
     * may return a non-linearizable (invalid) timestamp which may never occur in the ordering
     * due to a move/epoch change.
     *
     * @param       cached      Whether or not the timestamp returned is cached.
     * @param       primary     Whether or not to return timestamps only on the primary stream.
     * @return                  A timestamp, which reflects the most recently allocated timestamp in the stream,
     *                          or currently read, depending on whether cached is set or not.
     */
    public Timestamp check(boolean cached, boolean primary)
    {
        if (cached && primary) { return latestPrimary; }
        if (cached) { return latest; }
        else { return check(); }
    }

    /**
     * Requests a trim on this stream. This function informs the configuration master that the
     * position on this stream is trimmable, and moves the start position of this stream to the
     * new position.
     */
    public void trim(Timestamp address)
    {

    }

    /**
     * Synchronously block until we have seen the requested position.
     *
     * @param pos   The position to block until.
     */
    public void sync(Timestamp pos)
    throws LinearizationException, InterruptedException
    {
        sync(pos, -1);
    }

    /**
     * Synchronously block until we have seen the requested position, or a certain amount of real time has elapsed.
     *
     * @param pos       The position to block until.
     * @param timeout   The amount of time to wait. A negative number is interpreted as infinite.
     *
     * @return          True, if the sync was successful, or false if the timeout was reached.
     */
    public boolean sync(Timestamp pos, long timeout)
    throws LinearizationException, InterruptedException
    {
        while (true)
        {
            synchronized (streamPointer)
            {
                if (latest != null)
                {
                    try {
                    if (latest.compareTo(pos) >= 0)
                    {
                        return true;
                    }
                    else if (latest.getEpoch(streamID) != pos.getEpoch(streamID))
                    {
                        throw new LinearizationException("Linearization error due to epoch change", latest, pos);
                    }
                    } catch( ClassCastException cce)
                    {
                        throw new LinearizationException("Linearization error due to epoch change", latest, pos);
                    }
                }
                if (timeout < 0)
                {
                    streamPointer.wait();
                }
                else
                {
                    streamPointer.wait(timeout);
                    return false;
                }
            }
        }
    }

    /**
     *  Synchronously block until an epoch change is seen. Useful for quickly detecting
     *  when a permanent move is successful.
     */
    public void waitForEpochChange()
        throws InterruptedException
    {
        waitForEpochChange(-1);
    }

    /**
     *  Synchronously block until an epoch change is seen, or a certain amount of real time has elapsed.
     *  Useful for quickly detecting when a permanent move is successful.
     *
     * @param timeout   The amount of time to wait. A negative number is interpreted as infinite.
     * @return          True, if an epoch change occurs, or false if the timeout was reached.
     */
    public boolean waitForEpochChange(long timeout)
        throws InterruptedException
    {
        long last = getCurrentEpoch();
        synchronized(epochMap)
        {
            if (timeout < 0)
            {
                epochMap.wait();
            }
            else
            {
                epochMap.wait(timeout);
            }
            if (getCurrentEpoch() == last) { return false; }
        }
        return true;
    }

    /**
     *  Synchronously block until an epoch change is seen compared to a given timestamp. Useful for quickly detecting
     *  when a permanent move is successful.
     *
     *  @param t    The timestamp to compare against.
     */
    public void waitForEpochChange(Timestamp t)
        throws InterruptedException
    {
        waitForEpochChange(t, -1);
    }

    /**
     *  Synchronously block until an epoch change is seen compared to a given timestmap, or a certain amount of real time has elapsed.
     *  Useful for quickly detecting when a permanent move is successful.
     *
     * @param t         The timestamp to compare against.
     * @param timeout   The amount of time to wait. A negative number is interpreted as infinite.
     * @return          True, if an epoch change occurs, or false if the timeout was reached.
     */
    public boolean waitForEpochChange(Timestamp t, long timeout)
        throws InterruptedException
    {
        if (!t.checkEpoch(epochMap)) { return true; }
        synchronized(epochMap)
        {
            if (timeout < 0)
            {
                epochMap.wait();
            }
            else
            {
                epochMap.wait(timeout);
            }
            if (t.checkEpoch(epochMap)) { return false; }
        }
        return true;
    }



    /**
     * Permanently hop to another log. This function tries to hop this stream to
     * another log by obtaining a position in the destination log and inserting
     * a move entry from the source log to the destination log. It may or may not
     * be successful.
     *
     * @param destinationlog    The destination log to hop to.
     */
    public void hopLog(UUID destinationLog)
    throws RemoteException, OutOfSpaceException, IOException
    {
        // Get a sequence in the remote log
        StreamingSequencer sremote = new StreamingSequencer(cdbc, destinationLog);
        long remoteToken = sremote.getNext(streamIDstack.peekLast());
        // Write a start in the remote log
        IWriteOnceAddressSpace woasremote = getAddressSpace.apply(cdbc, destinationLog);
        woasremote.write(remoteToken, new CorfuDBStreamStartEntry(streamID, getCurrentEpoch() + 1));
        // Write the move request into the local log
        CorfuDBStreamMoveEntry cdbsme = new CorfuDBStreamMoveEntry(streamID, destinationLog, null, remoteToken, -1, getCurrentEpoch());
        long token = sequencer.getNext(streamID);
        woas.write(token, (Serializable) cdbsme);
    }

    /**
     * Permanently pull a remote stream into this stream. This function tries to
     * attach a remote stream to this stream. It may or may not succeed.
     *
     * @param targetStream     The destination stream to attach.
     *
     * @return                 A timestamp indicating where the attachment begins.
     */
    public Timestamp pullStream(UUID targetStream)
    throws RemoteException, OutOfSpaceException, IOException
    {
        return pullStream(targetStream, -1);
    }

    /**
     * Temporarily pull a remote stream into this stream. This function tries to
     * attach a remote stream to this stream. It may or may not succeed.
     *
     * @param targetStream     The destination stream to attach.
     * @param duration         The length of time, in log entries that this pull should last,
     *                         if -1, then the pull is permanent.
     *
     * @return                 A timestamp indicating where the attachment begins.
     */
    public Timestamp pullStream(UUID targetStream, int duration)
    throws RemoteException, OutOfSpaceException, IOException
    {
        List<UUID> streams = new ArrayList<UUID>();
        streams.add(targetStream);
        return pullStream(streams, duration);
    }

    /**
     * Temporarily pull multiple remote streams into this stream. This function tries to
     * attach multiple remote stream to this stream. It may or may not succeed.
     *
     * @param targetStreams    The destination streams to attach.
     * @param duration         The length of time, in log entries that this pull should last,
     *                         if -1, then the pull is permanent.
     *
     * @return                 A timestamp indicating where the attachment begins.
     */
    public Timestamp pullStream(List<UUID> targetStreams, int duration)
    throws RemoteException, OutOfSpaceException, IOException
    {
        return pullStream(targetStreams, null, duration);
    }

    /**
     * Temporarily pull multiple remote streams into this stream, including a serializable payload in the
     * remote move operation. This function tries to attach multiple remote stream to this stream.
     * It may or may not succeed.
     *
     * @param targetStreams    The destination streams to attach.
     * @param payload          The serializable payload to insert
     * @param duration         The length of time, in log entries that this pull should last,
     *                         if -1, then the pull is permanent.
     *
     * @return                 A timestamp indicating where the attachment begins.
     */
    public Timestamp pullStream(List<UUID> targetStreams, Serializable payload, int duration)
    throws RemoteException, OutOfSpaceException, IOException
    {
        try (ByteArrayOutputStream bs = new ByteArrayOutputStream())
        {
            try (ObjectOutput out = new ObjectOutputStream(bs))
            {
                out.writeObject(payload);
                return pullStream(targetStreams, bs.toByteArray(), duration);
            }
        }
    }

    /**
     * Temporarily pull multiple remote streams into this stream, including a payload in the
     * remote move operation. This function tries to attach multiple remote stream to this stream.
     * It may or may not succeed.
     *
     * @param targetStreams    The destination streams to attach.
     * @param payload          The payload to insert
     * @param duration         The length of time, in log entries that this pull should last,
     *                         if -1, then the pull is permanent.
     *
     * @return                 A timestamp indicating where the attachment begins.
     */
    public Timestamp pullStream(List<UUID> targetStreams, byte[] payload, int duration)
    throws RemoteException, OutOfSpaceException, IOException
    {
        return pullStream(targetStreams, payload, 0, duration);
    }

    /**
     * Temporarily pull multiple remote streams into this stream, including a serializable payload in the
     * remote move operation, and optionally reserve extra entries.
     * This function tries to attach multiple remote stream to this stream.
     * It may or may not succeed.
     *
     * @param targetStreams    The destination streams to attach.
     * @param payload          The serializable payload to insert
     * @param reservation      The number of entires to reserve, both in the local and global log.
     * @param duration         The length of time, in log entries that this pull should last,
     *                         if -1, then the pull is permanent.
     *
     * @return                 A timestamp indicating where the attachment begins.
     */
    public Timestamp pullStream(List<UUID> targetStreams, Serializable payload, int reservation, int duration)
    throws RemoteException, OutOfSpaceException, IOException
    {
        try (ByteArrayOutputStream bs = new ByteArrayOutputStream())
        {
            try (ObjectOutput out = new ObjectOutputStream(bs))
            {
                out.writeObject(payload);
                return pullStream(targetStreams, bs.toByteArray(), reservation, duration);
            }
        }
    }

    /**
     * Temporarily pull multiple remote streams into this stream, including a payload in the
     * remote move operation, and optionally reserve extra entries.
     * This function tries to attach multiple remote stream to this stream.
     * It may or may not succeed.
     *
     * @param targetStreams    The destination streams to attach.
     * @param payload          The payload to insert
     * @param reservation      The number of entries to reserve, both in the local and the remote log.
     * @param duration         The length of time, in log entries that this pull should last,
     *                         if -1, then the pull is permanent.
     *
     * @return                 A timestamp indicating where the attachment begins.
     */
    public Timestamp pullStream(List<UUID> targetStreams, byte[] payload, int reservation, int duration)
    throws RemoteException, OutOfSpaceException, IOException
    {
        HashMap<UUID, StreamData> datamap = new HashMap<UUID, StreamData>();
        for (UUID id : targetStreams)
        {
            // Get information about remote stream
            StreamData sd = getConfigMaster.get().getStream(id);
            if (sd == null) { throw new RemoteException("Unable to find target stream " + id.toString(), logID); }
            datamap.put(id, sd);
        }

        // Write a stream start in the local log.
        // This entry needs to contain the epoch+1 of the target stream as well as our own epoch
        Map<UUID, Long> epochMap = new HashMap<UUID, Long>();
        epochMap.put(streamID, getCurrentEpoch());
        for (UUID id : targetStreams)
        {
            StreamData sd = datamap.get(id);
            epochMap.put(id, duration == -1 ? sd.epoch + 1 : sd.epoch);
        }
        CorfuDBStreamStartEntry cdbsme = new CorfuDBStreamStartEntry(epochMap, targetStreams, payload);
        long token = sequencer.getNext(streamIDstack.peekLast(), reservation + 1);
        woas.write(token, (Serializable) cdbsme);

        for (UUID id : targetStreams)
        {
            StreamData sd = datamap.get(id);

            //remote, use a gossip message
            if (sd.currentLog == logID)
            {
                StreamPullGossip spg = new StreamPullGossip(id, logID, streamID, token, getCurrentEpoch(), reservation+1, duration, payload);
                ((IConfigMaster)cdbc.getView(sd.currentLog).getConfigMasters().get(0)).sendGossip(spg);
            }
            else {
                StreamingSequencer sremote = new StreamingSequencer(cdbc, sd.currentLog);
                long remoteToken = sremote.getNext(id, reservation + 1);
                // Write a move in the remote log
                IWriteOnceAddressSpace woasremote = getAddressSpace.apply(cdbc, sd.currentLog);
                woasremote.write(remoteToken, new CorfuDBStreamMoveEntry(id, logID, streamID, token, duration, sd.epoch, getCurrentEpoch(), payload));
            }
       }

        Timestamp ts = new Timestamp(epochMap, null, token, streamID);
        ts.setPhysicalPos(token);
        ts.setContainingStream(streamID);
        return ts;
    }

    public Timestamp pullStreamAsBundle(List<UUID> targetStreams, byte[] payload, int slots)
    throws RemoteException, OutOfSpaceException, IOException
    {
        return pullStreamAsBundle(targetStreams, (Serializable) payload, slots);
    }

    /**
     * Temporarily pull multiple remote streams into this stream, including a payload in the
     * remote move operation, and optionally reserve extra entries, using a BundleEntry.
     * This function tries to attach multiple remote stream to this stream.
     * It may or may not succeed.
     *
     * @param targetStreams    The destination streams to attach.
     * @param payload          The payload to insert
     * @param slots            The length of time, in slots that this pull should last.
     *
     * @return                 A timestamp indicating where the attachment begins.
     */
    public Timestamp pullStreamAsBundle(List<UUID> targetStreams, Serializable payload, int slots)
    throws RemoteException, OutOfSpaceException, IOException
    {
        final long token = sequencer.getNext(streamIDstack.peekLast(),  slots + 1);
        Timestamp ts = new Timestamp(epochMap, null, token, streamID);
        ts.setPhysicalPos(token);
        ts.setContainingStream(streamID);

        CompletableFuture<Void> cf = CompletableFuture.runAsync(() -> {
        try
        {
        HashMap<UUID, StreamData> datamap = new HashMap<UUID, StreamData>();
        for (UUID id : targetStreams)
        {
            // Get information about remote stream
            StreamData sd = remoteStreamMap.get(id);
            if (sd == null)
            {
                sd = getConfigMaster.get().getStream(id);
                remoteStreamMap.put(id, sd);
            }
            if (sd == null) { throw new RemoteException("Unable to find target stream " + id.toString(), logID); }
            datamap.put(id, sd);
        }

        // Write a stream start in the local log.
        // This entry needs to contain the epoch+1 of the target stream as well as our own epoch
        Map<UUID, Long> epochMap = new HashMap<UUID, Long>();
        epochMap.put(streamID, getCurrentEpoch());
        for (UUID id : targetStreams)
        {
            StreamData sd = datamap.get(id);
            epochMap.put(id, sd.epoch);
        }
        CorfuDBStreamStartEntry cdbsme = new CorfuDBStreamStartEntry(epochMap, targetStreams, payload);
                woas.write(token, (Serializable) cdbsme);

        int offset = 1;
        for (UUID id : targetStreams)
        {
            // Get a sequence in the remote stream
            /*
            log.debug("targetSequence");
            StreamData sd = datamap.get(id);
            StreamingSequencer sremote = new StreamingSequencer(cdbc, sd.currentLog);
            long remoteToken = sremote.getNext(id, slots + 1);
            log.debug("remoteLog write");
            // Write a move in the remote log
            IWriteOnceAddressSpace woasremote = getAddressSpace.apply(cdbc, sd.currentLog);
            woasremote.write(remoteToken, new BundleEntry(epochMap, logID, streamID, token, sd.epoch, payload, slots, token + offset));
           log.debug("hmm.");
            offset++;
*/

            StreamData sd = datamap.get(id);
            //remote, use a gossip message
            if (sd.currentLog != logID)
            {
                StreamBundleGossip spg = new StreamBundleGossip(id, epochMap, logID, streamID, token, getCurrentEpoch(), slots+1, slots, payload, token+offset);
                ((IConfigMaster)cdbc.getView(sd.currentLog).getConfigMasters().get(0)).sendGossip(spg);
            }
            else {
                StreamingSequencer sremote = new StreamingSequencer(cdbc, sd.currentLog);
                long remoteToken = sremote.getNext(id, slots + 1);
                // Write a move in the remote log
                IWriteOnceAddressSpace woasremote = getAddressSpace.apply(cdbc, sd.currentLog);
                woasremote.write(remoteToken, new BundleEntry(epochMap, logID, streamID, token, sd.epoch, payload, slots, token + offset));
            }

        }
        } catch (Exception ex) {
            log.debug("Exception", ex);
        }
        }, executor);

        return ts;
    }

    /**
     * Close the stream. This method must be called to free resources.
     */
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
