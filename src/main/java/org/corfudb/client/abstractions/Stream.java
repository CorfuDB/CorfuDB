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

import java.util.function.Supplier;
import org.corfudb.client.StreamData;

/**
 *  A hop-aware stream implementation. The stream must be closed after the application is done using it
 *  to free resources, or enclosed in a try-resource block.
 */
public class Stream implements AutoCloseable {

    private final Logger log = LoggerFactory.getLogger(Stream.class);

    CorfuDBClient cdbc;
    UUID streamID;
    UUID logID;

    StreamingSequencer sequencer;
    WriteOnceAddressSpace woas;

    ExecutorService executor;
    boolean prefetch = true;


    PriorityBlockingQueue<CorfuDBEntry> logQ;
    AtomicLong dispatchedReads;
    AtomicLong logPointer;
    AtomicLong currentEpoch;

    Timestamp latest = null;

    boolean closed = false;
    boolean killExecutor = false;
    PriorityBlockingQueue<CorfuDBStreamEntry> streamQ;
    CompletableFuture<Void> currentDispatch;
    AtomicLong streamPointer;
    int queueMax;

    Supplier<CorfuDBView> getView;
    Supplier<IConfigMaster> getConfigMaster;

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
        woas = new WriteOnceAddressSpace(cdbc);
        logQ = new PriorityBlockingQueue<CorfuDBEntry>();
        streamQ = new PriorityBlockingQueue<CorfuDBStreamEntry>();
        dispatchedReads = new AtomicLong();
        streamPointer = new AtomicLong();
        logPointer = new AtomicLong();
        currentEpoch = new AtomicLong();
        queueMax = queueSize;
        getView = () -> {
            return this.cdbc.getView();
        };
        getConfigMaster = () -> {
            return (IConfigMaster) this.getView.get().getConfigMasters().get(0);
        };
        this.prefetch = prefetch;
        this.executor = executor;
        //now, the stream starts reading from the beginning...
        StreamData sd = getConfigMaster.get().getStream(streamID);
        //it doesn't, so try to create
        while (sd == null)
        {
            try {
                long sequenceNo = sequencer.getNext(uuid);
                CorfuDBStreamStartEntry cdsse = new CorfuDBStreamStartEntry(streamID, currentEpoch.get());
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
            //log.debug("dispatch " + streamID.toString() + " " + logPos);
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
                                    // This move is just an allocation boundary change. The epoch doesn't change,
                                    // just the read pointer.
                                    if (cdbsme.destinationLog.equals(logID) || cdbsme.getTimestamp().getEpoch(streamID) == -1)
                                    {
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
                                            log.debug("Detected permanent move operation to different log " + cdbsme.destinationLog);
                                            //since this is a perma-move, we increment the epoch
                                            long newEpoch = currentEpoch.incrementAndGet();
                                            long logpos = streamPointer.getAndIncrement();
                                            getConfigMaster.get().sendGossip(new StreamEpochGossipEntry(streamID, cdbsme.destinationLog, newEpoch, logpos));
                                            //since this is on a different log, we change the address space and sequencer
                                            woas = new WriteOnceAddressSpace(cdbc, cdbsme.destinationLog);
                                            sequencer = new StreamingSequencer(cdbc, cdbsme.destinationLog);
                                            dispatchedReads.set(cdbsme.destinationPos);
                                            currentDispatch = getStreamTailAndDispatch(1);
                                            logID = cdbsme.destinationLog;
                                            synchronized(currentEpoch)
                                            {
                                                currentEpoch.notifyAll();
                                            }
                                            return;
                                        }
                                        else
                                        {
                                            //This is a temporary move, so expose it to the client without changing current
                                            //reads.

                                        }
                                    }
                                }
                                else if (payload instanceof CorfuDBStreamStartEntry)
                                {

                                }
                                else if (payload instanceof CorfuDBStreamEntry)
                                {
                                    CorfuDBStreamEntry cdbse = (CorfuDBStreamEntry) payload;
                                    if (cdbse.containsStream(streamID) && cdbse.getTimestamp().getEpoch(streamID) == (currentEpoch.get()))
                                    {
                                        cdbse.getTimestamp().setLogicalPos(r.pos);
                                        synchronized (streamPointer) {
                                            latest = cdbse.getTimestamp();
                                            streamPointer.notifyAll();
                                        }
                                        numReadable++;
                                        streamQ.offer(cdbse);
                                    }
                                    else
                                    {
                                        log.warn("Ignored log entry from wrong epoch (expected {}, got {})", currentEpoch.get(), cdbse.getTimestamp().getEpoch(streamID));
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
                long token = sequencer.getNext(streamID);
                long currentepoch = currentEpoch.get();
                CorfuDBStreamEntry cdse = new CorfuDBStreamEntry(streamID, data, currentepoch);
                woas.write(token, (Serializable) cdse);
                return new Timestamp(streamID, currentepoch, -1, token);
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

    /**
     * Read the next entry in the stream as a byte array. This convenience function
     * retireves the next entry in the stream, blocking if necessary.
     *
     * @return      A byte array containing the payload of the next entry in the stream.
     */
    public byte[] readNext()
    throws IOException, InterruptedException
    {
        return readNextEntry().getPayload();
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
        return readNextEntry().deserializePayload();
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
        return new Timestamp(streamID, currentEpoch.get(), streamPointer.get(), sequencer.getCurrent(streamID));
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
                    if (latest.compareTo(pos) >= 0)
                    {
                        return true;
                    }
                    else if (latest.getEpoch(streamID) != pos.getEpoch(streamID))
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
        long last = currentEpoch.get();
        synchronized(currentEpoch)
        {
            if (timeout < 0)
            {
                currentEpoch.wait();
            }
            else
            {
                currentEpoch.wait(timeout);
            }
            if (currentEpoch.get() == last) { return false; }
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
        long remoteToken = sremote.getNext(streamID);
        // Write a start in the remote log
        WriteOnceAddressSpace woasremote = new WriteOnceAddressSpace(cdbc, destinationLog);
        woasremote.write(remoteToken, new CorfuDBStreamStartEntry(streamID, currentEpoch.get() + 1));
        // Write the move request into the local log
        CorfuDBStreamMoveEntry cdbsme = new CorfuDBStreamMoveEntry(streamID, destinationLog, null, remoteToken, -1, currentEpoch.get(), -1);
        long token = sequencer.getNext(streamID);
        woas.write(token, (Serializable) cdbsme);
    }

    /**
     * Permanently hop to another stream. This function tries to hop this stream to
     * another stream by obtaining a position in the destination stream and inserting
     * a move entry from the source log to the destination log. It may or may not
     * be successful.
     *
     * @param destinationstream    The destination stream to hop to.
     */
    public void hopStream(UUID destinationStream)
    throws RemoteException, OutOfSpaceException, IOException
    {
        // Get the current location and epoch of the destination stream (through gossip)
        StreamData sd = getConfigMaster.get().getStream(destinationStream);
        if (sd == null) { throw new RemoteException("Unable to find destination stream " + destinationStream.toString(), logID); }

        // Get a sequence in the remote stream
        StreamingSequencer sremote = new StreamingSequencer(cdbc, sd.currentLog);
        long remoteToken = sremote.getNext(streamID);
        // Write a start in the remote log
        WriteOnceAddressSpace woasremote = new WriteOnceAddressSpace(cdbc, sd.currentLog);
        woasremote.write(remoteToken, new CorfuDBStreamStartEntry(streamID, sd.epoch));
        // Write the move request into the local log
        CorfuDBStreamMoveEntry cdbsme = new CorfuDBStreamMoveEntry(streamID, sd.currentLog, null, remoteToken, -1, currentEpoch.get(), -1);
        long token = sequencer.getNext(streamID);
        woas.write(token, (Serializable) cdbsme);
    }


    /**
     * Permanently attach a remote stream into this stream. This function tries to
     * attach a remote stream to this stream. In order to complete the attachment,
     * you need to call moveStream on the remote stream with this timestamp, but
     * only once you know that this attachment is successful. Otherwise,
     * the remote stream could be lost.
     *
     * @param attachStream     The destination stream to attach.
     */
    public void attachStream(UUID destinationStream)
    throws RemoteException, OutOfSpaceException, IOException
    {
        // Insert a start stream entry.

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
