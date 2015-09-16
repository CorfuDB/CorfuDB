package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import com.google.common.collect.Range;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import lombok.val;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.*;

import org.corfudb.infrastructure.thrift.*;
import org.corfudb.infrastructure.thrift.UUID;
import org.corfudb.util.Utils;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalAndSentinelRetry;

import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.lang.ref.SoftReference;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@NoArgsConstructor
public class NewLogUnitServer implements ICorfuDBServer, NewLogUnitService.AsyncIface {

    TServer server;
    AtomicBoolean running;
    @Getter
    Thread thread;
    ConcurrentHashMap<java.util.UUID, Long> trimMap;
    Thread gcThread;
    IntervalAndSentinelRetry gcRetry;

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        running.set(true);
        while (running.get()) {
            TNonblockingServerTransport serverTransport;
            NewLogUnitService.AsyncProcessor<NewLogUnitServer> processor;
            log.debug("New log unit entering service loop.");
            try {
                serverTransport = new TNonblockingServerSocket(port);
                processor =
                        new NewLogUnitService.AsyncProcessor<NewLogUnitServer>(this);
                server = new TThreadedSelectorServer(new TThreadedSelectorServer.Args(serverTransport)
                        .processor(processor)
                        .protocolFactory(TCompactProtocol::new)
                        .inputTransportFactory(new TFastFramedTransport.Factory())
                        .outputTransportFactory(new TFastFramedTransport.Factory())
                );
                log.info("New log unit starting on port " + port);
                server.serve();
            } catch (TTransportException e) {
                log.warn("New log unit encountered exception, restarting.", e);
            }
        }
        log.info("New log unit server shutting down.");
    }

    @Data
    public class NewLogUnitHints {
            HintType type;
            byte[] hintData;
    }

    /**
     * The port that this instance is being served on.
     *
     * @return The current port that this instance is being serviced on.
     */
    @Getter
    Integer port;

    /**
     * The current epoch of this log unit.
     *
     * @return The current epoch this instance is in.
     */
    @Getter
    long currentEpoch;

    /**
     * This cache services requests for data at various addresses. In a memory implementation,
     * it is not backed by anything, but in a disk implementation it is backed by persistent storage.
     */
    Cache<Long, ByteBuffer> dataCache;

    /**
     * This cache services requests for hints.
     */
    Cache<Long, Set<NewLogUnitHints>> hintCache;

    /**
     * The contiguous head of the log (that is, the lowest address which has NOT been trimmed yet).
     */
    @Getter
    long contiguousHead;

    /**
     * A range set representing trimmed addresses on the log unit.
     */
    RangeSet<Long> trimRange;

    /**
     * Returns an instance (main thread) of this server.
     * @param configuration     The configuration from the parsed Yaml file.
     * @return                  A runnable representing the main thread of this log unit.
     */
    @Override
    public ICorfuDBServer getInstance(Map<String, Object> configuration) {
        parseConfiguration(configuration);
        thread = new Thread(this);
        return this;
    }

    @Override
    public void close() {
        if (server != null)
        {
            running.set(false);
            server.stop();
        }
        if (gcThread != null)
        {
            gcThread.interrupt();
        }
    }

    /**
     * Parses the configuration for this server.
     * @param configuration    The configuration from the parsed Yaml file.
     * @throws RuntimeException     If the provided configuration is invalid.
     */
    void parseConfiguration(Map<String, Object> configuration) {
        if (configuration.get("port") == null)
        {
            log.error("Required key port is missing from configuration!");
            throw new RuntimeException("Invalid configuration provided!");
        }

        port = ((Integer)configuration.get("port"));
        contiguousHead = 0L;
        trimRange = TreeRangeSet.create();

        // Currently, only an in-memory configuration is supported.
        dataCache = Caffeine.newBuilder()
                .weakKeys()
                .build();

        // Hints are always in memory and never persisted.
        hintCache = Caffeine.newBuilder()
                .weakKeys()
                .build();

        // Trim map is set to empty on start
        // TODO: persist trim map - this is optional since trim is just a hint.
        trimMap = new ConcurrentHashMap<>();
        gcThread = new Thread(this::runGC);
        gcThread.start();
    }

    /**
     * Validate basic server parameters: namely, what epoch we are in and if the unit is in simulated failure mode.
     * If the validation fails, we throw a TException which will be passed on to the client.
     * @param epoch         What epoch the client thinks we should be in.
     * @throws TException   If the validation fails.
     */
    public void validate(long epoch)
            throws TException
    {
        if (epoch != currentEpoch)
        {
            throw new TException("Request from wrong epoch, got " + epoch + " expected " + currentEpoch);
        }
    }

    /**
     * Perform an asynchronous write. When AsyncMethodCallback is invoked, the write is guaranteed to be persisted
     * according to the persistence requirements in the configuration file.
     * @param epoch     What epoch the client thinks we should be in.
     * @param offset    The global offset (address) we are reading at.
     * @param stream    Which streams this log entry belongs to.
     * @param payload   The payload to store at the address.
     * @param resultHandler An asynchronous method callback which will be invoked when write is completed.
     * @throws TException   If there was an IO exception during the handling of this call.
     */
    @Override
    @SuppressWarnings("unchecked")
    public void write(long epoch, long offset, Set<UUID> stream, ByteBuffer payload, AsyncMethodCallback resultHandler) throws TException {
        validate(epoch);

        ByteBuffer dataBuffer;
        // The size of the metadata is equal to the number of stream UUIDs plus 2 16bit int.
        int metadataSize = (stream.size() * 16) + 4;
        short dataType = 0;
        // in-memory mode just uses a bytebuffer which is not memory mapped.
        dataBuffer = ByteBuffer.allocateDirect(payload.remaining() + metadataSize);

        //dump the metadata in the buffer.
        dataBuffer.putShort((short) ReadCode.READ_DATA.getValue());
        dataBuffer.putShort((short)stream.size());
        for (UUID streamid : stream)
        {
            dataBuffer.putLong(streamid.getMsb());
            dataBuffer.putLong(streamid.getLsb());
        }

        //dump the payload in the buffer.
        dataBuffer.put(payload);
        dataBuffer.flip();

        //put the buffer in the cache, if in memory (if it's not in-memory, we can rely on the cache retrieval).
        ByteBuffer newVal = dataCache.get(offset, (address) -> dataBuffer);

        if (newVal != dataBuffer)
        {
            resultHandler.onComplete(new WriteResult().setCode(ErrorCode.ERR_OVERWRITE).setData(newVal));
            return;
        }
        resultHandler.onComplete(new WriteResult().setCode(ErrorCode.OK));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void read(long epoch, long offset, AsyncMethodCallback resultHandler) throws TException {
        validate(epoch);
        //fetch the address from the cache.
        ByteBuffer data = dataCache.getIfPresent(offset);
        if (data == null) {
            //is this in the trim range?
            if (trimRange.contains(offset)) {
                resultHandler.onComplete(new ReadResult(ReadCode.READ_TRIMMED, Collections.emptySet(), ByteBuffer.allocate(0), Collections.emptySet()));
            } else {
                resultHandler.onComplete(new ReadResult(ReadCode.READ_EMPTY, Collections.emptySet(), ByteBuffer.allocate(0), Collections.emptySet()));
            }
        }
        else
        {
            data = data.duplicate();
            //rewind the buffer
            data.clear();
            //decode the metadata
            short dataType = data.getShort();
            Set<UUID> cset = new HashSet();
            if (dataType == (short)ReadCode.READ_DATA.getValue()) {
                short numStreams = data.getShort();
                for (short i = 0; i < numStreams; i++) {
                    UUID streamId = new UUID();
                    streamId.setMsb(data.getLong());
                    streamId.setLsb(data.getLong());
                    cset.add(streamId);
                }
                resultHandler.onComplete(new ReadResult(ReadCode.findByValue(dataType), cset, data.slice(), Collections.emptySet()));
            }
            //write the buffer
            resultHandler.onComplete(new ReadResult(ReadCode.findByValue(dataType), cset, null, Collections.emptySet()));
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void trim(long epoch, UUID stream, long prefix, AsyncMethodCallback resultHandler) throws TException {
        log.info("trim requested!");
        validate(epoch);
        trimMap.compute(Utils.fromThriftUUID(stream), (key, prev) -> prev == null ? prefix : Math.max(prev, prefix));
        resultHandler.onComplete(null);
    }

    public void runGC()
    {
        Thread.currentThread().setName("LogUnit-GC");
        val retry = IRetry.build(IntervalAndSentinelRetry.class, this::handleGC)
                .setOptions(x -> x.setSentinelReference(running))
                .setOptions(x -> x.setRetryInterval(60_000));

        gcRetry = (IntervalAndSentinelRetry) retry;

        retry.runForever();
    }


    public ReadCode getDataType(ByteBuffer data)
    {
        data.clear();
        return ReadCode.findByValue(data.getShort());
    }
    public Set<java.util.UUID> streamsInEntry(ByteBuffer data)
    {
        data.clear();
        //decode the metadata
        short dataType = data.getShort();
        Set<java.util.UUID> cset = new HashSet();
        if (dataType == (short)ReadCode.READ_DATA.getValue()) {
            short numStreams = data.getShort();
            for (short i = 0; i < numStreams; i++) {
                cset.add(new java.util.UUID(data.getLong(), data.getLong()));
            }
        }
        return cset;
    }

    public boolean handleGC()
    {
        log.info("Garbage collector starting...");
        long freedEntries = 0;

        /* Pick a non-compacted region or just scan the cache */
        Map<Long, ByteBuffer> map = dataCache.asMap();
        SortedSet<Long> addresses = new TreeSet<>(map.keySet());
        for (long address : addresses)
        {
            ByteBuffer buffer = dataCache.getIfPresent(address);
            if (buffer != null)
            {
                Set<java.util.UUID> streams = streamsInEntry(buffer);
                // this is a normal entry
                if (streams.size() > 0) {
                    boolean trimmable = true;
                    for (java.util.UUID stream : streams)
                    {
                        Long trimMark = trimMap.getOrDefault(stream, null);
                        // if the stream has not been trimmed,
                        if (trimMark == null) {trimmable = false;}
                        // or has not been trimmed to this point
                        else if (address > trimMark) { trimmable = false;}
                        // it is not trimmable.
                    }
                    if (trimmable) {
                        trimEntry(address, streams, buffer);
                        freedEntries++;
                    }
                }
                else {
                    //this is an entry which belongs in all streams
                    //find the minimum contiguous range - and see if this entry is after it.
                    Range<Long> minRange = (Range<Long>) trimRange.complement().asRanges().toArray()[0];
                    if (minRange.contains(address))
                    {
                        trimEntry(address, streams, buffer);
                        freedEntries++;
                    }
                }
            }
        }

        log.info("Garbage collection pass complete. Freed {} entries", freedEntries);
        return true;
    }

    public void trimEntry(long address, Set<java.util.UUID> streams, ByteBuffer entry)
    {
        log.info("Trim requested.");
        // Add this entry to the trimmed range map.
        trimRange.add(Range.closed(address, address));
        // Invalidate this entry from the cache. This will cause the CacheLoader to free the entry from the disk
        // assuming the entry is back by disk
        dataCache.invalidate(address);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void fillHole(long offset, AsyncMethodCallback resultHandler) throws TException {
        //put the buffer in the cache, if in memory (if it's not in-memory, we can rely on the cache retrieval).
        ByteBuffer newVal = ByteBuffer.allocateDirect(2);
        newVal.putShort((short) ReadCode.READ_FILLEDHOLE.getValue());
        dataCache.get(offset, (address) -> newVal);
        resultHandler.onComplete(null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void forceGC(AsyncMethodCallback resultHandler) throws TException {
        log.info("Garbage collection forced.");
        gcThread.interrupt();
        resultHandler.onComplete(null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setGCInterval(long millis, AsyncMethodCallback resultHandler) throws TException {
        log.info("Setting GC retry interval to {}", millis);
        gcRetry.setRetryInterval(millis);
        resultHandler.onComplete(null);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void ping(AsyncMethodCallback resultHandler) throws TException {
        resultHandler.onComplete(true);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void reset(AsyncMethodCallback resultHandler) throws TException {
        log.info("Reset requested!");
        currentEpoch = 0;
        dataCache.invalidateAll();
        resultHandler.onComplete(null);
    }
}
