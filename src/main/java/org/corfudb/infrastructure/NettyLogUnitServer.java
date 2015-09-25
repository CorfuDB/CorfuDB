package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.collect.Range;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import lombok.*;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.wireprotocol.*;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalAndSentinelRetry;

import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@NoArgsConstructor
public class NettyLogUnitServer extends AbstractNettyServer {

    ConcurrentHashMap<java.util.UUID, Long> trimMap;
    Thread gcThread;
    IntervalAndSentinelRetry gcRetry;

    @Data
    @RequiredArgsConstructor
    public class LogUnitEntry implements IMetadata {
        final ByteBuf buffer;
        final EnumMap<LogUnitMetadataType, Object> metadataMap;
        final boolean isHole;

        /** Generate a new log unit entry which is a hole */
        public LogUnitEntry()
        {
            buffer = null;
            metadataMap = new EnumMap<>(LogUnitMetadataType.class);
            isHole = true;
        }
    }

    @RequiredArgsConstructor
    public enum LogUnitMetadataType {
        STREAM(0),
        RANK(1),
        STREAM_ADDRESS(2)
        ;

        final int type;

        public byte asByte() { return (byte)type; }
    }

    public static Map<Byte, LogUnitMetadataType> metadataTypeMap =
            Arrays.<LogUnitMetadataType>stream(LogUnitMetadataType.values())
                    .collect(Collectors.toMap(LogUnitMetadataType::asByte, Function.identity()));

    @RequiredArgsConstructor
    public enum ReadResultType {
        EMPTY(0),
        DATA(1),
        FILLED_HOLE(2),
        TRIMMED(3)
        ;

        final int type;

        public byte asByte() { return (byte)type; }
    }

    public static Map<Byte, ReadResultType> readResultTypeMap =
            Arrays.<ReadResultType>stream(ReadResultType.values())
                    .collect(Collectors.toMap(ReadResultType::asByte, Function.identity()));


    /**
     * This cache services requests for data at various addresses. In a memory implementation,
     * it is not backed by anything, but in a disk implementation it is backed by persistent storage.
     */
    LoadingCache<Long, LogUnitEntry> dataCache;

    /**
     * This cache services requests for hints.
     */
    //Cache<Long, Set<NewLogUnitHints>> hintCache;

    /**
     * The contiguous head of the log (that is, the lowest address which has NOT been trimmed yet).
     */
    @Getter
    long contiguousHead;

    /**
     * A range set representing trimmed addresses on the log unit.
     */
    RangeSet<Long> trimRange;

    @Override
    public void close() {
        if (gcThread != null)
        {
            gcThread.interrupt();
        }
        /** Free all references */
        dataCache.asMap().values().parallelStream()
                .map(m -> m.buffer.release());
        super.close();
    }

    @Override
    void parseConfiguration(Map<String, Object> configuration)
    {
        serverName = "NettyLogUnitServer";
        reset();
        gcThread = new Thread(this::runGC);
        gcThread.start();
    }

    /**
     * Process an incoming message
     *
     * @param msg The message to process.
     * @param ctx The channel context from the handler adapter.
     */
    @Override
    @SuppressWarnings("unchecked")
    void processMessage(NettyCorfuMsg msg, ChannelHandlerContext ctx) {
        switch(msg.getMsgType())
        {
            case WRITE:
                write((NettyLogUnitWriteMsg) msg, ctx);
            break;
            case READ_REQUEST:
                read((NettyLogUnitReadRequestMsg) msg, ctx);
            break;
            case GC_INTERVAL:
            {
                NettyLogUnitGCIntervalMsg m = (NettyLogUnitGCIntervalMsg) msg;
                gcRetry.setRetryInterval(m.getInterval());
            }
            break;
            case FORCE_GC:
            {
                gcThread.interrupt();
            }
            break;
            case FILL_HOLE:
            {
                NettyLogUnitFillHoleMsg m = (NettyLogUnitFillHoleMsg) msg;
                dataCache.get(m.getAddress(), (address) -> new LogUnitEntry());
            }
            break;
            case TRIM:
            {
                NettyLogUnitTrimMsg m = (NettyLogUnitTrimMsg) msg;
                trimMap.compute(m.getStreamID(), (key, prev) ->
                        prev == null ? m.getPrefix() : Math.max(prev, m.getPrefix()));
            }
            break;
        }
    }

    /**
     * Reset the state of the server.
     */
    @Override
    public void reset() {
        contiguousHead = 0L;
        trimRange = TreeRangeSet.create();

        if (dataCache != null)
        {
            /** Free all references */
            dataCache.asMap().values().parallelStream()
                    .map(m -> m.buffer.release());
        }
        // Currently, only an in-memory configuration is supported.
        dataCache = Caffeine.newBuilder()
                .build(a -> null);

        // Hints are always in memory and never persisted.
        /*
        hintCache = Caffeine.newBuilder()
                .weakKeys()
                .build();
*/
        // Trim map is set to empty on start
        // TODO: persist trim map - this is optional since trim is just a hint.
        trimMap = new ConcurrentHashMap<>();
    }

    /** Service an incoming read request. */
    public void read(NettyLogUnitReadRequestMsg msg, ChannelHandlerContext ctx)
    {
        if (trimRange.contains (msg.getAddress()))
        {
            sendResponse(new NettyLogUnitReadResponseMsg(ReadResultType.TRIMMED), msg, ctx);
        }
        else
        {
            LogUnitEntry e = dataCache.get(msg.getAddress());
            if (e == null)
            {
                sendResponse(new NettyLogUnitReadResponseMsg(ReadResultType.EMPTY), msg, ctx);
            }
            else if (e.isHole)
            {
                sendResponse(new NettyLogUnitReadResponseMsg(ReadResultType.FILLED_HOLE), msg, ctx);
            } else {
                sendResponse(new NettyLogUnitReadResponseMsg(e), msg, ctx);
            }
        }
    }

    /** Service an incoming write request. */
    public void write(NettyLogUnitWriteMsg msg, ChannelHandlerContext ctx)
    {
        //TODO: locking of trimRange.
        if (trimRange.contains (msg.getAddress()))
        {
            sendResponse(new NettyCorfuMsg(NettyCorfuMsg.NettyCorfuMsgType.ERROR_TRIMMED), msg, ctx);
        }
        else {
            LogUnitEntry e = new LogUnitEntry(msg.getData(), msg.getMetadataMap(), false);
            e.getBuffer().retain();
            if (e == dataCache.get(msg.getAddress(), (address) -> e)) {
                sendResponse(new NettyCorfuMsg(NettyCorfuMsg.NettyCorfuMsgType.ERROR_OK), msg, ctx);
            }
            else
            {
                e.getBuffer().release();
                sendResponse(new NettyCorfuMsg(NettyCorfuMsg.NettyCorfuMsgType.ERROR_OVERWRITE), msg, ctx);
            }
        }
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

    @SuppressWarnings("unchecked")
    public boolean handleGC()
    {
        log.info("Garbage collector starting...");
        long freedEntries = 0;

        /* Pick a non-compacted region or just scan the cache */
        Map<Long, LogUnitEntry> map = dataCache.asMap();
        SortedSet<Long> addresses = new TreeSet<>(map.keySet());
        for (long address : addresses)
        {
            LogUnitEntry buffer = dataCache.getIfPresent(address);
            if (buffer != null)
            {
                Set<UUID> streams = buffer.getStreams();
                // this is a normal entry
                if (streams.size() > 0) {
                    boolean trimmable = true;
                    for (java.util.UUID stream : streams)
                    {
                        Long trimMark = trimMap.getOrDefault(stream, null);
                        // if the stream has not been trimmed, or has not been trimmed to this point
                        if (trimMark == null || address > trimMark) {
                            trimmable = false;
                            break;
                        }
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

    public void trimEntry(long address, Set<java.util.UUID> streams, LogUnitEntry entry)
    {
        // Add this entry to the trimmed range map.
        trimRange.add(Range.closed(address, address));
        // Invalidate this entry from the cache. This will cause the CacheLoader to free the entry from the disk
        // assuming the entry is back by disk
        dataCache.invalidate(address);
        //and free any references the buffer might have
        if (entry.getBuffer() != null)
        {
            entry.getBuffer().release();
        }
    }




}
