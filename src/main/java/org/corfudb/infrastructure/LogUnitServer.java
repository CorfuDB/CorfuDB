package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.*;
import org.corfudb.util.Utils;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalAndSentinelRetry;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg.ReadResultType;
import org.corfudb.protocols.wireprotocol.LogUnitReadResponseMsg.LogUnitEntry;
/**
 * Created by mwei on 12/10/15.
 */
@Slf4j
public class LogUnitServer implements IServer {

    /** The options map. */
    Map<String,Object> opts;

    /** The file channel. */
    FileChannel fc;

    /** The file pointer. */
    AtomicLong filePointer;

    /** The garbage collection thread. */
    Thread gcThread;

    /**
     * The contiguous head of the log (that is, the lowest address which has NOT been trimmed yet).
     */
    @Getter
    long contiguousHead;

    /**
     * A range set representing trimmed addresses on the log unit.
     */
    RangeSet<Long> trimRange;

    ConcurrentHashMap<UUID, Long> trimMap;

    IntervalAndSentinelRetry gcRetry;

    AtomicBoolean running = new AtomicBoolean(true);

    /**
     * This cache services requests for data at various addresses. In a memory implementation,
     * it is not backed by anything, but in a disk implementation it is backed by persistent storage.
     */
    LoadingCache<Long, LogUnitReadResponseMsg.LogUnitEntry> dataCache;

    long maxCacheSize;

    public LogUnitServer(Map<String, Object> opts)
    {
        this.opts = opts;

        maxCacheSize = Utils.parseLong((String)opts.get("--max-cache"));

        if ((Boolean)opts.get("--memory")) {
            log.warn("Log unit opened in-memory mode (Maximum size={}). " +
                    "This should be run for testing purposes only. " +
                    "If you exceed the maximum size of the unit, old entries will be AUTOMATICALLY trimmed. " +
                    "The unit WILL LOSE ALL DATA if it exits.", Utils.convertToByteStringRepresentation(maxCacheSize));
            reset();
        }
        else {
            try {
                fc = FileChannel.open(FileSystems.getDefault().getPath((String) opts.get("--log-path") + File.separator + "log"),
                        EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE,
                                StandardOpenOption.CREATE, StandardOpenOption.SPARSE));
                filePointer = new AtomicLong(fc.size());
                reset();
                log.info("Opened file channel for file {}", opts.get("--log-path"));
            } catch (Exception e)
            {
                log.error("Exception opening file channel, switching to in-memory mode.", e);
            }
        }
        gcThread = new Thread(this::runGC);
        gcThread.start();
    }

    @Override
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        switch(msg.getMsgType())
        {
            case WRITE:
                LogUnitWriteMsg writeMsg = (LogUnitWriteMsg) msg;
                log.trace("Handling write request for address {}", writeMsg.getAddress());
                write(writeMsg, ctx, r);
                break;
            case READ_REQUEST:
                LogUnitReadRequestMsg readMsg = (LogUnitReadRequestMsg) msg;
                log.trace("Handling read request for address {}", readMsg.getAddress());
                read(readMsg, ctx, r);
                break;
            case GC_INTERVAL:
            {
                LogUnitGCIntervalMsg m = (LogUnitGCIntervalMsg) msg;
                log.info("Garbage collection interval set to {}", m.getInterval());
                gcRetry.setRetryInterval(m.getInterval());
            }
            break;
            case FORCE_GC:
            {
                log.info("GC forced by client {}", msg.getClientID());
                gcThread.interrupt();
            }
            break;
            case FILL_HOLE:
            {
                LogUnitFillHoleMsg m = (LogUnitFillHoleMsg) msg;
                log.debug("Hole fill requested at {}", m.getAddress());
                dataCache.get(m.getAddress(), (address) -> new LogUnitEntry());
                r.sendResponse(ctx, m, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
            }
            break;
            case TRIM:
            {
                LogUnitTrimMsg m = (LogUnitTrimMsg) msg;
                trimMap.compute(m.getStreamID(), (key, prev) ->
                        prev == null ? m.getPrefix() : Math.max(prev, m.getPrefix()));
                log.debug("Trim requested at prefix={}", m.getPrefix());
            }
            break;
        }
    }

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

        dataCache = Caffeine.newBuilder()
                .<Long,LogUnitEntry>weigher((k,v) -> v.buffer == null ? 1 : v.buffer.readableBytes())
                .maximumWeight(maxCacheSize)
                .removalListener(this::handleEviction)
                .build(this::handleRetrieval);


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

    /** Retrieve the LogUnitEntry from disk, given an address.
     *
     * @param address   The address to retrieve the entry from.
     * @return          The log unit entry to retrieve into the cache.
     *                  This function should not care about trimmed addresses, as that is handled in
     *                  the read() and write(). Any address that cannot be retrieved should be returned as
     *                  unwritten (null).
     */
    public synchronized LogUnitEntry handleRetrieval(Long address) {
        log.trace("Retrieve[{}]", address);
        if (fc == null)
        {
            log.trace("This is an in-memory log unit, but a load was requested.");
            return null;
        }
        else
        {
            //do a linear scan of the correct file.
            long rAddress;
            long fAddress = 0;
            try {
                do {
                    ByteBuffer entryMeta = fc.map(FileChannel.MapMode.READ_ONLY, fAddress, 21);
                    int magic = entryMeta.getInt();
                    if (magic != 0xDEAD)
                    {
                        log.warn("Retrieve[{}]: magic expected to be 0xDEAD but found {}!", address, magic);
                    }
                    int entrySize = entryMeta.getInt();
                    rAddress = entryMeta.getLong();
                    int mmapSize = entryMeta.getInt();
                    byte control = entryMeta.get();
                    if (rAddress == address && control == 0)
                    {
                        log.warn("Retrieve[{}]: Found data but incomplete (control=0), skipping!", address);
                    }
                    else if (rAddress == address)
                    {
                        ByteBuffer oData = fc.map(FileChannel.MapMode.READ_ONLY, fAddress+21+mmapSize, entrySize-21-mmapSize);
                        ByteBuffer mData = fc.map(FileChannel.MapMode.READ_ONLY, fAddress+21,mmapSize);
                        ByteBuf mBuf = Unpooled.wrappedBuffer(mData);
                        log.trace("Retrieve[{}]: Match for data at address {}.", address, fAddress);
                        LogUnitEntry o =  new LogUnitEntry(Unpooled.wrappedBuffer(oData),
                                LogUnitMetadataMsg.mapFromBuffer(mBuf),
                                false,
                                true);
                        mBuf.release();
                        return o;
                    }
                    fAddress = fAddress + entrySize;
                    if (entrySize == 0)
                    {
                        log.warn("Encountered entry size of 0, aborting.");
                        break;
                    }
                }
                while (fAddress < fc.size());
                log.error("Retrieve[{}]: Couldn't find data in file!", address);
                return null;
            }
            catch (Exception e)
            {
                log.error("Retrieve[{}]: Exception reading file.", e);
                return null;
            }
        }
    }

    public synchronized void handleEviction(Long address, LogUnitEntry entry, RemovalCause cause) {
        log.trace("Eviction[{}]: {}", address, cause);
        if (entry.buffer != null) {
            if (fc == null) {
                log.warn("This is an in-memory log unit, data@{} will be trimmed and lost due to {}!", address, cause);
                trimRange.add(Range.closed(address, address));
            } else if (!entry.isPersisted) { //don't persist an entry twice.
                //evict the data by getting the next pointer.
                try {
                    ByteBuf metadataBuffer = Unpooled.buffer();
                    LogUnitMetadataMsg.bufferFromMap(metadataBuffer, entry.getMetadataMap());
                    int entrySize = entry.getBuffer().writerIndex() + metadataBuffer.writerIndex() + 21;
                    long entryPointer = filePointer.getAndAdd(entrySize);
                    log.trace("Eviction[{}]: Mapped to {}", address, entryPointer);
                    ByteBuffer pointerBuffer = fc.map(FileChannel.MapMode.READ_WRITE, entryPointer, 21);
                    pointerBuffer.putInt(0xDEAD);
                    pointerBuffer.putInt(entrySize);
                    pointerBuffer.putLong(address);
                    pointerBuffer.putInt(metadataBuffer.writerIndex());
                    fc.write(metadataBuffer.nioBuffer(), entryPointer + 21);
                    fc.write(entry.buffer.nioBuffer(), entryPointer + 21 + metadataBuffer.writerIndex());
                    fc.force(true);
                    metadataBuffer.release();
                    pointerBuffer.put((byte) 1);
                    pointerBuffer.flip();
                    log.info("Eviction[{}]: Written to disk.", address);
                }
                catch (Exception e)
                {
                    log.error("Eviction[{}]: Exception", address, e);
                }
            }
            // Free the internal buffer once the data has been evicted (in the case the server is not sync).
            entry.buffer.release();
        }
    }
    /** Service an incoming read request. */
    public void read(LogUnitReadRequestMsg msg, ChannelHandlerContext ctx, IServerRouter r)
    {
        log.trace("Read[{}]", msg.getAddress());
        if (trimRange.contains (msg.getAddress()))
        {
            r.sendResponse(ctx, msg, new LogUnitReadResponseMsg(ReadResultType.TRIMMED));
        }
        else
        {
            LogUnitEntry e = dataCache.get(msg.getAddress());
            if (e == null)
            {
                r.sendResponse(ctx, msg, new LogUnitReadResponseMsg(ReadResultType.EMPTY));
            }
            else if (e.isHole)
            {
                r.sendResponse(ctx, msg, new LogUnitReadResponseMsg(ReadResultType.FILLED_HOLE));
            } else {
                r.sendResponse(ctx, msg, new LogUnitReadResponseMsg(e));
            }
        }
    }

    /** Service an incoming write request. */
    public void write(LogUnitWriteMsg msg, ChannelHandlerContext ctx, IServerRouter r)
    {
        log.trace("Write[{}]", msg.getAddress());
        //TODO: locking of trimRange.
        if (trimRange.contains (msg.getAddress()))
        {
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ERROR_TRIMMED));
        }
        else {
            LogUnitEntry e = new LogUnitEntry(msg.getData(), msg.getMetadataMap(), false);
            e.getBuffer().retain();
            if (e == dataCache.get(msg.getAddress(), (address) -> e)) {
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ERROR_OK));
            }
            else
            {
                e.getBuffer().release();
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ERROR_OVERWRITE));
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

        log.trace("Trim range is {}", trimRange);

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
                        log.trace("Trimming entry at {}", address);
                        trimEntry(address, streams, buffer);
                        freedEntries++;
                    }
                }
                else {
                    //this is an entry which belongs in all streams
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
