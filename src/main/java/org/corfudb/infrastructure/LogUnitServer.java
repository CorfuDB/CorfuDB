package org.corfudb.infrastructure;

import com.github.benmanes.caffeine.cache.CacheWriter;
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
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.Utils;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalAndSentinelRetry;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
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

    /** The log file prefix, which can be null if the server is in memory. */
    String prefix;

    @Data
    class FileHandle {
        final AtomicLong filePointer;
        final FileChannel channel;
        final RangeSet<Long> knownAddresses = TreeRangeSet.create();
    }

    /** A map mapping to file channels. */
    Map<Long, FileHandle> channelMap;

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
            channelMap = new ConcurrentHashMap<>();
            prefix = opts.get("--log-path") + File.separator + "log";
        }

        reset();

        gcThread = new Thread(this::runGC);
        gcThread.start();
    }

    @Data
    static class LogFileHeader {
        static final String magic = "CORFULOG";
        final int version;
        final long flags;
        ByteBuffer getBuffer()
        {
            ByteBuffer b = ByteBuffer.allocate(64);
            // 0: "CORFULOG" header(8)
            b.put(magic.getBytes(Charset.forName("UTF-8")),0, 8);
            // 8: Version number(4)
            b.putInt(version);
            // 12: Flags (8)
            b.putLong(flags);
            // 20: Reserved (54)
            b.position(64);
            b.flip();
            return b;
        }
        static LogFileHeader fromBuffer(ByteBuffer buffer)
        {
            byte[] bMagic = new byte[8];
            buffer.get(bMagic, 0, 8);
            if (!new String(bMagic).equals(magic))
            {
                log.warn("Encountered invalid magic, expected {}, got {}", magic, new String(bMagic));
                throw new RuntimeException("Invalid header magic!");
            }
            return new LogFileHeader(buffer.getInt(), buffer.getLong());
        }
    }

    /** Write the header for a Corfu log file.
     *
     * @param fc            The filechannel to use.
     * @param pointer       The pointer to increment to the start position.
     * @param version       The version number to write to the header.
     * @param flags         Flags, if any to write to the header.
     * @throws IOException
     */
    public void writeHeader(FileChannel fc, AtomicLong pointer, int version, long flags)
            throws IOException
    {
        LogFileHeader lfg = new LogFileHeader(version, flags);
        ByteBuffer b = lfg.getBuffer();
        pointer.getAndAdd(b.remaining());
        fc.write(b);
        fc.force(true);
    }

    /** Read the header for a Corfu log file.
     *
     * @param fc            The filechannel to use.
     * @param version       The version number to write to the header.
     * @param flags         Flags, if any to write to the header.
     * @throws IOException
     */
    public LogFileHeader readHeader(FileChannel fc)
            throws IOException
    {
        ByteBuffer b = fc.map(FileChannel.MapMode.READ_ONLY, 0, 64);
        return LogFileHeader.fromBuffer(b);
    }

    /** Write a log entry to a file.
     *
     * @param fh            The file handle to use.
     * @param address       The address of the entry.
     * @param entry         The LogUnitEntry to write.
     */
    public void writeEntry(FileHandle fh, long address, LogUnitEntry entry)
        throws IOException
    {
        ByteBuf metadataBuffer = Unpooled.buffer();
        LogUnitMetadataMsg.bufferFromMap(metadataBuffer, entry.getMetadataMap());
        int entrySize = entry.getBuffer().writerIndex() + metadataBuffer.writerIndex() + 24;
        long pos = fh.getFilePointer().getAndAdd(entrySize);
        MappedByteBuffer o = fh.getChannel().map(FileChannel.MapMode.READ_WRITE, pos, entrySize);
        o.putInt(0x4C450000); // Flags
        o.putLong(address); // the log unit address
        o.putInt(entrySize); // Size
        o.putInt(metadataBuffer.writerIndex()); // the metadata size
        o.put(metadataBuffer.nioBuffer());
        o.put(entry.buffer.nioBuffer());
        metadataBuffer.release();
        o.putShort(2, (short) 1); // written flag
        o.flip();
        o.force();
    }

    /** Find a log entry in a file.
     * @param fh            The file handle to use.
     * @param address       The address of the entry.
     * @return              The log unit entry at that address, or NULL if there was no entry.
     */
    public LogUnitEntry readEntry(FileHandle fh, long address)
        throws IOException
    {
        MappedByteBuffer o = fh.getChannel().map(FileChannel.MapMode.READ_ONLY, 64L, fh.getChannel().size());
        while (o.hasRemaining())
        {
            short magic = o.getShort();
            if (magic != 0x4C45)
            {
                return null;
            }
            short flags = o.getShort();
            long addr = o.getLong();
            fh.knownAddresses.add(Range.singleton(address));
            int size = o.getInt();
            if (addr != address)
            {
                o.position(o.position() + size-16); //skip over (size-20 is what we haven't read).
                log.trace("Read address {}, not match, skipping. (remain={})", addr, o.remaining());
            }
            else {
                log.debug("Entry at {} hit, reading.", address);
                if (flags % 2 == 0) {
                    log.error("Read a log entry but the write was torn, aborting!");
                    throw new IOException("Torn write detected!");
                }
                int metadataMapSize = o.getInt();
                ByteBuf mBuf = Unpooled.wrappedBuffer(o.slice());
                o.position(o.position() + metadataMapSize);
                ByteBuffer dBuf = o.slice();
                dBuf.limit(size - 36);
                return new LogUnitEntry(Unpooled.wrappedBuffer(dBuf),
                        LogUnitMetadataMsg.mapFromBuffer(mBuf),
                        false,
                        true);
            }
        }
        return null;
    }

    /** Gets the file channel for a particular address, creating it
     * if is not present in the map.
     * @param address   The address to open.
     * @return          The FileChannel for that address.
     */
    public FileHandle getChannelForAddress(long address)
    {
        return channelMap.computeIfAbsent(address/10000, a -> {
            String filePath = prefix + a.toString();
            try {
                FileChannel fc = FileChannel.open(FileSystems.getDefault().getPath(filePath),
                        EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE,
                                StandardOpenOption.CREATE, StandardOpenOption.SPARSE));
                AtomicLong fp = new AtomicLong();
                writeHeader(fc, fp, 1, 0);
                log.info("Created new log file at {}", filePath);
                return new FileHandle(fp, fc);
            }
            catch (IOException e)
            {
                log.error("Error opening file {}", a, e);
                throw new RuntimeException(e);
            }
        });
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
                .<Long, LogUnitEntry>weigher((k, v) -> v.buffer == null ? 1 : v.buffer.readableBytes())
                .maximumWeight(maxCacheSize)
                .removalListener(this::handleEviction)
                .writer(new CacheWriter<Long, LogUnitEntry>() {
                    @Override
                    public void write(Long address, LogUnitEntry entry) {
                        if (dataCache.getIfPresent(address) != null) {
                            throw new RuntimeException("overwrite");
                        }
                        if (!entry.isPersisted && prefix != null) { //don't persist an entry twice.
                            //evict the data by getting the next pointer.
                            try {
                                // make sure the entry doesn't currently exist...
                                // (probably need a faster way to do this - high watermark?)
                                FileHandle fh = getChannelForAddress(address);
                                if (!fh.getKnownAddresses().contains(address)) {
                                    writeEntry(fh, address, entry);
                                } else {
                                    throw new Exception("overwrite");
                                }
                                log.info("Disk_write[{}]: Written to disk.", address);
                            } catch (Exception e) {
                                log.error("Disk_write[{}]: Exception", address, e);
                                throw new RuntimeException(e);
                            }
                        }
                    }

                    @Override
                    public void delete(Long aLong, LogUnitEntry logUnitEntry, RemovalCause removalCause) {
                        // never need to delete
                    }
                }).build(this::handleRetrieval);


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
    public LogUnitEntry handleRetrieval(Long address) {
        log.trace("Retrieve[{}]", address);
        if (prefix == null)
        {
            log.trace("This is an in-memory log unit, but a load was requested.");
            return null;
        }
        FileHandle fh = getChannelForAddress(address);
        try {
            log.info("Got header {}", readHeader(fh.getChannel()));
            return readEntry(getChannelForAddress(address), address);
        } catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    public void handleEviction(Long address, LogUnitEntry entry, RemovalCause cause) {
        log.trace("Eviction[{}]: {}", address, cause);
        if (entry.buffer != null) {
            if (prefix == null) {
                log.warn("This is an in-memory log unit, data@{} will be trimmed and lost due to {}!", address, cause);
                trimRange.add(Range.closed(address, address));
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
            try {
                dataCache.put(msg.getAddress(), e);
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ERROR_OK));
            } catch (Exception ex)
            {
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ERROR_OVERWRITE));
                e.getBuffer().release();
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
