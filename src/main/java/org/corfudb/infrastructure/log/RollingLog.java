package org.corfudb.infrastructure.log;

import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ICorfuPayload;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.util.serializer.Serializers;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by maithem on 7/15/16.
 */

@Slf4j
public class RollingLog extends AbstractLocalLog {

    private final Map<Long, FileHandle> channelMap;

    public RollingLog(long start, long end, String path, boolean sync) {
        super(start, end, path, sync);
        channelMap = new HashMap<>();
    }

    /**
     * Write the header for a Corfu log file.
     *
     * @param fc      The filechannel to use.
     * @param pointer The pointer to increment to the start position.
     * @param version The version number to write to the header.
     * @param flags   Flags, if any to write to the header.
     * @throws IOException
     */
    private void writeHeader(FileChannel fc, AtomicLong pointer, int version, long flags)
            throws IOException {
        LogFileHeader lfg = new LogFileHeader(version, flags);
        ByteBuffer b = lfg.getBuffer();
        pointer.getAndAdd(b.remaining());
        fc.write(b);
        fc.force(true);
    }

    /**
     * Find a log entry in a file.
     *
     * @param fh      The file handle to use.
     * @param address The address of the entry.
     * @return The log unit entry at that address, or NULL if there was no entry.
     */
    private LogData readEntry(FileHandle fh, long address)
            throws IOException {
        ByteBuffer o = fh.getMapForRegion(64, (int) fh.getChannel().size());
        while (o.hasRemaining()) {
            short magic = o.getShort();
            if (magic != 0x4C45) {
                return null;
            }
            short flags = o.getShort();
            long addr = o.getLong();
            if (address == -1) {
                //Todo(Maithem) : maybe we can move this to getChannelForAddress
                fh.knownAddresses.add(addr);
            }
            int size = o.getInt();
            if (addr != address) {
                o.position(o.position() + size - 16); //skip over (size-20 is what we haven't read).
                log.trace("Read address {}, not match {}, skipping. (remain={})", addr, address, o.remaining());
            } else {
                log.debug("Entry at {} hit, reading (size={}).", address, size);
                if (flags % 2 == 0) {
                    log.error("Read a log entry but the write was torn, aborting!");
                    throw new IOException("Torn write detected!");
                }
                int metadataMapSize = o.getInt();
                ByteBuf mBuf = Unpooled.wrappedBuffer(o.slice());
                o.position(o.position() + metadataMapSize);
                ByteBuffer dBuf = o.slice();
                dBuf.limit(size - metadataMapSize - 24);
                return new LogData(Unpooled.wrappedBuffer(dBuf),
                        ICorfuPayload.enumMapFromBuffer(mBuf, IMetadata.LogUnitMetadataType.class, Object.class));
            }
        }
        return null;
    }

    /**
     * Gets the file channel for a particular address, creating it
     * if is not present in the map.
     *
     * @param address The address to open.
     * @return The FileChannel for that address.
     */
    private FileHandle getChannelForAddress(long address) {
        return channelMap.computeIfAbsent(address / 10000, a -> {
            String filePath = logPathDir + a.toString();
            try {
                FileChannel fc = FileChannel.open(FileSystems.getDefault().getPath(filePath),
                        EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE,
                                StandardOpenOption.CREATE, StandardOpenOption.SPARSE));

                AtomicLong fp = new AtomicLong();
                writeHeader(fc, fp, 1, 0);
                log.info("Opened new log file at {}", filePath);
                FileHandle fh = new FileHandle(fp, fc);
                // The first time we open a file we should read to the end, to load the
                // map of entries we already have.
                readEntry(fh, -1);
                return fh;
            } catch (IOException e) {
                log.error("Error opening file {}", a, e);
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Read the header for a Corfu log file.
     *
     * @param fc The filechannel to use.
     * @throws IOException
     */
    private LogFileHeader readHeader(FileChannel fc)
            throws IOException {
        ByteBuffer b = fc.map(FileChannel.MapMode.READ_ONLY, 0, 64);
        return LogFileHeader.fromBuffer(b);
    }

    /**
     * Write a log entry to a file.
     *
     * @param fh      The file handle to use.
     * @param address The address of the entry.
     * @param entry   The LogUnitEntry to write.
     */
    private void writeEntry(FileHandle fh, long address, LogData entry)
            throws IOException {
        ByteBuf metadataBuffer = Unpooled.buffer();
        ICorfuPayload.serialize(metadataBuffer, entry.getMetadataMap());
        int entrySize = entry.getData().writerIndex() + metadataBuffer.writerIndex() + 24;
        long pos = fh.getFilePointer().getAndAdd(entrySize);
        ByteBuffer o = fh.getMapForRegion((int) pos, entrySize);
        o.putInt(0x4C450000); // Flags
        o.putLong(address); // the log unit address
        o.putInt(entrySize); // Size
        o.putInt(metadataBuffer.writerIndex()); // the metadata size
        o.put(metadataBuffer.nioBuffer());
        o.put(entry.getData().nioBuffer());
        metadataBuffer.release();
        o.putShort(2, (short) 1); // written flag
        o.flip();
    }

    protected void backendWrite(long address, LogData entry) {
        //evict the data by getting the next pointer.
        try {
            // make sure the entry doesn't currently exist...
            // (probably need a faster way to do this - high watermark?)
            FileHandle fh = getChannelForAddress(address);
            if (!fh.getKnownAddresses().contains(address)) {
                fh.getKnownAddresses().add(address);
                if (sync) {
                    writeEntry(fh, address, entry);
                } else {
                    CompletableFuture.runAsync(() -> {
                        try {
                            writeEntry(fh, address, entry);
                        } catch (Exception e) {
                            log.error("Disk_write[{}]: Exception", address, e);
                        }
                    });
                }
            } else {
                throw new Exception("overwrite");
            }
            log.info("Disk_write[{}]: Written to disk.", address);
        } catch (Exception e) {
            log.error("Disk_write[{}]: Exception", address, e);
            throw new RuntimeException(e);
        }
    }

    protected LogData backendRead(long address) {
        try {
            return readEntry(getChannelForAddress(address), address);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void initializeLog() {

    }

    protected void backendStreamWrite(UUID streamID, RangeSet<Long> entry){
        try {
            ByteBuf b = Unpooled.buffer();
            Set<Range<Long>> rs = entry.asRanges();
            b.writeInt(rs.size());
            for (Range<Long> r : rs) {
                Serializers
                        .getSerializer(Serializers.SerializerType.JAVA).serialize(r, b);
            }
            com.google.common.io.Files.write(b.array(), new File(logPathDir + File.pathSeparator +
                    "stream" + streamID.toString()));
        } catch (IOException ie) {
            log.error("IOException while writing stream range for stream {}", streamID);
        }
    }

    protected RangeSet<Long> backendStreamRead(UUID streamID) {
        Path p = FileSystems.getDefault().getPath(logPathDir + File.pathSeparator +
                "stream" + streamID.toString());
        try {
            if (Files.exists(p)) {
                ByteBuf b = Unpooled.wrappedBuffer(Files.readAllBytes(p));
                RangeSet rs = TreeRangeSet.create();
                int ranges = b.readInt();
                for (int i = 0; i < ranges; i++) {
                    Range r = (Range) Serializers
                            .getSerializer(Serializers.SerializerType.JAVA).deserialize(b, null);
                    rs.add(r);
                }
                return rs;
            }
        } catch (IOException ie) {
            log.error("IO Exception reading from stream file {}", p);
        }

        return TreeRangeSet.create();
    }

    @Data
    class FileHandle {
        final AtomicLong filePointer;
        final FileChannel channel;
        final Set<Long> knownAddresses = Collections.newSetFromMap(new ConcurrentHashMap<>());
        @Getter(lazy = true)
        private final MappedByteBuffer byteBuffer = getMappedBuffer();

        public ByteBuffer getMapForRegion(int offset, int size) {
            ByteBuffer o = getByteBuffer().duplicate();
            o.position(offset);
            return o.slice();
        }

        private MappedByteBuffer getMappedBuffer() {
            try {
                return channel.map(FileChannel.MapMode.READ_WRITE, 0L, Integer.MAX_VALUE);
            } catch (IOException ie) {
                log.error("Failed to map buffer for channel.");
                throw new RuntimeException(ie);
            }
        }
    }

    @Data
    static class LogFileHeader {
        static final String magic = "CORFULOG";
        final int version;
        final long flags;

        static LogFileHeader fromBuffer(ByteBuffer buffer) {
            byte[] bMagic = new byte[8];
            buffer.get(bMagic, 0, 8);
            if (!new String(bMagic).equals(magic)) {
                log.warn("Encountered invalid magic, expected {}, got {}", magic, new String(bMagic));
                throw new RuntimeException("Invalid header magic!");
            }
            return new LogFileHeader(buffer.getInt(), buffer.getLong());
        }

        ByteBuffer getBuffer() {
            ByteBuffer b = ByteBuffer.allocate(64);
            // 0: "CORFULOG" header(8)
            b.put(magic.getBytes(Charset.forName("UTF-8")), 0, 8);
            // 8: Version number(4)
            b.putInt(version);
            // 12: Flags (8)
            b.putLong(flags);
            // 20: Reserved (54)
            b.position(64);
            b.flip();
            return b;
        }
    }
}
