package org.corfudb.infrastructure.log;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.FileUtils;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OverwriteException;

/**
 * This class implements the StreamLog by persisting the stream log as records in multiple files.
 * This StreamLog implementation can detect log file corruption, if checksum is enabled, otherwise
 * the checksum field will be ignored.
 *
 * StreamLogFiles:
 *     Header LogRecords
 *
 * Header: {@LogFileHeader}
 *
 * LogRecords: LogRecord || LogRecord LogRecords
 *
 * LogRecord: {
 *     delimiter 2 bytes
 *     checksum 4 bytes
 *     address 8 bytes
 *     LogData size 4 bytes
 *     LogData
 * }
 *
 * Created by maithem on 10/28/16.
 */

@Slf4j
public class StreamLogFiles implements StreamLog {

    static public final short RECORD_DELIMITER = 0x4C45;
    static public int VERSION = 1;

    private final boolean noVerify;
    public final String logDir;
    private Map<Long, FileHandle> channelMap;

    public StreamLogFiles(String logDir, boolean noVerify) {
        this.logDir = logDir;
        channelMap = new HashMap<>();
        this.noVerify = noVerify;

        verifyLogs();
    }

    private void verifyLogs() {
        String[] extension = {"log"};
        File dir = new File(logDir);

        if(dir.exists()) {
            Collection<File> files = FileUtils.listFiles(dir, extension, true);

            for(File file : files){
                try {
                    FileInputStream fIn = new FileInputStream(file);
                    FileChannel fc = fIn.getChannel();
                    ByteBuffer buf = ByteBuffer.allocate(LogFileHeader.size);
                    fc.read(buf);
                    buf.rewind();

                    LogFileHeader header = LogFileHeader.fromBuffer(buf);

                    if(header.getVersion() != VERSION) {
                        String msg = String.format("Log version {} for {} should match the logunit log version {}",
                                header.getVersion(), file.getAbsoluteFile(), VERSION);
                        throw new RuntimeException(msg);
                    }

                    if (noVerify == false && header.verify == false) {
                        String msg = String.format("Log file {} not generated with checksums, can't verify!",
                                file.getAbsoluteFile());
                        throw new RuntimeException(msg);
                    }

                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e.getCause());
                }
            }
        }
    }

    @Override
    public void sync(){
        //Todo(Maithem) flush writes to disk.
    }

    /**
     * Write the header for a Corfu log file.
     *
     * @param fc      The filechannel to use.
     * @param pointer The pointer to increment to the start position.
     * @param version The version number to write to the header.
     * @param verify  Checksum verify flag
     * @throws IOException
     */
    static public void writeHeader(FileChannel fc, AtomicLong pointer, int version, boolean verify)
            throws IOException {
        LogFileHeader lfg = new LogFileHeader(version, verify);
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
    private LogData readRecord(FileHandle fh, long address)
            throws IOException {
        ByteBuffer o = fh.getMapForRegion(LogFileHeader.size, (int) fh.getChannel().size());
        while (o.hasRemaining()) {

            short magic = o.getShort();

            if (magic != RECORD_DELIMITER) {
                return null;
            }


            int checksum = o.getInt();
            ByteBuffer checksumBuf = o.slice();
            long entryAddress = o.getLong();
            int entryLen = o.getInt();

            if(!noVerify) {
                Hasher computedChecksum = Hashing.crc32c().newHasher();

                int recordLen = Long.BYTES     // Size of address
                              + Integer.BYTES  // Size of entry length
                              + entryLen;      // entry size

                for(int x = 0; x < recordLen; x++) {
                    computedChecksum.putByte(checksumBuf.get());
                }

                if(checksum != computedChecksum.hash().asInt()) {
                    log.error("Checksum mismatch detected while trying to read address {}", address);
                    throw new DataCorruptionException();
                }
            }


            if (address == -1) {
                //Todo(Maithem) : maybe we can move this to getChannelForAddress
                fh.knownAddresses.add(entryAddress);
            }

            if (entryAddress != address) {
                o.position(o.position() + entryLen); //skip over (size-20 is what we haven't read).
                log.trace("Read address {}, not match {}, skipping. (remain={})", entryAddress, address, o.remaining());
            } else {
                log.debug("Entry at {} hit, reading (size={}).", address, entryLen);

                ByteBuf buf = Unpooled.wrappedBuffer(o.slice());
                return new LogData(buf);
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
            String filePath = logDir + a.toString() + ".log";
            try {
                FileChannel fc = FileChannel.open(FileSystems.getDefault().getPath(filePath),
                        EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE,
                                StandardOpenOption.CREATE, StandardOpenOption.SPARSE));

                AtomicLong fp = new AtomicLong();
                boolean verify = true;

                if(noVerify){
                    verify = false;
                }

                writeHeader(fc, fp, VERSION, verify);
                log.info("Opened new log file at {}", filePath);
                FileHandle fh = new FileHandle(fp, fc);
                // The first time we open a file we should read to the end, to load the
                // map of entries we already have.
                readRecord(fh, -1);
                return fh;
            } catch (IOException e) {
                log.error("Error opening file {}", a, e);
                throw new RuntimeException(e);
            }
        });
    }


    /**
     * Write a log entry record to a file.
     *
     * @param fh      The file handle to use.
     * @param address The address of the entry.
     * @param entry   The LogUnitEntry to write.
     */
    private void writeRecord(FileHandle fh, long address, LogData entry) throws IOException {
        ByteBuf recordBuf = Unpooled.buffer();

        recordBuf.writeShort(RECORD_DELIMITER);
        int checkSumInd = recordBuf.writerIndex();
        recordBuf.writeInt(0);
        recordBuf.writeLong(address);
        int dataSizeInd = recordBuf.writerIndex();
        recordBuf.writeInt(0);
        int entryInd = recordBuf.writerIndex();
        entry.doSerialize(recordBuf);
        int recordBufLastInd = recordBuf.writerIndex();

        // Rewind writer index to write entry length
        recordBuf.writerIndex(dataSizeInd);
        recordBuf.writeInt(recordBufLastInd - entryInd);

        // Restore writer index
        recordBuf.writerIndex(recordBufLastInd);

        if(!noVerify){
            // Checksum is only computed over the address and the entry, so the reader
            // index needs to skip the delimiter and checksum
            recordBuf.readShort();
            recordBuf.readInt();

            Hasher hasher = Hashing.crc32c().newHasher();

            while (recordBuf.isReadable()) {
                hasher.putByte(recordBuf.readByte());
            }

            recordBuf.readerIndex(checkSumInd);
            recordBuf.writerIndex(checkSumInd);
            recordBuf.writeInt(hasher.hash().asInt());
        }

        // Restore record buffer pointers
        recordBuf.readerIndex(0);
        recordBuf.writerIndex(recordBufLastInd);

        long pos = fh.getFilePointer().getAndAdd(recordBuf.writerIndex());
        ByteBuffer o = fh.getMapForRegion((int) pos, recordBuf.writerIndex());
        o.put(recordBuf.nioBuffer());
        o.flip();
        recordBuf.release();
    }

    @Override
    public void append(long address, LogData entry) {
        //evict the data by getting the next pointer.
        try {
            // make sure the entry doesn't currently exist...
            // (probably need a faster way to do this - high watermark?)
            FileHandle fh = getChannelForAddress(address);
            if (!fh.getKnownAddresses().contains(address)) {
                writeRecord(fh, address, entry);
                fh.getKnownAddresses().add(address);
            } else {
                throw new OverwriteException();
            }
            log.info("Disk_write[{}]: Written to disk.", address);
        } catch (Exception e) {
            log.error("Disk_write[{}]: Exception", address, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public LogData read(long address) {
        try {
            return readRecord(getChannelForAddress(address), address);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Data
    class FileHandle {
        final AtomicLong filePointer;
        @NonNull
        private FileChannel channel;
        private Set<Long> knownAddresses = Collections.newSetFromMap(new ConcurrentHashMap<>());
        private MappedByteBuffer byteBuffer;

        public ByteBuffer getMapForRegion(int offset, int size) {
            if (byteBuffer == null) {
                byteBuffer = getMappedBuffer();
            }
            ByteBuffer o = byteBuffer.duplicate();
            o.position(offset);
            return o.slice();
        }

        private MappedByteBuffer getMappedBuffer() {
            try {
                return channel.map(FileChannel.MapMode.READ_WRITE, 0L, LogUnitServer.maxLogFileSize);
            } catch (IOException ie) {
                log.error("Failed to map buffer for channel.");
                throw new RuntimeException(ie);
            }
        }
    }

    @Data
    static class LogFileHeader {
        static final String magic = "CORFULOG";
        static final int size = 64;
        final int version;
        final boolean verify;

        static LogFileHeader fromBuffer(ByteBuffer buffer) {
            byte[] bMagic = new byte[8];
            buffer.get(bMagic, 0, 8);
            if (!new String(bMagic).equals(magic)) {
                log.warn("Encountered invalid magic, expected {}, got {}", magic, new String(bMagic));
                throw new RuntimeException("Invalid header magic!");
            }

            int version = buffer.getInt();
            byte verifyByte = buffer.get();
            boolean verify = true;

            if(verifyByte == 0x0) {
                verify = false;
            }

            return new LogFileHeader(version, verify);
        }

        ByteBuffer getBuffer() {
            ByteBuffer b = ByteBuffer.allocate(size);
            // 0: "CORFULOG" header(8)
            b.put(magic.getBytes(Charset.forName("UTF-8")), 0, 8);
            // 8: Version number(4)
            b.putInt(version);
            // 12: Flags (8)
            if(verify) {
                b.put((byte) 0xf);
            } else {
                b.put((byte) 0x0);
            }
            b.put((byte) 0);
            b.putShort((short) 0);
            b.putInt(0);
            // 20: Reserved (54)
            b.position(size);
            b.flip();
            return b;
        }
    }

    @Override
    public void close() {
        Iterator<Long> it = channelMap.keySet().iterator();
        while (it.hasNext()) {
            Long key = it.next();
            FileHandle fh = channelMap.get(key);
            try {
                fh.getChannel().close();
                fh.channel = null;
                fh.knownAddresses = null;
                fh.byteBuffer = null;
                // We need to call System.gc() to force the unmapping of the file.
                // Without unmapping, the file remains open & leaks space. {sadpanda}
                //
                // OS X + HFS+ makes an additional hassle because HFS+ doesn't support
                // sparse files, so if the mapping is 2GB, then the OS will write 2GB
                // of data at unmap time, whether we like it or not.
                System.gc();
            } catch (IOException e) {
                log.warn("Error closing fh {}: {}", fh.toString(), e.toString());
            }
        }
        channelMap = new HashMap<>();
    }
}
