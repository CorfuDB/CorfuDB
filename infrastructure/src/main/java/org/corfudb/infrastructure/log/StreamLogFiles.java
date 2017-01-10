package org.corfudb.infrastructure.log;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.FileUtils;
import org.corfudb.format.Types.DataType;
import org.corfudb.format.Types.LogEntry;
import org.corfudb.format.Types.LogHeader;
import org.corfudb.format.Types.Metadata;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OverwriteException;

/**
 * This class implements the StreamLog by persisting the stream log as records in multiple files.
 * This StreamLog implementation can detect log file corruption, if checksum is enabled, otherwise
 * the checksum field will be ignored.
 * <p>
 * StreamLogFiles:
 * Header LogRecords
 * <p>
 * Header: {@LogFileHeader}
 * <p>
 * LogRecords: LogRecord || LogRecord LogRecords
 * <p>
 * LogRecord: {
 * delimiter 2 bytes
 * checksum 4 bytes
 * address 8 bytes
 * LogData size 4 bytes
 * LogData
 * }
 * <p>
 * Created by maithem on 10/28/16.
 */

@Slf4j
public class StreamLogFiles implements StreamLog {

    static public final short RECORD_DELIMITER = 0x4C45;
    static public int VERSION = 1;
    static public int RECORDS_PER_LOG_FILE = 10000;

    static public final int METADATA_SIZE = Metadata.newBuilder()
            .setChecksum(-1)
            .setLength(-1)
            .build()
            .getSerializedSize();
    private final boolean noVerify;
    public final String logDir;
    private Map<String, FileHandle> writeChannels;
    private Set<FileChannel> channelsToSync;

    public StreamLogFiles(String logDir, boolean noVerify) {
        this.logDir = logDir;
        writeChannels = new HashMap<>();
        channelsToSync = new HashSet<>();
        this.noVerify = noVerify;

        verifyLogs();
    }

    private void verifyLogs() {
        String[] extension = {"log"};
        File dir = new File(logDir);

        if (dir.exists()) {
            Collection<File> files = FileUtils.listFiles(dir, extension, true);

            for (File file : files) {
                try {
                    FileInputStream fIn = new FileInputStream(file);
                    FileChannel fc = fIn.getChannel();


                    ByteBuffer metadataBuf = ByteBuffer.allocate(METADATA_SIZE);
                    fc.read(metadataBuf);
                    metadataBuf.flip();

                    Metadata metadata = Metadata.parseFrom(metadataBuf.array());

                    ByteBuffer headerBuf = ByteBuffer.allocate(metadata.getLength());
                    fc.read(headerBuf);
                    headerBuf.flip();

                    LogHeader header = LogHeader.parseFrom(headerBuf.array());

                    fc.close();
                    fIn.close();

                    if(metadata.getChecksum() != getChecksum(header.toByteArray())) {
                        log.error("Checksum mismatch detected while trying to read header for logfile {}", file);
                        throw new DataCorruptionException();
                    }

                    if (header.getVersion() != VERSION) {
                        String msg = String.format("Log version {} for {} should match the logunit log version {}",
                                header.getVersion(), file.getAbsoluteFile(), VERSION);
                        throw new RuntimeException(msg);
                    }

                    if (noVerify == false && header.getVerifyChecksum() == false) {
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
    public void sync() throws IOException {
        for (FileChannel ch : channelsToSync) {
            ch.force(true);
        }
        log.debug("Sync'd {} channels", channelsToSync.size());
        channelsToSync.clear();
    }

    /**
     * Write the header for a Corfu log file.
     *
     * @param fc      The file channel to use.
     * @param version The version number to write to the header.
     * @param verify  Checksum verify flag
     * @throws IOException
     */
    static public void writeHeader(FileChannel fc, int version, boolean verify)
            throws IOException {

        LogHeader header = LogHeader.newBuilder()
                .setVersion(version)
                .setVerifyChecksum(verify)
                .build();

        ByteBuffer buf = getByteBufferWithMetaData(header);
        fc.write(buf);
        fc.force(true);
    }

    static private ByteBuffer getByteBufferWithMetaData(AbstractMessage message) {
        Metadata metadata = Metadata.newBuilder()
                .setChecksum(getChecksum(message.toByteArray()))
                .setLength(message.getSerializedSize())
                .build();

        ByteBuffer buf = ByteBuffer.allocate(metadata.getSerializedSize() + message.getSerializedSize());
        buf.put(metadata.toByteArray());
        buf.put(message.toByteArray());
        buf.flip();
        return buf;
    }

    private LogData getLogData(LogEntry entry) {
        ByteBuf data = Unpooled.wrappedBuffer(entry.getData().toByteArray());
        LogData logData = new LogData(org.corfudb.protocols.wireprotocol.
                DataType.typeMap.get((byte) entry.getDataType().getNumber()), data);

        logData.setBackpointerMap(getUUIDLongMap(entry.getBackpointersMap()));
        logData.setGlobalAddress(entry.getGlobalAddress());
        logData.setBackpointerMap(getUUIDLongMap(entry.getLogicalAddressesMap()));
        logData.setStreams(getStreamsSet(entry.getStreamsList().asByteStringList()));
        logData.setRank(entry.getRank());

        logData.clearCommit();

        if(entry.getCommit()) {
            logData.setCommit();
        }

        return logData;
    }

    Set<UUID> getStreamsSet(List<ByteString> list) {
        Set<UUID> set = new HashSet();

        for(ByteString string : list) {
            set.add(UUID.fromString(string.toString(StandardCharsets.UTF_8)));
        }

        return set;
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

        // A channel lock is required to read the file size because the channel size can change
        // when it is written to, so when we read the size we need to guarantee that the size only
        // includes fully written records.
        long logFileSize;

        synchronized (fh.lock) {
            logFileSize = fh.channel.size();
        }

        FileChannel fc = getChannel(fh.fileName, true);

        if (fc == null) {
            return null;
        }

        // Skip the header
        ByteBuffer headerMetadataBuf = ByteBuffer.allocate(METADATA_SIZE);
        fc.read(headerMetadataBuf);
        headerMetadataBuf.flip();

        Metadata headerMetadata = Metadata.parseFrom(headerMetadataBuf.array());

        fc.position(fc.position() + headerMetadata.getLength());
        ByteBuffer o = ByteBuffer.allocate((int) logFileSize - (int) fc.position());
        fc.read(o);
        fc.close();
        o.flip();

        while (o.hasRemaining()) {

            short magic = o.getShort();

            if (magic != RECORD_DELIMITER) {
                return null;
            }

            byte[] metadataBuf = new byte[METADATA_SIZE];
            o.get(metadataBuf);

            try {
                Metadata metadata = Metadata.parseFrom(metadataBuf);

                byte[] logEntryBuf = new byte[metadata.getLength()];

                o.get(logEntryBuf);

                LogEntry entry = LogEntry.parseFrom(logEntryBuf);

                if (!noVerify) {
                    if (metadata.getChecksum() != getChecksum(entry.toByteArray())) {
                        log.error("Checksum mismatch detected while trying to read address {}", address);
                        throw new DataCorruptionException();
                    }
                }

                if (address == -1) {
                    //Todo(Maithem) : maybe we can move this to getChannelForAddress
                    fh.knownAddresses.add(entry.getGlobalAddress());
                } else if (entry.getGlobalAddress() != address) {
                    log.trace("Read address {}, not match {}, skipping. (remain={})", entry.getGlobalAddress(), address);
                } else {
                    log.debug("Entry at {} hit, reading (size={}).", address, metadata.getLength());

                    return getLogData(entry);
                }
            } catch (InvalidProtocolBufferException e) {
                throw new DataCorruptionException();
            }
        }
        return null;
    }

    private FileChannel getChannel(String filePath, boolean readOnly) throws IOException {
        try {

            if (readOnly) {
                if (!new File(filePath).exists()) {
                    return null;
                } else {
                    return FileChannel.open(FileSystems.getDefault().getPath(filePath),
                            EnumSet.of(StandardOpenOption.READ));
                }
            } else {
                return FileChannel.open(FileSystems.getDefault().getPath(filePath),
                        EnumSet.of(StandardOpenOption.APPEND, StandardOpenOption.WRITE,
                                StandardOpenOption.CREATE, StandardOpenOption.SPARSE));
            }
        } catch (IOException e) {
            log.error("Error opening file {}", filePath, e);
            throw new RuntimeException(e);
        }

    }

    /**
     * Gets the file channel for a particular address, creating it
     * if is not present in the map.
     *
     * @param logAddress The address to open.
     * @return The FileChannel for that address.
     */
    private synchronized FileHandle getFileHandleForAddress(LogAddress logAddress) {
        String filePath = logDir + File.separator;
        long segment = logAddress.address / RECORDS_PER_LOG_FILE;

        if (logAddress.getStream() == null) {
            filePath += segment;
        } else {
            filePath += logAddress.getStream().toString() + "-" + segment;
        }

        filePath += ".log";

        return writeChannels.computeIfAbsent(filePath, a -> {

            try {
                FileChannel fc = getChannel(a, false);

                boolean verify = true;

                if (noVerify) {
                    verify = false;
                }

                writeHeader(fc, VERSION, verify);
                log.trace("Opened new log file at {}", a);
                FileHandle fh = new FileHandle(fc, a);
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

    Map<String, Long> getStrLongMap(Map<UUID, Long> uuidLongMap) {
        Map<String, Long> stringLongMap = new HashMap();

        for(Map.Entry<UUID, Long> entry : uuidLongMap.entrySet()) {
            stringLongMap.put(entry.getKey().toString(), entry.getValue());
        }

        return stringLongMap;
    }

    Map<UUID, Long> getUUIDLongMap(Map<String, Long> stringLongMap) {
        Map<UUID, Long> uuidLongMap = new HashMap();

        for(Map.Entry<String, Long> entry : stringLongMap.entrySet()) {
            uuidLongMap.put(UUID.fromString(entry.getKey()), entry.getValue());
        }

        return uuidLongMap;
    }


    Set<String> getStrUUID(Set<UUID> uuids) {
        Set<String> strUUIds = new HashSet();

        for(UUID uuid : uuids) {
            strUUIds.add(uuid.toString());
        }

        return strUUIds;
    }

    LogEntry getLogEntry(long address, LogData entry) {
        byte[] data = new byte[0];

        if (entry.getData() != null) {
            data = entry.getData();
        }

        boolean setCommit = false;
        Object val = entry.getMetadataMap().get(IMetadata.LogUnitMetadataType.COMMIT);
        if(val != null) {
            setCommit = (boolean) val;
        }

        LogEntry logEntry = LogEntry.newBuilder()
                .setDataType(DataType.forNumber(entry.getType().ordinal()))
                .setData(ByteString.copyFrom(data))
                .setGlobalAddress(address)
                .setRank(entry.getRank())
                .setCommit(setCommit)
                .addAllStreams(getStrUUID(entry.getStreams()))
                .putAllLogicalAddresses(getStrLongMap(entry.getLogicalAddresses()))
                .putAllBackpointers(getStrLongMap(entry.getBackpointerMap()))
                .build();

        return logEntry;
    }

    static int getChecksum(byte[] bytes) {
        Hasher hasher = Hashing.crc32c().newHasher();

        for(byte a : bytes) {
            hasher.putByte(a);
        }

        return hasher.hash().asInt();
    }

    /**
     * Write a log entry record to a file.
     *
     * @param fh      The file handle to use.
     * @param address The address of the entry.
     * @param entry   The LogUnitEntry to write.
     */
    private void writeRecord(FileHandle fh, long address, LogData entry) throws IOException {
        LogEntry logEntry = getLogEntry(address, entry);

        ByteBuffer record = getByteBufferWithMetaData(logEntry);


        ByteBuffer recordBuf = ByteBuffer.allocate(Short.BYTES // Delimiter
                + record.capacity());

        recordBuf.putShort(RECORD_DELIMITER);
        recordBuf.put(record.array());
        recordBuf.flip();

        synchronized (fh.lock) {
            fh.channel.write(recordBuf);
            channelsToSync.add(fh.channel);
        }
    }

    @Override
    public void append(LogAddress logAddress, LogData entry) {
        //evict the data by getting the next pointer.
        try {
            // make sure the entry doesn't currently exist...
            // (probably need a faster way to do this - high watermark?)
            FileHandle fh = getFileHandleForAddress(logAddress);
            if (!fh.getKnownAddresses().contains(logAddress.address)) {
                writeRecord(fh, logAddress.address, entry);
                fh.getKnownAddresses().add(logAddress.address);
            } else {
                throw new OverwriteException();
            }
            log.trace("Disk_write[{}]: Written to disk.", logAddress);
        } catch (IOException e) {
            log.error("Disk_write[{}]: Exception", logAddress, e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public LogData read(LogAddress logAddress) {
        try {
            return readRecord(getFileHandleForAddress(logAddress), logAddress.address);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Data
    class FileHandle {
        @NonNull
        private FileChannel channel;
        @NonNull
        private String fileName;
        private Set<Long> knownAddresses = Collections.newSetFromMap(new ConcurrentHashMap<>());
        private final Lock lock = new ReentrantLock();
    }

    @Override
    public void close() {
        for (FileHandle fh : writeChannels.values()) {
            try {
                fh.getChannel().force(true);
                fh.getChannel().close();
                fh.channel = null;
                fh.knownAddresses = null;
            } catch (IOException e) {
                log.warn("Error closing fh {}: {}", fh.toString(), e.toString());
            }
        }

        writeChannels = new HashMap<>();
    }

    @Override
    public void release(LogAddress logAddress, LogData entry) {
    }

    @VisibleForTesting
    Set<FileChannel> getChannelsToSync() {
        return channelsToSync;
    }
}
