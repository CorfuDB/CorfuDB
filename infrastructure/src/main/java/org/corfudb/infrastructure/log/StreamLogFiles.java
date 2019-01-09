package org.corfudb.infrastructure.log;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.corfudb.format.Types;
import org.corfudb.format.Types.LogEntry;
import org.corfudb.format.Types.LogHeader;
import org.corfudb.format.Types.Metadata;
import org.corfudb.format.Types.TrimEntry;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.corfudb.infrastructure.utils.Persistence.syncDirectory;


/**
 * This class implements the StreamLog by persisting the stream log as records in multiple files.
 * This StreamLog implementation can detect log file corruption, if checksum is enabled, otherwise
 * the checksum field will be ignored.
 *
 * <p>Created by maithem on 10/28/16.
 */

@Slf4j
public class StreamLogFiles implements StreamLog, StreamLogWithRankedAddressSpace {

    public static final int METADATA_SIZE = Metadata.newBuilder()
            .setLengthChecksum(-1)
            .setPayloadChecksum(-1)
            .setLength(-1)
            .build()
            .getSerializedSize();
    public static int VERSION = 2;
    public static int RECORDS_PER_LOG_FILE = 10000;
    public static int TRIM_THRESHOLD = (int) (.25 * RECORDS_PER_LOG_FILE);
    public final String logDir;
    private final boolean noVerify;
    private final ServerContext serverContext;
    private Map<String, SegmentHandle> writeChannels;
    private Set<FileChannel> channelsToSync;
    private MultiReadWriteLock segmentLocks = new MultiReadWriteLock();

    //=================Log Metadata=================
    // TODO(Maithem) this should effectively be final, but it is used
    // by a reset API that clears the state of this class, on reset
    // a new instance of this class should be created after deleting
    // the files of the old instance
    private LogMetadata logMetadata;
    private long lastSegment;
    private volatile long startingAddress;


    /**
     * Returns a file-based stream log object.
     * @param serverContext  Context object that provides server state such as epoch,
     *                       segment and start address
     * @param noVerify       Disable checksum if true
     */
    public StreamLogFiles(ServerContext serverContext, boolean noVerify) {
        logDir = serverContext.getServerConfig().get("--log-path") + File.separator + "log";
        File dir = new File(logDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        writeChannels = new ConcurrentHashMap();
        channelsToSync = new HashSet<>();
        this.noVerify = noVerify;
        this.serverContext = serverContext;
        verifyLogs();
        // Starting address initialization should happen before
        // initializing the tail segment (i.e. initializeMaxGlobalAddress)
        startingAddress = serverContext.getStartingAddress();
        long firstSegment = startingAddress / RECORDS_PER_LOG_FILE;
        lastSegment = serverContext.getTailSegment();
        logMetadata = initializeLogMetadata(firstSegment, lastSegment);

        // This can happen if a prefix trim happens on
        // addresses that haven't been written
        if (Math.max(logMetadata.getGlobalTail(), 0L) < getTrimMark()) {
            syncTailSegment(getTrimMark() - 1);
        }
    }

    /**
     * This method will scan the log (i.e. read all log segment files)
     * on this LU and create a map of stream offsets and the global
     * addresses seen.
     * @param startSegment first segment in the log to be scanned
     * @param endSegment the last segment to be scanned
     * @return LogMetadata constructed from all the addresses in the
     * consecutive segments from [startSegment, endSegment]
     */
    private LogMetadata initializeLogMetadata(long startSegment, long endSegment) {
        LogMetadata metadata = new LogMetadata();
        long start = System.currentTimeMillis();
        for (long currentSegment = startSegment; currentSegment <= endSegment; currentSegment++) {
            // TODO(Maithem): factor out getSegmentHandleForAddress to allow getting
            // segments by segment number
            SegmentHandle sh = getSegmentHandleForAddress(currentSegment * RECORDS_PER_LOG_FILE + 1);
            try {
                for (Map.Entry<Long, AddressMetaData> record : sh.getKnownAddresses().entrySet()) {
                    // skip trimmed entries
                    if (record.getKey() < startingAddress) continue;
                    LogData logEntry = read(record.getKey());
                    metadata.update(logEntry);
                }
            } finally {
                sh.close();
            }
        }

        // Open segment will add entries to the writeChannels map, therefore we need to clear it
        writeChannels.clear();
        long end = System.currentTimeMillis();
        log.info("initializeStreamTails: took {} ms to load {}", end - start, metadata);
        return metadata;
    }

    /**
     * Write the header for a Corfu log file.
     *
     * @param fc      The file channel to use.
     * @param version The version number to append to the header.
     * @param verify  Checksum verify flag
     * @throws IOException I/O exception
     */
    public static void writeHeader(FileChannel fc, int version, boolean verify)
            throws IOException {

        LogHeader header = LogHeader.newBuilder()
                .setVersion(version)
                .setVerifyChecksum(verify)
                .build();

        ByteBuffer buf = getByteBufferWithMetaData(header);
        safeWrite(fc, buf);
        fc.force(true);
    }

    public static Metadata getMetadata(AbstractMessage message) {
        return Metadata.newBuilder()
                .setPayloadChecksum(getChecksum(message.toByteArray()))
                .setLengthChecksum(getChecksum(message.getSerializedSize()))
                .setLength(message.getSerializedSize())
                .build();
    }

    public static ByteBuffer getByteBuffer(Metadata metadata, AbstractMessage message) {
        ByteBuffer buf = ByteBuffer.allocate(metadata.getSerializedSize()
                + message.getSerializedSize());
        buf.put(metadata.toByteArray());
        buf.put(message.toByteArray());
        buf.flip();
        return buf;
    }

    public static ByteBuffer getByteBufferWithMetaData(AbstractMessage message) {
        Metadata metadata = getMetadata(message);

        ByteBuffer buf = ByteBuffer.allocate(metadata.getSerializedSize()
                + message.getSerializedSize());
        buf.put(metadata.toByteArray());
        buf.put(message.toByteArray());
        buf.flip();
        return buf;
    }

    /**
     * Returns checksum used for log.
     * @param bytes  data over which to compute the checksum
     * @return       checksum of bytes
     */
    public static int getChecksum(byte[] bytes) {
        Hasher hasher = Hashing.crc32c().newHasher();
        for (byte a : bytes) {
            hasher.putByte(a);
        }

        return hasher.hash().asInt();
    }

    static int getChecksum(long num) {
        Hasher hasher = Hashing.crc32c().newHasher();
        return hasher.putLong(num).hash().asInt();
    }

    static int getChecksum(int num) {
        Hasher hasher = Hashing.crc32c().newHasher();
        return hasher.putInt(num).hash().asInt();
    }

    @Override
    public TailsResponse getTails() {
        Map<UUID, Long> tails = new HashMap<>(logMetadata.getStreamTails().size());

        for (Map.Entry<UUID, Long> entry : logMetadata.getStreamTails().entrySet()) {
            tails.put(entry.getKey(), entry.getValue());
        }
        return new TailsResponse(logMetadata.getGlobalTail(), tails);
    }

    private void syncTailSegment(long address) {
        // TODO(Maithem) since writing a record and setting the tail segment is not
        // an atomic operation, it is possible to set an incorrect tail segment. In
        // that case we will need to scan more than one segment
        logMetadata.updateGlobalTail(address);
        long segment = address / RECORDS_PER_LOG_FILE;
        if (lastSegment < segment) {
            serverContext.setTailSegment(segment);
            lastSegment = segment;
        }
    }

    @Override
    public void prefixTrim(long address) {
        if (address < startingAddress) {
            log.warn("prefixTrim: Ignoring repeated trim {}", address);
        } else {
            // TODO(Maithem): Although this operation is persisted to disk,
            // the startingAddress can be lost even after the method has completed.
            // This is due to the fact that updates on the local datastore don't
            // expose disk sync functionalty.
            long newStartingAddress = address + 1;
            serverContext.setStartingAddress(newStartingAddress);
            startingAddress = newStartingAddress;
            syncTailSegment(address);
            log.debug("Trimmed prefix, new starting address {}", newStartingAddress);
        }
    }

    private boolean isTrimmed(long address) {
        if (address < startingAddress) {
            return true;
        }
        return false;
    }

    private void verifyLogs() {
        String[] extension = {"log"};
        File dir = new File(logDir);

        if (dir.exists()) {
            Collection<File> files = FileUtils.listFiles(dir, extension, true);

            for (File file : files) {
                LogHeader header;

                try (FileChannel fc = FileChannel.open(file.toPath())) {
                    header = parseHeader(fc);
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }

                if (header == null) {
                    log.warn("verifyLogs: Ignoring partially written header in {}", file.getAbsoluteFile());
                    continue;
                }

                if (header.getVersion() != VERSION) {
                    String msg = String.format("Log version %s for %s should match the logunit log version %s",
                            header.getVersion(), file.getAbsoluteFile(), VERSION);
                    throw new RuntimeException(msg);
                }

                if (!noVerify && !header.getVerifyChecksum()) {
                    String msg = String.format("Log file %s not generated with checksums, can't verify!",
                            file.getAbsoluteFile());
                    throw new RuntimeException(msg);
                }
            }
        }
    }

    @Override
    public void sync(boolean force) throws IOException {
        if (force) {
            for (FileChannel ch : channelsToSync) {
                ch.force(true);
            }
        }
        log.debug("Sync'd {} channels", channelsToSync.size());
        channelsToSync.clear();
    }

    @Override
    public synchronized void compact() {
        trimPrefix();
    }

    @Override
    public long getTrimMark() {
        return startingAddress;
    }

    private void trimPrefix() {
        // Trim all segments up till the segment that contains the starting address
        // (i.e. trim only complete segments)
        long endSegment = (startingAddress / RECORDS_PER_LOG_FILE) - 1;

        if (endSegment <= 0) {
            log.debug("Only one segment detected, ignoring trim");
            return;
        }

        // Close segments before deleting their corresponding log files
        closeSegmentHandlers(endSegment);

        deleteFilesMatchingFilter(file -> {
            try {
                String segmentStr = file.getName().split("\\.")[0];
                return Long.parseLong(segmentStr) < endSegment;
            } catch (Exception e) {
                log.warn("trimPrefix: ignoring file {}", file.getName());
                return false;
            }
        });

        log.info("trimPrefix: completed, end segment {}", endSegment);
    }

    private LogData getLogData(LogEntry entry) {
        ByteBuf data = Unpooled.wrappedBuffer(entry.getData().toByteArray());
        LogData logData = new LogData(org.corfudb.protocols.wireprotocol
                .DataType.typeMap.get((byte) entry.getDataType().getNumber()), data);

        logData.setBackpointerMap(getUUIDLongMap(entry.getBackpointersMap()));
        logData.setGlobalAddress(entry.getGlobalAddress());
        logData.setRank(createDataRank(entry));

        if (entry.hasThreadId()) {
            logData.setThreadId(entry.getThreadId());
        }
        if (entry.hasClientIdLeastSignificant() && entry.hasClientIdMostSignificant()){
            long lsd = entry.getClientIdLeastSignificant();
            long msd = entry.getClientIdMostSignificant();
            logData.setClientId(new UUID(msd, lsd));
        }

        if (entry.hasCheckpointEntryType()) {
            logData.setCheckpointType(CheckpointEntry.CheckpointEntryType
                    .typeMap.get((byte) entry.getCheckpointEntryType().ordinal()));

            if (!entry.hasCheckpointIdLeastSignificant()
                    || !entry.hasCheckpointIdMostSignificant()) {
                log.error("Checkpoint has missing information {}", entry);
            }

            long lsd = entry.getCheckpointIdLeastSignificant();
            long msd = entry.getCheckpointIdMostSignificant();
            UUID checkpointId = new UUID(msd, lsd);

            logData.setCheckpointId(checkpointId);

            lsd = entry.getCheckpointedStreamIdLeastSignificant();
            msd = entry.getCheckpointedStreamIdMostSignificant();
            UUID streamId = new UUID(msd, lsd);

            logData.setCheckpointedStreamId(streamId);

            logData.setCheckpointedStreamStartLogAddress(
                    entry.getCheckpointedStreamStartLogAddress());
        }

        return logData;
    }

    //================Parsing Helper Methods================//

    /**
     * Parse the metadata field. This method should only be called
     * when a metadata field is expected.
     *
     * @param ch the channel to read from
     * @return metadata field of null if it was partially written.
     * @throws IOException
     */
    private Metadata parseMetadata(FileChannel ch) throws IOException {
        if (ch.size() - ch.position() < METADATA_SIZE) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(METADATA_SIZE);
        ch.read(buf);
        buf.flip();

        Metadata metadata;

        try {
            metadata = Metadata.parseFrom(buf.array());
        } catch (InvalidProtocolBufferException e) {
            throw new DataCorruptionException();
        }

        if (metadata.getLengthChecksum() != getChecksum(metadata.getLength())) {
            throw new DataCorruptionException();
        }

        return metadata;
    }

    /**
     * Read a payload given metadata.
     *
     * @param ch channel to read the payload from
     * @param metadata the metadata that is written before the payload
     * @return ByteBuffer for the payload
     * @throws IOException
     */
    private ByteBuffer getPayloadForMetadata(FileChannel ch, Metadata metadata) throws IOException {
        if (ch.size() - ch.position() < metadata.getLength()) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(metadata.getLength());
        ch.read(buf);
        buf.flip();
        return buf;
    }

    /**
     *
     * Parse the logfile header, or create it, or recreate it if it was
     * partially written.
     *
     * @param ch
     * @return
     * @throws IOException
     */
    private LogHeader parseHeader(FileChannel ch) throws IOException {
        Metadata metadata = parseMetadata(ch);
        if (metadata == null) {
            // Partial write on the metadata for the header
            // Rewind the channel position to the begining of the file
            ch.position(0);
            return null;
        }

        ByteBuffer buffer = getPayloadForMetadata(ch, metadata);
        if (buffer == null) {
            // partial write on the header payload
            // Rewind the channel position to the begining of the file
            ch.position(0);
            return null;
        }

        if (getChecksum(buffer.array()) != metadata.getPayloadChecksum()) {
            throw new DataCorruptionException();
        }

        LogHeader header;

        try {
            header = LogHeader.parseFrom(buffer.array());
        } catch (InvalidProtocolBufferException e) {
            throw new DataCorruptionException();
        }

        return header;
    }

    /**
     * Parse an entry.
     *
     * @param ch
     * @param metadata
     * @return
     * @throws IOException
     */
    private LogEntry parseEntry(FileChannel ch, Metadata metadata) throws IOException {
        if (metadata == null) {
            // The metadata for this entry was partial written
            return null;
        }

        ByteBuffer buffer = getPayloadForMetadata(ch, metadata);
        if (buffer == null) {
            // partial write on the entry
            // rewind the channel position to point before
            // the metadata field for this partially written payload
            ch.position(ch.position() - METADATA_SIZE);
            return null;
        }

        if (!noVerify) {
            if (metadata.getPayloadChecksum() != getChecksum(buffer.array())) {
                log.error("Checksum mismatch detected while trying to read file {}",
                        ch);
                throw new DataCorruptionException();
            }
        }


        LogEntry entry;
        try {
            entry = LogEntry.parseFrom(buffer.array());
        } catch (InvalidProtocolBufferException e) {
            throw new DataCorruptionException();
        }
        return entry;
    }

    /**
     * Reads an address space from a log file into a SegmentHandle.
     *
     * @param sh  Object containing state for the segment to be read
     */
    private void readAddressSpace(SegmentHandle sh) throws IOException {
        FileChannel fc = sh.getWriteChannel();
        fc.position(0);

        LogHeader header = parseHeader(fc);
        if (header == null) {
            log.warn("Couldn't find log header for {}, creating new header.", sh.getFileName());
            boolean verify = true;
            if (noVerify) {
                verify = false;
            }
            writeHeader(fc, VERSION, verify);
            return;
        }

        while (fc.size() - fc.position() > 0) {
            long channelOffset = fc.position();
            Metadata metadata = parseMetadata(fc);
            LogEntry entry = parseEntry(fc, metadata);

            if (entry == null) {
                // Metadata or Entry were partially written
                log.warn("Malformed entry, metadata {} in file {}", metadata, sh.getFileName());

                // Note that after rewinding the channel pointer, it is important to truncate
                // any bytes that were written. This is required to avoid an ambigous case
                // where a subsequent write (after a failed write) succeeds but writes less
                // bytes than the partially written buffer. In that case, the log unit can't
                // determine if the bytes correspund to a partially written buffer that needs
                // to be ignored, or if the bytes corrrespond to a corrupted metadata field.
                fc.truncate(fc.position());
                fc.force(true);
                return;
            }

            sh.getKnownAddresses().put(entry.getGlobalAddress(),
                    new AddressMetaData(metadata.getPayloadChecksum(),
                            metadata.getLength(), channelOffset + METADATA_SIZE));
        }
    }

    /**
     * Read a log entry in a file.
     *
     * @param sh      The file handle to use.
     * @param address The address of the entry.
     * @return The log unit entry at that address, or NULL if there was no entry.
     */
    private LogData  readRecord(SegmentHandle sh, long address)
            throws IOException {
        FileChannel fc = sh.getReadChannel();

        AddressMetaData metaData = sh.getKnownAddresses().get(address);
        if (metaData == null) {
            return null;
        }

        try {
            ByteBuffer entryBuf = ByteBuffer.allocate(metaData.length);
            fc.read(entryBuf, metaData.offset);
            return getLogData(LogEntry.parseFrom(entryBuf.array()));
        } catch (InvalidProtocolBufferException e) {
            throw new DataCorruptionException();
        }
    }

    private @Nullable FileChannel getChannel(String filePath, boolean readOnly) throws IOException {
        try {

            if (readOnly) {
                if (!new File(filePath).exists()) {
                    throw new FileNotFoundException();
                } else {
                    return FileChannel.open(FileSystems.getDefault().getPath(filePath),
                            EnumSet.of(StandardOpenOption.READ));
                }
            } else {

                try {
                    EnumSet<StandardOpenOption> options = EnumSet.of(
                            StandardOpenOption.READ, StandardOpenOption.WRITE,
                            StandardOpenOption.CREATE_NEW, StandardOpenOption.SPARSE
                    );
                    FileChannel channel = FileChannel.open(FileSystems.getDefault().getPath(filePath), options);

                    // First time creating this segment file, need to sync the parent directory
                    File segFile = new File(filePath);
                    syncDirectory(segFile.getParent());
                    return channel;
                } catch (FileAlreadyExistsException ex) {
                    return FileChannel.open(FileSystems.getDefault().getPath(filePath),
                            EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE));
                }
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
     * @param address The address to open.
     * @return The FileChannel for that address.
     */
    @VisibleForTesting
    synchronized SegmentHandle getSegmentHandleForAddress(long address) {
        String filePath = logDir + File.separator;
        long segment = address / RECORDS_PER_LOG_FILE;
        filePath += segment;
        filePath += ".log";

        SegmentHandle handle = writeChannels.computeIfAbsent(filePath, a -> {
            FileChannel writeCh = null;
            FileChannel readCh = null;

            try {
                boolean verify = true;
                if (noVerify) {
                    verify = false;
                }

                writeCh = getChannel(a, false);
                readCh = getChannel(a, true);

                SegmentHandle sh = new SegmentHandle(segment, writeCh, readCh, a);
                // The first time we open a file we should read to the end, to load the
                // map of entries we already have.
                // Once the segment address space is loaded, it should be ready to accept writes.
                readAddressSpace(sh);
                return sh;
            } catch (IOException e) {
                log.error("Error opening file {}", a, e);
                IOUtils.closeQuietly(writeCh);
                IOUtils.closeQuietly(readCh);
                throw new RuntimeException(e);
            }
        });

        handle.retain();
        return handle;
    }

    Map<String, Long> getStrLongMap(Map<UUID, Long> uuidLongMap) {
        Map<String, Long> stringLongMap = new HashMap();

        for (Map.Entry<UUID, Long> entry : uuidLongMap.entrySet()) {
            stringLongMap.put(entry.getKey().toString(), entry.getValue());
        }

        return stringLongMap;
    }

    @Deprecated // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviationaswordinname")  // Due to deprecation
    Map<UUID, Long> getUUIDLongMap(Map<String, Long> stringLongMap) {
        Map<UUID, Long> uuidLongMap = new HashMap();

        for (Map.Entry<String, Long> entry : stringLongMap.entrySet()) {
            uuidLongMap.put(UUID.fromString(entry.getKey()), entry.getValue());
        }

        return uuidLongMap;
    }

    @Deprecated  // TODO: Add replacement method that conforms to style
    @SuppressWarnings("checkstyle:abbreviationaswordinname") // Due to deprecation
    Set<String> getStrUUID(Set<UUID> uuids) {
        Set<String> strUUIds = new HashSet();

        for (UUID uuid : uuids) {
            strUUIds.add(uuid.toString());
        }

        return strUUIds;
    }

    LogEntry getLogEntry(long address, LogData entry) {
        byte[] data = new byte[0];

        if (entry.getData() != null) {
            data = entry.getData();
        }

        LogEntry.Builder logEntryBuilder = LogEntry.newBuilder()
                .setDataType(Types.DataType.forNumber(entry.getType().ordinal()))
                .setData(ByteString.copyFrom(data))
                .setGlobalAddress(address)
                .addAllStreams(getStrUUID(entry.getStreams()))
                .putAllBackpointers(getStrLongMap(entry.getBackpointerMap()));

        Optional<Types.DataRank> rank = createProtobufsDataRank(entry);
        if (rank.isPresent()) {
            logEntryBuilder.setRank(rank.get());
        }

        if (entry.getClientId() != null && entry.getThreadId() != null) {
            logEntryBuilder.setClientIdMostSignificant(
                    entry.getClientId().getMostSignificantBits());
            logEntryBuilder.setClientIdLeastSignificant(
                    entry.getClientId().getLeastSignificantBits());
            logEntryBuilder.setThreadId(entry.getThreadId());
        }

        if (entry.hasCheckpointMetadata()) {
            logEntryBuilder.setCheckpointEntryType(
                    Types.CheckpointEntryType.forNumber(
                            entry.getCheckpointType().ordinal()));
            logEntryBuilder.setCheckpointIdMostSignificant(
                    entry.getCheckpointId().getMostSignificantBits());
            logEntryBuilder.setCheckpointIdLeastSignificant(
                    entry.getCheckpointId().getLeastSignificantBits());
            logEntryBuilder.setCheckpointedStreamIdLeastSignificant(
                    entry.getCheckpointedStreamId().getLeastSignificantBits());
            logEntryBuilder.setCheckpointedStreamIdMostSignificant(
                    entry.getCheckpointedStreamId().getMostSignificantBits());
            logEntryBuilder.setCheckpointedStreamStartLogAddress(
                    entry.getCheckpointedStreamStartLogAddress());
        }

        return logEntryBuilder.build();
    }

    private Optional<Types.DataRank> createProtobufsDataRank(IMetadata entry) {
        IMetadata.DataRank rank = entry.getRank();
        if (rank == null) {
            return Optional.empty();
        }
        Types.DataRank result = Types.DataRank.newBuilder()
                .setRank(rank.getRank())
                .setUuidLeastSignificant(rank.getUuid().getLeastSignificantBits())
                .setUuidMostSignificant(rank.getUuid().getMostSignificantBits())
                .build();
        return Optional.of(result);
    }

    private @Nullable IMetadata.DataRank createDataRank(LogEntry entity) {
        if (!entity.hasRank()) {
            return null;
        }
        Types.DataRank rank = entity.getRank();
        return new IMetadata.DataRank(rank.getRank(),
                new UUID(rank.getUuidMostSignificant(), rank.getUuidLeastSignificant()));
    }

    /**
     * Write a list of LogData entries to the log file.
     * @param sh segment handle to the logfile
     * @param entries list of LogData entries to write.
     * @return A map of AddressMetaData for the written records
     * @throws IOException
     */
    private Map<Long, AddressMetaData> writeRecords(SegmentHandle sh,
                                             List<LogData> entries) throws IOException {
        Map<Long, AddressMetaData> recordsMap = new HashMap<>();

        List<ByteBuffer> entryBuffs = new ArrayList<>();
        int totalBytes = 0;

        List<Metadata> metadataList = new ArrayList<>();

        for (int ind = 0; ind < entries.size(); ind++) {
            LogData curr = entries.get(ind);
            LogEntry logEntry = getLogEntry(curr.getGlobalAddress(), curr);
            Metadata metadata = getMetadata(logEntry);
            metadataList.add(metadata);
            ByteBuffer record = getByteBuffer(metadata, logEntry);
            totalBytes += record.limit();
            entryBuffs.add(record);
        }

        ByteBuffer allRecordsBuf = ByteBuffer.allocate(totalBytes);

        try (MultiReadWriteLock.AutoCloseableLock ignored =
                     segmentLocks.acquireWriteLock(sh.getSegment())) {
            for (int ind = 0; ind < entryBuffs.size(); ind++) {
                long channelOffset = sh.getWriteChannel().position()
                        + allRecordsBuf.position() + METADATA_SIZE;
                allRecordsBuf.put(entryBuffs.get(ind));
                Metadata metadata = metadataList.get(ind);
                recordsMap.put(entries.get(ind).getGlobalAddress(),
                        new AddressMetaData(metadata.getPayloadChecksum(),
                                metadata.getLength(), channelOffset));
            }

            allRecordsBuf.flip();
            safeWrite(sh.getWriteChannel(), allRecordsBuf);
            channelsToSync.add(sh.getWriteChannel());
            // Sync the global and stream tail(s)
            // TODO(Maithem): on ioexceptions the StreamLogFiles needs to be reinitialized
            syncTailSegment(entries.get(entries.size() - 1).getGlobalAddress());
            logMetadata.update(entries);
        }

        return recordsMap;
    }

    /**
     *
     * Attempts to write a buffer to a file channel, if write fails with an
     * IOException then the channel pointer is moved back to its original positon
     * before the write
     *
     * @param channel the channel to write to
     * @param buf  the buffer to write
     * @throws IOException
     */
    private static void safeWrite(FileChannel channel, ByteBuffer buf) throws IOException {
        long prev = channel.position();
        try {
            channel.write(buf);
        } catch (IOException e) {
            // Write failed restore the channels position, so the subsequent writes
            // can overwrite the failed write.

            // Note that after rewinding the channel pointer, it is important to truncate
            // any bytes that were written. This is required to avoid an ambiguous case
            // where a subsequent write (after a failed write) succeeds but writes less
            // bytes than the partially written buffer. In that case, the log unit can't
            // determine if the bytes correspond to a partially written buffer that needs
            // to be ignored, or if the bytes correspond to a corrupted metadata field.
            channel.position(prev);
            channel.truncate(prev);
            channel.force(true);
            throw e;
        }
    }

    /**
     * Write a log entry record to a file.
     *
     * @param fh      The file handle to use.
     * @param address The address of the entry.
     * @param entry   The LogData to append.
     * @return Returns metadata for the written record
     */
    private AddressMetaData writeRecord(SegmentHandle fh, long address,
                                        LogData entry) throws IOException {
        LogEntry logEntry = getLogEntry(address, entry);
        Metadata metadata = getMetadata(logEntry);

        ByteBuffer record = getByteBuffer(metadata, logEntry);
        long channelOffset;

        try (MultiReadWriteLock.AutoCloseableLock ignored =
                     segmentLocks.acquireWriteLock(fh.getSegment())) {
            channelOffset = fh.getWriteChannel().position() + METADATA_SIZE;
            safeWrite(fh.getWriteChannel(), record);
            channelsToSync.add(fh.getWriteChannel());
            syncTailSegment(address);
            logMetadata.update(entry);
        }

        return new AddressMetaData(metadata.getPayloadChecksum(), metadata.getLength(), channelOffset);
    }

    long getSegment(LogData entry) {
        return entry.getGlobalAddress() / RECORDS_PER_LOG_FILE;
    }

    /**
     * Pre-process a range of entries to be written. This includes
     * removing trimmed entries and advancing the trim mark appropriately.
     * @param range range of entries
     * @return A subset of range; entries to be written
     */
    List<LogData> preprocess(List<LogData> range) {
        List<LogData> processed  = new ArrayList<>();
        for (int x = 0; x < range.size(); x++) {
            // TODO(Maithem) Add an extra check to make
            // sure that trimmed entres don't alternate
            // with non-trimmed entries
            LogData curr = range.get(x);
            if (curr.isTrimmed()) {
                // We don't need to write trimmed entries
                // because we already track the trim mark
                prefixTrim(curr.getGlobalAddress());
                continue;
            } else if (isTrimmed(curr.getGlobalAddress())) {
                // This is the case where the client tries to
                // write a range of addresses, but before the
                // request starts, a prefix trim request executes
                // on this logging unit, as a consequence trimming
                // all of, or part of the range.
                continue;
            }
            else {
                processed.add(curr);
            }
        }
        return processed;
    }

    /**
     * This method verifies that a range of entries doesn't
     * span more than two segments and that the log addresses
     * are ordered sequentially.
     * @param range entries to verify
     * @return return true if the range is valid.
     */
    boolean verify(List<LogData> range) {

        // Make sure that entries are ordered sequentially
        long firstAddress = range.get(0).getGlobalAddress();
        for (int x = 1; x < range.size(); x++) {
            if (range.get(x).getGlobalAddress() !=
                    firstAddress + x) {
                return false;
            }
        }

        // Check if the range spans more than two segments
        long lastAddress = range.get(range.size() - 1).getGlobalAddress();
        long firstSegment = firstAddress / RECORDS_PER_LOG_FILE;
        long endSegment = lastAddress / RECORDS_PER_LOG_FILE;

        if (endSegment - firstSegment > 1) {
            return false;
        }

        return true;
    }

    @Override
    public void append(List<LogData> range) {
        // Remove trimmed entries
        List<LogData> entries = preprocess(range);

        if (entries.isEmpty()) {
            log.info("No entries to write.");
            return;
        }

        if (!verify(entries)) {
            // Range overlaps more than two segments
            throw new IllegalArgumentException("Write range too large!");
        }

        // check if the entries range cross a segment
        LogData first = entries.get(0);
        LogData last = entries.get(entries.size() - 1);
        SegmentHandle firstSh = getSegmentHandleForAddress(first.getGlobalAddress());
        SegmentHandle lastSh = getSegmentHandleForAddress(last.getGlobalAddress());

        List<LogData> segOneEntries = new ArrayList<>();
        List<LogData> segTwoEntries = new ArrayList<>();

        for (int ind = 0; ind < entries.size(); ind++) {
            LogData curr = entries.get(ind);

            if (getSegment(curr) == firstSh.getSegment() &&
                    !firstSh.getKnownAddresses().containsKey(curr.getGlobalAddress())) {
                segOneEntries.add(curr);
            } else if (getSegment(curr) == lastSh.getSegment() &&
                    !lastSh.getKnownAddresses().containsKey(curr.getGlobalAddress())) {
                segTwoEntries.add(curr);
            }
        }

        try {
            if (!segOneEntries.isEmpty()) {
                Map<Long, AddressMetaData> firstSegAddresses = writeRecords(firstSh, segOneEntries);
                firstSh.getKnownAddresses().putAll(firstSegAddresses);
            }

            if (!segTwoEntries.isEmpty()) {
                Map<Long, AddressMetaData> lastSegAddresses = writeRecords(lastSh, segTwoEntries);
                lastSh.getKnownAddresses().putAll(lastSegAddresses);
            }
        } catch (IOException e) {
            log.error("Disk_write[{}-{}]: Exception", first.getGlobalAddress(),
                    last.getGlobalAddress(), e);
            throw new RuntimeException(e);
        } finally {
            firstSh.release();
            lastSh.release();
        }
    }

    @Override
    public void append(long address, LogData entry) {
        if(isTrimmed(address)) {
            throw new OverwriteException(OverwriteCause.TRIM);
        }

        SegmentHandle fh = getSegmentHandleForAddress(address);

        try {
            // make sure the entry doesn't currently exist...
            // (probably need a faster way to do this - high watermark?)
            if (fh.getKnownAddresses().containsKey(address)
                    || fh.getTrimmedAddresses().contains(address)) {
                if (entry.getRank() == null) {
                    OverwriteCause overwriteCause = getOverwriteCauseForAddress(address, entry);
                    log.trace("Disk_write[{}]: overwritten exception, cause: {}", address, overwriteCause);
                    throw new OverwriteException(overwriteCause);
                } else {
                    // the method below might throw DataOutrankedException or ValueAdoptedException
                    assertAppendPermittedUnsafe(address, entry);
                    AddressMetaData addressMetaData = writeRecord(fh, address, entry);
                    fh.getKnownAddresses().put(address, addressMetaData);
                }
            } else {
                AddressMetaData addressMetaData = writeRecord(fh, address, entry);
                fh.getKnownAddresses().put(address, addressMetaData);
            }
            log.trace("Disk_write[{}]: Written to disk.", address);
        } catch (IOException e) {
            log.error("Disk_write[{}]: Exception", address, e);
            throw new RuntimeException(e);
        } finally {
            fh.release();
        }
    }

    @Override
    public LogData read(long address) {
        if (isTrimmed(address)) {
            return LogData.getTrimmed(address);
        }
        SegmentHandle sh = getSegmentHandleForAddress(address);

        try {
            if (sh.getPendingTrims().contains(address)) {
                return LogData.getTrimmed(address);
            }
            return readRecord(sh, address);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            sh.release();
        }
    }

    @Override
    public void close() {
        for (SegmentHandle fh : writeChannels.values()) {
            fh.close();
        }

        writeChannels = new HashMap<>();
    }

    @Override
    public void release(long address, LogData entry) {
    }

    /**
     * Closes all segment handlers up to and including the handler for the endSegment.
     *
     * @param endSegment The segment index of the last segment up to (including) the end segment.
     */
    private void closeSegmentHandlers(long endSegment) {
        for (SegmentHandle sh : writeChannels.values()) {
            if (sh.getSegment() <= endSegment) {
                if (sh.getRefCount() != 0) {
                    log.warn("closeSegmentHandlers: Segment {} is trimmed, but refCount is {},"
                                    + " attempting to trim anyways", sh.getSegment(),
                            sh.getRefCount());
                }
                sh.close();
                writeChannels.remove(sh.getFileName());
            }
        }
    }

    /**
     * Deletes all files matching the given filter.
     *
     * @param fileFilter File filter to delete files.
     */
    private void deleteFilesMatchingFilter(FileFilter fileFilter) {
        int numFiles = 0;
        long freedBytes = 0;
        File dir = new File(logDir);
        File[] files = dir.listFiles(fileFilter);
        for (File file : files) {
            long delta = file.length();

            if (!file.delete()) {
                log.error("deleteFilesMatchingFilter: Couldn't delete file {}", file.getName());
            } else {
                freedBytes += delta;
                numFiles++;
            }
        }
        log.info("deleteFilesMatchingFilter: completed, deleted {} files, freed {} bytes",
                numFiles, freedBytes);
    }

    /**
     * TODO(Maithem) remove this method. Obtaining a new instance should happen
     * through instantiation not by clearing this class' state
     *
     * Resets the Stream log.
     * Clears all data and resets the handlers.
     * Usage: To heal a recovering node, we require to wipe off existing data.
     */
    @Override
    public void reset() {
        // Trim all segments
        long endSegment = (Math.max(logMetadata.getGlobalTail(), 0L) / RECORDS_PER_LOG_FILE);
        log.warn("Global Tail:{}, endSegment={}", logMetadata.getGlobalTail(), endSegment);

        // Close segments before deleting their corresponding log files
        closeSegmentHandlers(endSegment);

        deleteFilesMatchingFilter(file -> {
            try {
                String segmentStr = file.getName().split("\\.")[0];
                return Long.parseLong(segmentStr) <= endSegment;
            } catch (Exception e) {
                log.warn("reset: ignoring file {}", file.getName());
                return false;
            }
        });

        serverContext.setStartingAddress(0L);
        serverContext.setTailSegment(0L);
        startingAddress = 0L;
        lastSegment = 0L;
        logMetadata = new LogMetadata();
        writeChannels.clear();
        log.info("reset: Completed, end segment {}", endSegment);
    }

    @VisibleForTesting
    Set<FileChannel> getChannelsToSync() {
        return channelsToSync;
    }

    @VisibleForTesting
    Collection<SegmentHandle> getSegmentHandles() {
        return writeChannels.values();
    }
}
