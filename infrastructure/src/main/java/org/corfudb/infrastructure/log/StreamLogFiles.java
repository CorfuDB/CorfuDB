package org.corfudb.infrastructure.log;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.FileUtils;
import org.corfudb.format.Types;
import org.corfudb.format.Types.LogEntry;
import org.corfudb.format.Types.LogHeader;
import org.corfudb.format.Types.Metadata;
import org.corfudb.format.Types.TrimEntry;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OverwriteException;


/**
 * This class implements the StreamLog by persisting the stream log as records in multiple files.
 * This StreamLog implementation can detect log file corruption, if checksum is enabled, otherwise
 * the checksum field will be ignored.
 *
 * <p>Created by maithem on 10/28/16.
 */

@Slf4j
public class StreamLogFiles implements StreamLog, StreamLogWithRankedAddressSpace {

    public static final short RECORD_DELIMITER = 0x4C45;
    public static final int METADATA_SIZE = Metadata.newBuilder()
            .setChecksum(-1)
            .setLength(-1)
            .build()
            .getSerializedSize();
    public static int VERSION = 1;
    public static int RECORDS_PER_LOG_FILE = 10000;
    public static int TRIM_THRESHOLD = (int) (.25 * RECORDS_PER_LOG_FILE);
    public final String logDir;
    private final boolean noVerify;
    private final ServerContext serverContext;
    private final AtomicLong globalTail = new AtomicLong(0L);
    private Map<String, SegmentHandle> writeChannels;
    private Set<FileChannel> channelsToSync;
    private MultiReadWriteLock segmentLocks = new MultiReadWriteLock();
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
        initializeStartingAddress();
        initializeMaxGlobalAddress();

        // This can happen if a prefix trim happens on
        // addresses that haven't been written
        if (getGlobalTail() < getTrimMark()) {
            syncTailSegment(getTrimMark() - 1);
        }
    }

    public static String getPendingTrimsFilePath(String segmentPath) {
        return segmentPath + ".pending";
    }

    public static String getTrimmedFilePath(String segmentPath) {
        return segmentPath + ".trimmed";
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
        fc.write(buf);
        fc.force(true);
    }

    private static Metadata getMetadata(AbstractMessage message) {
        return Metadata.newBuilder()
                .setChecksum(getChecksum(message.toByteArray()))
                .setLength(message.getSerializedSize())
                .build();
    }

    private static ByteBuffer getByteBuffer(Metadata metadata, AbstractMessage message) {
        ByteBuffer buf = ByteBuffer.allocate(metadata.getSerializedSize()
                + message.getSerializedSize());
        buf.put(metadata.toByteArray());
        buf.put(message.toByteArray());
        buf.flip();
        return buf;
    }

    private static ByteBuffer getByteBufferWithMetaData(AbstractMessage message) {
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

    @Override
    public long getGlobalTail() {
        return globalTail.get();
    }

    private void syncTailSegment(long address) {
        // TODO(Maithem) since writing a record and setting the tail segment is not
        // an atomic operation, it is possible to set an incorrect tail segment. In
        // that case we will need to scan more than one segment
        globalTail.getAndUpdate(maxTail -> address > maxTail ? address : maxTail);
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

    private void initializeStartingAddress() {
        startingAddress = serverContext.getStartingAddress();
    }

    private void initializeMaxGlobalAddress() {
        long tailSegment = serverContext.getTailSegment();
        long addressInTailSegment = (tailSegment * RECORDS_PER_LOG_FILE) + 1;
        SegmentHandle sh = getSegmentHandleForAddress(addressInTailSegment);
        try {
            Collection<LogEntry> segmentEntries = (Collection<LogEntry>)
                    getCompactedEntries(sh.getFileName(), new HashSet()).getEntries();

            for (LogEntry entry : segmentEntries) {
                long currentAddress = entry.getGlobalAddress();
                globalTail.getAndUpdate(maxTail -> currentAddress > maxTail
                        ? currentAddress : maxTail);
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            sh.release();
        }

        lastSegment = tailSegment;
    }

    private void verifyLogs() {
        String[] extension = {"log"};
        File dir = new File(logDir);

        if (dir.exists()) {
            Collection<File> files = FileUtils.listFiles(dir, extension, true);

            for (File file : files) {
                try (FileInputStream fsIn = new FileInputStream(file)) {
                    FileChannel fc = fsIn.getChannel();


                    ByteBuffer metadataBuf = ByteBuffer.allocate(METADATA_SIZE);
                    fc.read(metadataBuf);
                    metadataBuf.flip();

                    Metadata metadata = Metadata.parseFrom(metadataBuf.array());

                    ByteBuffer headerBuf = ByteBuffer.allocate(metadata.getLength());
                    fc.read(headerBuf);
                    headerBuf.flip();

                    LogHeader header = LogHeader.parseFrom(headerBuf.array());

                    fc.close();
                    fsIn.close();

                    if (metadata.getChecksum() != getChecksum(header.toByteArray())) {
                        log.error("Checksum mismatch detected while trying to read "
                                + "header for logfile {}", file);
                        throw new DataCorruptionException();
                    }

                    if (header.getVersion() != VERSION) {
                        String msg = String.format("Log version {} for {} should match "
                                + "the logunit log version {}",
                                header.getVersion(), file.getAbsoluteFile(), VERSION);
                        throw new RuntimeException(msg);
                    }

                    if (!noVerify && !header.getVerifyChecksum()) {
                        String msg = String.format("Log file {} not generated with "
                                + "checksums, can't verify!", file.getAbsoluteFile());
                        throw new RuntimeException(msg);
                    }

                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
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
    public void trim(long address) {
        SegmentHandle handle = getSegmentHandleForAddress(address);
        try {
            if (!handle.getKnownAddresses().containsKey(address)
                    || handle.getPendingTrims().contains(address)) {
                return;
            }

            TrimEntry entry = TrimEntry.newBuilder()
                    .setChecksum(getChecksum(address))
                    .setAddress(address)
                    .build();

            // TODO(Maithem) possibly move this to SegmentHandle. Do we need to close and flush?
            OutputStream outputStream = Channels.newOutputStream(handle.getPendingTrimChannel());

            entry.writeDelimitedTo(outputStream);
            outputStream.flush();
            handle.pendingTrims.add(address);
            channelsToSync.add(handle.getPendingTrimChannel());
        } catch (IOException e) {
            log.warn("Exception while writing a trim entry {} : {}", address, e.toString());
        } finally {
            handle.release();
        }
    }

    @Override
    public synchronized void compact() {
        if (startingAddress == 0) {
            spaseCompact();
        } else {
            trimPrefix();
        }
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
        int numFiles = 0;
        long freedBytes = 0;
        for (SegmentHandle sh : writeChannels.values()) {
            if (sh.getSegment() <= endSegment) {
                if (sh.getRefCount() != 0) {
                    log.warn("trimPrefix: Segment {} is trimmed, but refCount is {},"
                                    + " attempting to trim anyways", sh.getSegment(),
                            sh.getRefCount());
                }
                sh.close();
                writeChannels.remove(sh.getFileName());
            }
        }

        File dir = new File(logDir);
        FileFilter fileFilter = new FileFilter() {
            public boolean accept(File file) {
                try {
                    String segmentStr = file.getName().split("\\.")[0];
                    return Long.parseLong(segmentStr) < endSegment;
                } catch (Exception e) {
                    log.warn("trimPrefix: ignoring file {}", file.getName());
                    return false;
                }
            }
        };

        File[] files = dir.listFiles(fileFilter);

        for (File file : files) {
            long delta = file.length();

            if (!file.delete()) {
                log.error("trimPrefix: Couldn't delete file {}", file.getName());
            } else {
                freedBytes += delta;
                numFiles++;
            }
        }

        log.info("trimPrefix: completed, deleted {} files, freed {} bytes, end segment {}",
                numFiles, freedBytes, endSegment);
    }

    private void spaseCompact() {
        //TODO(Maithem) Open all segment handlers?
        for (SegmentHandle sh : writeChannels.values()) {
            Set<Long> pending = new HashSet(sh.getPendingTrims());
            Set<Long> trimmed = sh.getTrimmedAddresses();

            if (sh.getKnownAddresses().size() + trimmed.size() != RECORDS_PER_LOG_FILE) {
                log.info("Log segment still not complete, skipping");
                continue;
            }

            pending.removeAll(trimmed);

            //what if pending size  == knownaddresses size ?
            if (pending.size() < TRIM_THRESHOLD) {
                log.trace("Thresh hold not exceeded. Ratio {} threshold {}",
                            pending.size(), TRIM_THRESHOLD);
                return; // TODO - should not return if compact on ranked address space is necessary
            }

            try {
                log.info("Starting compaction, pending entries size {}", pending.size());
                trimLogFile(sh.getFileName(), pending);
            } catch (IOException e) {
                log.error("Compact operation failed for file {}, {}", sh.getFileName(), e);
            }
        }
    }

    private void trimLogFile(String filePath, Set<Long> pendingTrim) throws IOException {
        try (FileChannel fc = FileChannel.open(FileSystems.getDefault().getPath(filePath + ".copy"),
                EnumSet.of(StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE, StandardOpenOption.SPARSE))) {

            CompactedEntry log = getCompactedEntries(filePath, pendingTrim);

            LogHeader header = log.getLogHeader();
            Collection<LogEntry> compacted = log.getEntries();

            writeHeader(fc, header.getVersion(), header.getVerifyChecksum());

            for (LogEntry entry : compacted) {

                ByteBuffer record = getByteBufferWithMetaData(entry);
                ByteBuffer recordBuf = ByteBuffer.allocate(Short.BYTES // Delimiter
                        + record.capacity());

                recordBuf.putShort(RECORD_DELIMITER);
                recordBuf.put(record.array());
                recordBuf.flip();

                fc.write(recordBuf);
            }

            fc.force(true);
        }

        try (FileChannel fc2 = FileChannel.open(FileSystems.getDefault()
                        .getPath(getTrimmedFilePath(filePath)),
                EnumSet.of(StandardOpenOption.APPEND))) {
            try (OutputStream outputStream = Channels.newOutputStream(fc2)) {
                // Todo(Maithem) How do we verify that the compacted file is correct?
                for (Long address : pendingTrim) {
                    TrimEntry entry = TrimEntry.newBuilder()
                            .setChecksum(getChecksum(address))
                            .setAddress(address)
                            .build();
                    entry.writeDelimitedTo(outputStream);
                }
                outputStream.flush();
                fc2.force(true);
            }
        }

        Files.move(Paths.get(filePath + ".copy"), Paths.get(filePath),
                StandardCopyOption.ATOMIC_MOVE);

        // Force the reload of the new segment
        writeChannels.remove(filePath);
    }

    private CompactedEntry getCompactedEntries(String filePath,
                                               Set<Long> pendingTrim) throws IOException {

        FileChannel fc = getChannel(filePath, true);

        // Skip the header
        ByteBuffer headerMetadataBuf = ByteBuffer.allocate(METADATA_SIZE);
        fc.read(headerMetadataBuf);
        headerMetadataBuf.flip();

        Metadata headerMetadata = Metadata.parseFrom(headerMetadataBuf.array());
        ByteBuffer headerBuf = ByteBuffer.allocate(headerMetadata.getLength());
        fc.read(headerBuf);
        headerBuf.flip();

        ByteBuffer o = ByteBuffer.allocate((int) fc.size() - (int) fc.position());
        fc.read(o);
        fc.close();
        o.flip();

        LinkedHashMap<Long, LogEntry> compacted = new LinkedHashMap<>();

        while (o.hasRemaining()) {

            //Skip delimiter
            o.getShort();

            byte[] metadataBuf = new byte[METADATA_SIZE];
            o.get(metadataBuf);

            try {
                Metadata metadata = Metadata.parseFrom(metadataBuf);

                byte[] logEntryBuf = new byte[metadata.getLength()];

                o.get(logEntryBuf);

                LogEntry entry = LogEntry.parseFrom(logEntryBuf);

                if (!noVerify) {
                    if (metadata.getChecksum() != getChecksum(entry.toByteArray())) {
                        log.error("Checksum mismatch detected while trying to read address {}",
                                    entry.getGlobalAddress());
                        throw new DataCorruptionException();
                    }
                }

                if (!pendingTrim.contains(entry.getGlobalAddress())) {
                    compacted.put(entry.getGlobalAddress(), entry);
                }

            } catch (InvalidProtocolBufferException e) {
                throw new DataCorruptionException();
            }
        }

        LogHeader header = LogHeader.parseFrom(headerBuf.array());
        return new CompactedEntry(header, compacted.values());
    }

    private LogData getLogData(LogEntry entry) {
        ByteBuf data = Unpooled.wrappedBuffer(entry.getData().toByteArray());
        LogData logData = new LogData(org.corfudb.protocols.wireprotocol
                .DataType.typeMap.get((byte) entry.getDataType().getNumber()), data);

        logData.setBackpointerMap(getUUIDLongMap(entry.getBackpointersMap()));
        logData.setGlobalAddress(entry.getGlobalAddress());
        logData.setRank(createDataRank(entry));

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

    /**
     * Reads an address space from a log file into a SegmentHandle.
     *
     * @param sh  Object containing state for the segment to be read
     */
    private void readAddressSpace(SegmentHandle sh) throws IOException {
        long logFileSize;


        try (MultiReadWriteLock.AutoCloseableLock ignored =
                     segmentLocks.acquireReadLock(sh.getSegment())) {
            logFileSize = sh.logChannel.size();
        }

        FileChannel fc = getChannel(sh.fileName, true);

        if (fc == null) {
            log.trace("Can't read address space, {} doesn't exist", sh.fileName);
            return;
        }

        // Skip the header
        ByteBuffer headerMetadataBuf = ByteBuffer.allocate(METADATA_SIZE);
        fc.read(headerMetadataBuf);
        headerMetadataBuf.flip();

        Metadata headerMetadata = Metadata.parseFrom(headerMetadataBuf.array());

        fc.position(fc.position() + headerMetadata.getLength());
        long channelOffset = fc.position();
        ByteBuffer o = ByteBuffer.allocate((int) logFileSize - (int) fc.position());
        fc.read(o);
        fc.close();
        o.flip();

        while (o.hasRemaining()) {

            short magic = o.getShort();
            channelOffset += Short.BYTES;

            if (magic != RECORD_DELIMITER) {
                log.error("Expected a delimiter but found something else while "
                        + "trying to read file {}", sh.fileName);
                throw new DataCorruptionException();
            }

            byte[] metadataBuf = new byte[METADATA_SIZE];
            o.get(metadataBuf);
            channelOffset += METADATA_SIZE;

            try {
                Metadata metadata = Metadata.parseFrom(metadataBuf);

                byte[] logEntryBuf = new byte[metadata.getLength()];

                o.get(logEntryBuf);

                LogEntry entry = LogEntry.parseFrom(logEntryBuf);

                if (!noVerify) {
                    if (metadata.getChecksum() != getChecksum(entry.toByteArray())) {
                        log.error("Checksum mismatch detected while trying to read file {}",
                                sh.fileName);
                        throw new DataCorruptionException();
                    }
                }

                sh.knownAddresses.put(entry.getGlobalAddress(),
                        new AddressMetaData(metadata.getChecksum(),
                                metadata.getLength(), channelOffset));

                channelOffset += metadata.getLength();

            } catch (InvalidProtocolBufferException e) {
                throw new DataCorruptionException();
            }
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
        FileChannel fc = null;
        try {
            fc = getChannel(sh.fileName, true);
            AddressMetaData metaData = sh.getKnownAddresses().get(address);
            if (metaData == null) {
                return null;
            }

            fc.position(metaData.offset);

            try {
                ByteBuffer entryBuf = ByteBuffer.allocate(metaData.length);
                fc.read(entryBuf);
                return getLogData(LogEntry.parseFrom(entryBuf.array()));
            } catch (InvalidProtocolBufferException e) {
                throw new DataCorruptionException();
            }
        } finally {
            if (fc != null) {
                fc.close();
            }
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
            try {
                boolean verify = true;
                if (noVerify) {
                    verify = false;
                }

                FileChannel fc1 = getChannel(a, false);
                FileChannel fc2 = getChannel(getTrimmedFilePath(a), false);
                FileChannel fc3 = getChannel(getPendingTrimsFilePath(a), false);

                if (fc1.size() == 0) {
                    writeHeader(fc1, VERSION, verify);
                    log.trace("Opened new segment file, writing header for {}", a);
                }
                log.trace("Opened new log file at {}", a);
                SegmentHandle sh = new SegmentHandle(segment, fc1, fc2, fc3, a);
                // The first time we open a file we should read to the end, to load the
                // map of entries we already have.
                readAddressSpace(sh);
                loadTrimAddresses(sh);
                return sh;
            } catch (IOException e) {
                log.error("Error opening file {}", a, e);
                throw new RuntimeException(e);
            }
        });

        handle.retain();
        return handle;
    }

    private void loadTrimAddresses(SegmentHandle sh) throws IOException {
        long trimmedSize;
        long pendingTrimSize;

        //TODO(Maithem) compute checksums and refactor
        try (MultiReadWriteLock.AutoCloseableLock ignored =
                     segmentLocks.acquireReadLock(sh.getSegment())) {
            trimmedSize = sh.getTrimmedChannel().size();
            pendingTrimSize = sh.getPendingTrimChannel().size();
        }

        try (FileChannel fcTrimmed = getChannel(getTrimmedFilePath(sh.getFileName()), true)) {
            try (InputStream inputStream = Channels.newInputStream(fcTrimmed)) {

                while (fcTrimmed.position() < trimmedSize) {
                    TrimEntry trimEntry = TrimEntry.parseDelimitedFrom(inputStream);
                    sh.getTrimmedAddresses().add(trimEntry.getAddress());
                }

                inputStream.close();
                fcTrimmed.close();

                try (FileChannel fcPending =
                             getChannel(getPendingTrimsFilePath(sh.getFileName()), true)) {
                    try (InputStream pendingInputStream = Channels.newInputStream(fcPending)) {

                        while (fcPending.position() < pendingTrimSize) {
                            TrimEntry trimEntry = TrimEntry.parseDelimitedFrom(pendingInputStream);
                            sh.getPendingTrims().add(trimEntry.getAddress());
                        }
                    }
                }
            }
        } catch (FileNotFoundException fe) {
            return;
        }
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

        ByteBuffer recordBuf = ByteBuffer.allocate(Short.BYTES // Delimiter
                + record.capacity());

        recordBuf.putShort(RECORD_DELIMITER);
        recordBuf.put(record.array());
        recordBuf.flip();

        long channelOffset;

        try (MultiReadWriteLock.AutoCloseableLock ignored =
                     segmentLocks.acquireWriteLock(fh.getSegment())) {
            channelOffset = fh.logChannel.position() + Short.BYTES + METADATA_SIZE;
            fh.logChannel.write(recordBuf);
            channelsToSync.add(fh.logChannel);
            syncTailSegment(address);
        }

        return new AddressMetaData(metadata.getChecksum(), metadata.getLength(), channelOffset);
    }

    @Override
    public void append(long address, LogData entry) {
        if(isTrimmed(address)) {
            throw new OverwriteException();
        }

        SegmentHandle fh = getSegmentHandleForAddress(address);

        try {
            // make sure the entry doesn't currently exist...
            // (probably need a faster way to do this - high watermark?)
            if (fh.getKnownAddresses().containsKey(address)
                    || fh.getTrimmedAddresses().contains(address)) {
                if (entry.getRank() == null) {
                    throw new OverwriteException();
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
            return LogData.TRIMMED;
        }
        SegmentHandle sh = getSegmentHandleForAddress(address);

        try {
            if (sh.getPendingTrims().contains(address)) {
                return LogData.TRIMMED;
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

    @VisibleForTesting
    Set<FileChannel> getChannelsToSync() {
        return channelsToSync;
    }

    @VisibleForTesting
    Collection<SegmentHandle> getSegmentHandles() {
        return writeChannels.values();
    }

    public static class CompactedEntry {
        private final LogHeader logHeader;
        private final Collection<LogEntry> entries;

        public CompactedEntry(LogHeader logHeader, Collection<LogEntry> entries) {
            this.logHeader = logHeader;
            this.entries = entries;
        }

        public LogHeader getLogHeader() {
            return logHeader;
        }

        public Collection<LogEntry> getEntries() {
            return entries;
        }
    }

    /**
     * A SegmentHandle is a range view of consecutive addresses in the log. It contains
     * the address space along with metadata like addresses that are trimmed and pending trims.
     */

    @Data
    class SegmentHandle {
        private final long segment;
        @NonNull
        private final FileChannel logChannel;
        @NonNull
        private final FileChannel trimmedChannel;
        @NonNull
        private final FileChannel pendingTrimChannel;
        @NonNull
        private String fileName;

        private Map<Long, AddressMetaData> knownAddresses = new ConcurrentHashMap();
        private Set<Long> trimmedAddresses = Collections.newSetFromMap(new ConcurrentHashMap<>());
        private Set<Long> pendingTrims = Collections.newSetFromMap(new ConcurrentHashMap<>());
        private volatile int refCount = 0;


        public synchronized void retain() {
            refCount++;
        }

        public synchronized void release() {
            if (refCount == 0) {
                throw new IllegalStateException("refCount cannot be less than 0, segment " + segment);
            }
            refCount--;
        }

        public void close() {
            Set<FileChannel> channels =
                    new HashSet(Arrays.asList(logChannel, trimmedChannel, pendingTrimChannel));
            for (FileChannel channel : channels) {
                try {
                    channel.force(true);
                    channel.close();
                    channel = null;
                } catch (Exception e) {
                    log.warn("Error closing channel {}: {}", channel.toString(), e.toString());
                }
            }

            knownAddresses = null;
            trimmedAddresses = null;
            pendingTrims = null;
        }
    }
}
