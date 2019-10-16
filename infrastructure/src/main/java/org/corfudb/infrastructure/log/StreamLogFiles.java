package org.corfudb.infrastructure.log;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.corfudb.format.Types;
import org.corfudb.format.Types.LogEntry;
import org.corfudb.format.Types.LogHeader;
import org.corfudb.format.Types.Metadata;
import org.corfudb.infrastructure.ResourceQuota;
import org.corfudb.infrastructure.log.CompactionPolicy.CompactionPolicyType;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.LogUnitException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileStore;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.corfudb.infrastructure.log.StreamLogParams.METADATA_SIZE;
import static org.corfudb.infrastructure.log.StreamLogParams.VERSION;

/**
 * This class implements the StreamLog by persisting the stream log as records in multiple files.
 * This StreamLog implementation can detect log file corruption, if checksum is enabled, otherwise
 * the checksum field will be ignored.
 *
 * <p>Created by maithem on 10/28/16.
 */

@Slf4j
public class StreamLogFiles implements StreamLog {

    private static int CLOSE_SEGMENT_EXCEPTION_RETRY = 1;

    @Getter
    private final StreamLogParams logParams;
    @Getter
    private final Path logDir;
    @Getter
    private final FileStore fileStore;

    @Getter
    private final SegmentManager segmentManager;

    private final StreamLogDataStore dataStore;

    @Getter
    private final StreamLogCompactor compactor;

    private final Set<AbstractLogSegment> segmentsToSync;

    //=================Log Metadata=================
    // TODO(Maithem) this should effectively be final, but it is used
    // by a reset API that clears the state of this class, on reset
    // a new instance of this class should be created after deleting
    // the files of the old instance
    @Getter
    private LogMetadata logMetadata;

    // Derived size in bytes that normal writes to the log unit are capped at.
    // This is derived as a percentage of the log's filesystem capacity.
    private final long logSizeLimit;
    // Resource quota to track the log size, which acts as an efficient
    // estimation and may not reflect the real disk usage.
    @Getter
    private ResourceQuota logSizeQuota;

    /**
     * Returns a file-based stream log object.
     *
     * @param streamLogParams    stream log parameters
     * @param streamLogDataStore stream log data-store which stores persisted meta information
     */
    public StreamLogFiles(StreamLogParams streamLogParams, StreamLogDataStore streamLogDataStore) {
        this.logParams = streamLogParams;
        this.dataStore = streamLogDataStore;

        logDir = Paths.get(logParams.logPath, "log");
        segmentsToSync = new HashSet<>();

        if (logParams.logSizeQuotaPercentage < 0.0 || 100.0 < logParams.logSizeQuotaPercentage) {
            String msg = String.format("Invalid quota: quota(%f)%% must be between 0-100%%",
                    logParams.logSizeQuotaPercentage);
            throw new IllegalArgumentException(msg);
        }

        fileStore = initStreamLogDirectory();
        long fileSystemCapacity = getStorageTotalSpace(fileStore);
        logSizeLimit = (long) (fileSystemCapacity * logParams.logSizeQuotaPercentage / 100.0);

        long initialLogSize = estimateSize(logDir);
        log.info("StreamLogFiles: {} size is {} bytes, limit {}", logDir, initialLogSize, logSizeLimit);
        logSizeQuota = new ResourceQuota("LogSizeQuota", logSizeLimit);
        logSizeQuota.consume(initialLogSize);

        segmentManager = new SegmentManager(logParams, logDir, logSizeQuota, dataStore);
        segmentManager.deleteExistingCompactionOutputFiles();

        verifyLogs();
        // Starting address initialization should happen before
        // initializing the tail segment (i.e. initializeMaxGlobalAddress)
        logMetadata = new LogMetadata();
        initializeLogMetadata();

        compactor = new StreamLogCompactor(logParams, getCompactionPolicy(),
                segmentManager, dataStore, logMetadata);
    }

    private CompactionPolicy getCompactionPolicy() {
        CompactionPolicyType policyType = CompactionPolicyType
                .valueOf(logParams.compactionPolicyType.toUpperCase());

        if (policyType == CompactionPolicyType.GARBAGE_SIZE_FIRST) {
            log.info("getCompactionPolicy: using {} compaction policy", policyType);
            return new GarbageSizeFirstPolicy(logParams, logSizeQuota, fileStore);
        } else if (policyType == CompactionPolicyType.SNAPSHOT_LENGTH_FIRST) {
            log.info("getCompactionPolicy: using {} compaction policy", policyType);
            return new SnapshotLengthFirstPolicy(logParams, logSizeQuota, fileStore, logMetadata);
        } else {
            throw new IllegalArgumentException("Compaction policy not found.");
        }
    }

    /**
     * Create stream log directory if not exists.
     *
     * @return the underlying file store of the corfu directory.
     */
    private FileStore initStreamLogDirectory() {
        FileStore corfuDirFileStore;

        try {
            if (!logDir.toFile().exists()) {
                Files.createDirectories(logDir);
            }

            String corfuDir = logDir.getParent().toString();
            corfuDirFileStore = Files.getFileStore(Paths.get(corfuDir));

            if (corfuDirFileStore.isReadOnly()) {
                throw new LogUnitException("Cannot start Corfu on a read-only filesystem:" + corfuDir);
            }

            File corfuDirFile = new File(corfuDir);
            if (!corfuDirFile.canWrite()) {
                throw new LogUnitException("Corfu directory is not writable " + corfuDir);
            }

            File logDirectory = new File(logDir.toString());
            if (!logDirectory.canWrite()) {
                throw new LogUnitException("Stream log directory not writable in " + corfuDir);
            }

        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }

        log.info("initStreamLogDirectory: initialized {}", logDir);
        return corfuDirFileStore;
    }

    /**
     * This method will scan the log (i.e. read all log segment files)
     * on this LU and create a map of stream offsets and the global
     * addresses seen.
     * <p>
     * consecutive segments from [startSegment, endSegment]
     */
    private void initializeLogMetadata() {
        long startSegment = segmentManager.getSegmentOrdinal(dataStore.getStartingAddress());
        long tailSegment = dataStore.getTailSegment();

        long start = System.currentTimeMillis();
        for (long currentSegment = tailSegment; currentSegment >= startSegment; currentSegment--) {
            StreamLogSegment streamSegment = segmentManager.getStreamLogSegmentByOrdinal(currentSegment);

            for (Long address : streamSegment.getKnownAddresses().keySet()) {
                LogData logData = read(address);
                logMetadata.update(Collections.singletonList(logData));
            }

            // Update global tail with compactedAddresses to prevent holes regressing global tail.
            // It is safe for stream tails and stream address map since holes do not belong to any
            // stream and if a stream update in a LogData is compacted, it must had been marked
            // garbage by someone with greater address and still not compacted yet.
            Roaring64NavigableMap compactedAddresses = streamSegment.getCompactedAddresses();
            LongIterator iterator = compactedAddresses.getReverseLongIterator();
            if (iterator.hasNext()) {
                logMetadata.updateGlobalTail(iterator.next());
            }

            // Close and remove the reference of the unprotected segments.
            if (streamSegment.getOrdinal() <= tailSegment - logParams.protectedSegments) {
                segmentManager.close(streamSegment.getOrdinal());
            }
        }

        long end = System.currentTimeMillis();
        log.info("initializeLogMetadata: took {} ms to load log metadata," +
                "global tail: {}", end - start, logMetadata.getGlobalTail());
    }

    /**
     * Write the header for a Corfu log file.
     *
     * @param fileChannel The file channel to use.
     * @param version     The version number to append to the header.
     * @param verify      Checksum verify flag
     * @throws IOException I/O exception
     */
    static void writeHeader(FileChannel fileChannel, ResourceQuota quota,
                            int version, boolean verify) throws IOException {
        LogHeader header = LogHeader.newBuilder()
                .setVersion(version)
                .setVerifyChecksum(verify)
                .build();

        ByteBuffer buf = getByteBufferWithMetaData(header);
        writeByteBuffer(fileChannel, buf, quota);
        fileChannel.force(true);
    }

    static Metadata getMetadata(AbstractMessage message) {
        return Metadata.newBuilder()
                .setPayloadChecksum(Checksum.getChecksum(message.toByteArray()))
                .setLengthChecksum(Checksum.getChecksum(message.getSerializedSize()))
                .setLength(message.getSerializedSize())
                .build();
    }

    static ByteBuffer getByteBuffer(Metadata metadata, AbstractMessage message) {
        ByteBuffer buf = ByteBuffer.allocate(METADATA_SIZE + message.getSerializedSize());
        buf.put(metadata.toByteArray());
        buf.put(message.toByteArray());
        buf.flip();
        return buf;
    }

    @VisibleForTesting
    public static ByteBuffer getByteBufferWithMetaData(AbstractMessage message) {
        Metadata metadata = getMetadata(message);
        return getByteBuffer(metadata, message);
    }

    @Override
    public boolean quotaExceeded() {
        return !logSizeQuota.hasAvailable();
    }

    @Override
    public long getLogTail() {
        return logMetadata.getGlobalTail();
    }

    @Override
    public TailsResponse getTails(List<UUID> streams) {
        Map<UUID, Long> tails = new HashMap<>();
        streams.forEach(stream -> {
            tails.put(stream, logMetadata.getStreamTails().get(stream));
        });
        return new TailsResponse(logMetadata.getGlobalTail(), tails);
    }

    @Override
    public StreamsAddressResponse getStreamsAddressSpace() {
        return new StreamsAddressResponse(logMetadata.getGlobalTail(), logMetadata.getStreamsAddressSpaceMap());
    }

    @Override
    public TailsResponse getAllTails() {
        Map<UUID, Long> tails = new HashMap<>(logMetadata.getStreamTails());
        return new TailsResponse(logMetadata.getGlobalTail(), tails);
    }

    private void verifyLogs() {
        String[] extension = {"log"};
        File dir = logDir.toFile();

        if (!dir.exists()) {
            throw new UnrecoverableCorfuError("Stream log data directory doesn't exists");
        }

        Collection<File> files = FileUtils.listFiles(dir, extension, true);

        for (File file : files) {
            LogHeader header;

            try (FileChannel fileChannel = FileChannel.open(file.toPath())) {
                header = parseHeader(fileChannel, file.getAbsolutePath());
            } catch (IOException e) {
                throw new IllegalStateException("Invalid header: " + file.getAbsolutePath(), e);
            }

            if (header == null) {
                log.warn("verifyLogs: Ignoring partially written header in {}", file.getAbsoluteFile());
                continue;
            }

            if (header.getVersion() != VERSION) {
                String msg = String.format("Log version %s for %s should match the LogUnit log version %s",
                        header.getVersion(), file.getAbsoluteFile(), VERSION);
                throw new IllegalStateException(msg);
            }

            if (logParams.verifyChecksum && !header.getVerifyChecksum()) {
                String msg = String.format("Log file %s not generated with check sums, can't verify!",
                        file.getAbsoluteFile());
                throw new IllegalStateException(msg);
            }
        }
    }

    @Override
    public void sync(boolean force) {
        if (!force) {
            segmentsToSync.clear();
            return;
        }

        for (AbstractLogSegment segment : segmentsToSync) {
            try {
                segment.sync();
            } catch (ClosedSegmentException e) {
                // Ignore, segment could be closed by compactor or SegmentManager.
                log.debug("sync: segment {} closed, ignore sync", segment.filePath);
            }
        }
        log.trace("Sync'd {} segments", segmentsToSync.size());
        segmentsToSync.clear();
    }

    @Override
    public void startCompactor() {
        compactor.start();
    }

    /**
     * Verifies that a range of entries doesn't span more than two
     * segments and that the log addresses are ordered sequentially.
     *
     * @param range entries to verify
     * @return return true if the range is valid
     */
    private boolean verifyRangeWrite(List<LogData> range) {
        // Make sure that entries are ordered sequentially.
        long firstAddress = range.get(0).getGlobalAddress();
        for (int x = 1; x < range.size(); x++) {
            if (range.get(x).getGlobalAddress() != firstAddress + x) {
                return false;
            }
        }

        // Check if the range spans more than two segments.
        long lastAddress = range.get(range.size() - 1).getGlobalAddress();
        long firstSegment = segmentManager.getSegmentOrdinal(firstAddress);
        long endSegment = segmentManager.getSegmentOrdinal(lastAddress);

        return endSegment - firstSegment <= 1;
    }

    /**
     * This method requests for known addresses in this Log Unit in the specified consecutive
     * range of addresses.
     *
     * @param rangeStart Start address of range.
     * @param rangeEnd   End address of range.
     * @return Set of known addresses.
     */
    @Override
    public Set<Long> getKnownAddressesInRange(long rangeStart, long rangeEnd) {
        Set<Long> result = new HashSet<>();

        for (long address = rangeStart; address <= rangeEnd; address++) {
            if (segmentManager.getStreamLogSegment(address).getKnownAddresses().containsKey(address)) {
                result.add(address);
            }
        }

        return result;
    }

    @Override
    public void append(List<LogData> range) {
        if (range.isEmpty()) {
            log.info("No entries to write.");
            return;
        }

        // Assuming garbage log entries are not mixed up with stream log entries.
        DataType dataType = range.get(0).getType();
        if (dataType != DataType.GARBAGE && !verifyRangeWrite(range)) {
            throw new IllegalArgumentException("Write range not consecutive or " +
                    "too large. Range size: " + range.size());
        }

        Collection<List<LogData>> segmentedRange = getSegmentedEntries(range);
        segmentedRange.forEach(entries -> appendToSegment(entries, dataType));
    }

    /**
     * Group a list of entries by their corresponding segment's ordinal.
     */
    private Collection<List<LogData>> getSegmentedEntries(List<LogData> entries) {
        Map<Long, List<LogData>> ordinalToRangeMap = new HashMap<>();

        entries.forEach(entry -> {
            long ordinal = segmentManager.getSegmentOrdinal(entry.getGlobalAddress());
            List<LogData> list = ordinalToRangeMap.computeIfAbsent(ordinal, ord -> new ArrayList<>());
            list.add(entry);
        });

        return ordinalToRangeMap.values();
    }

    /**
     * Append to one segment. The caller should ensure entries do not span segments.
     */
    private void appendToSegment(List<LogData> entries, DataType dataType) {
        LogData first = entries.get(0);

        int retryCount = 0;
        while (true) {
            try {
                AbstractLogSegment segment = (dataType == DataType.GARBAGE)
                        ? segmentManager.getGarbageLogSegment(first.getGlobalAddress())
                        : segmentManager.getStreamLogSegment(first.getGlobalAddress());
                segment.append(entries);
                updateGlobalMetaData(entries.get(entries.size() - 1).getGlobalAddress(), entries, segment);
                return;
            } catch (ClosedSegmentException e) {
                // Segment could be closed because of compaction, retry once.
                if (retryCount == CLOSE_SEGMENT_EXCEPTION_RETRY) {
                    throw e;
                }
            }
        }
    }

    @Override
    public void append(long address, LogData entry) {
        int retryCount = 0;
        while (true) {
            try {
                AbstractLogSegment segment = (entry.getType() == DataType.GARBAGE)
                        ? segmentManager.getGarbageLogSegment(address)
                        : segmentManager.getStreamLogSegment(address);
                segment.append(address, entry);
                updateGlobalMetaData(address, Collections.singletonList(entry), segment);
                return;
            } catch (ClosedSegmentException e) {
                log.warn("Segment channel closed by compactor, retry for another time.");
                // Segment could be closed because of compaction, retry once.
                if (retryCount == CLOSE_SEGMENT_EXCEPTION_RETRY) {
                    throw e;
                }
            }
        }
    }

    private void updateGlobalMetaData(long lastAddress, List<LogData> entries,
                                      AbstractLogSegment segmentToSync) {
        // TODO(Maithem) since writing a record and setting the tail segment is not
        // an atomic operation, it is possible to set an incorrect tail segment. In
        // that case we will need to scan more than one segment.
        segmentsToSync.add(segmentToSync);
        if (segmentToSync instanceof StreamLogSegment) {
            logMetadata.updateGlobalTail(lastAddress);
            logMetadata.update(entries);
            dataStore.updateTailSegment(segmentToSync.getOrdinal());
        }
    }

    @Override
    public LogData read(long address) {
        return read(address, true);
    }

    @Override
    public LogData readGarbageEntry(long address) {
        return read(address, false);
    }

    private LogData read(long address, boolean fromStreamLog) {
        int retryCount = 0;
        while (true) {
            try {
                AbstractLogSegment segment = fromStreamLog
                        ? segmentManager.getStreamLogSegment(address)
                        : segmentManager.getGarbageLogSegment(address);
                return segment.read(address);
            } catch (ClosedSegmentException e) {
                log.warn("Segment channel closed by compactor, retry for another time.");
                // Segment could be closed because of compaction, retry once.
                if (retryCount++ == CLOSE_SEGMENT_EXCEPTION_RETRY) {
                    throw e;
                }
            }
        }
    }

    @Override
    public long getGlobalCompactionMark() {
        return dataStore.getGlobalCompactionMark();
    }

    @Override
    public void close() {
        compactor.shutdown();
        segmentManager.close();
        segmentsToSync.clear();
    }

    /**
     * TODO(Maithem) remove this method. Obtaining a new instance should happen
     * through instantiation not by clearing this class' state
     * <p>
     * Resets the Stream log.
     * Clears all data and resets the handlers.
     * Usage: To heal a recovering node, we require to wipe off existing data.
     */
    @Override
    public void reset() {
        // Trim all segments
        long endSegment = segmentManager.getSegmentOrdinal(Math.max(logMetadata.getGlobalTail(), 0L));
        log.warn("Global Tail:{}, endSegment={}", logMetadata.getGlobalTail(), endSegment);

        segmentManager.cleanAndClose();

        dataStore.resetStartingAddress();
        dataStore.resetTailSegment();
        dataStore.resetGlobalCompactionMark();
        dataStore.resetAllCompactedAddresses();
        logMetadata = new LogMetadata();
        segmentsToSync.clear();
        logSizeQuota = new ResourceQuota("LogSizeQuota", logSizeLimit);
        log.info("reset: Completed, end segment {}", endSegment);
    }

    @VisibleForTesting
    Set<AbstractLogSegment> getSegmentsToSync() {
        return segmentsToSync;
    }

    //================File Operation Utilities (Parsing & I/O)================//

    private static long getStorageTotalSpace(FileStore fileStore) {
        try {
            return fileStore.getTotalSpace();
        } catch (IOException e) {
            log.error("Error trying to get total disk space");
            throw new IllegalStateException(e);
        }
    }

    static LogData getLogData(LogEntry entry) {
        ByteBuf data = Unpooled.wrappedBuffer(entry.getData().toByteArray());
        LogData logData = new LogData(org.corfudb.protocols.wireprotocol
                .DataType.typeMap.get((byte) entry.getDataType().getNumber()), data);

        logData.setBackpointerMap(getUUIDLongMap(entry.getBackpointersMap()));
        logData.setGlobalAddress(entry.getGlobalAddress());
        logData.setRank(createDataRank(entry));

        if (entry.hasThreadId()) {
            logData.setThreadId(entry.getThreadId());
        }
        if (entry.hasClientIdLeastSignificant() && entry.hasClientIdMostSignificant()) {
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

    /**
     * Parse the metadata field. This method should only be called
     * when a metadata field is expected.
     *
     * @param fileChannel the channel to read from
     * @return metadata field of null if it was partially written.
     * @throws IOException IO exception
     */
    static Metadata parseMetadata(FileChannel fileChannel, String segmentFile) throws IOException {
        long actualMetaDataSize = fileChannel.size() - fileChannel.position();
        if (actualMetaDataSize < METADATA_SIZE) {
            log.warn("Meta data has wrong size. Actual size: {}, expected: {}",
                    actualMetaDataSize, METADATA_SIZE
            );
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(METADATA_SIZE);
        fileChannel.read(buf);
        buf.flip();

        Metadata metadata;

        try {
            metadata = Metadata.parseFrom(buf.array());
        } catch (InvalidProtocolBufferException e) {
            String errorMessage = getDataCorruptionErrorMessage(
                    "Can't parse metadata", segmentFile, fileChannel);
            throw new DataCorruptionException(errorMessage, e);
        }

        if (metadata.getLengthChecksum() != Checksum.getChecksum(metadata.getLength())) {
            String errorMessage = getDataCorruptionErrorMessage(
                    "Metadata: invalid length checksum", segmentFile, fileChannel);
            throw new DataCorruptionException(errorMessage);
        }

        return metadata;
    }

    static String getDataCorruptionErrorMessage(String message,
                                                String segmentFile,
                                                FileChannel fileChannel) throws IOException {
        return String.format("%s. Segment file: %s, file size: %s, file position: %s",
                message, segmentFile, fileChannel.size(), fileChannel.position());
    }

    /**
     * Read a payload given metadata.
     *
     * @param fileChannel channel to read the payload from
     * @param metadata    the metadata that is written before the payload
     * @return ByteBuffer for the payload
     * @throws IOException IO exception
     */
    static ByteBuffer getPayloadForMetadata(FileChannel fileChannel, Metadata metadata) throws IOException {
        if (fileChannel.size() - fileChannel.position() < metadata.getLength()) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(metadata.getLength());
        fileChannel.read(buf);
        buf.flip();
        return buf;
    }

    /**
     * Parse the logfile header, or create it, or recreate it if it was
     * partially written.
     *
     * @param channel file channel
     * @return log header
     * @throws IOException IO exception
     */
    static LogHeader parseHeader(FileChannel channel, String segmentFile) throws IOException {
        Metadata metadata = parseMetadata(channel, segmentFile);
        if (metadata == null) {
            // Partial write on the metadata for the header or no header
            // Rewind the channel position to the beginning of the file
            channel.position(0);
            return null;
        }

        ByteBuffer buffer = getPayloadForMetadata(channel, metadata);
        if (buffer == null) {
            // partial write on the header payload
            // Rewind the channel position to the beginning of the file
            channel.position(0);
            return null;
        }

        if (Checksum.getChecksum(buffer.array()) != metadata.getPayloadChecksum()) {
            String errorMessage = getDataCorruptionErrorMessage(
                    "Invalid metadata checksum", segmentFile, channel);
            throw new DataCorruptionException(errorMessage);
        }

        LogHeader header;

        try {
            header = LogHeader.parseFrom(buffer.array());
        } catch (InvalidProtocolBufferException e) {
            String errorMessage = getDataCorruptionErrorMessage(
                    "Invalid header", segmentFile, channel);
            throw new DataCorruptionException(errorMessage, e);
        }

        return header;
    }

    /**
     * Parse an entry.
     *
     * @param channel file channel
     * @return an log entry
     * @throws IOException IO exception
     */
    static LogEntry parseEntry(FileChannel channel, Metadata metadata,
                               String fileName, StreamLogParams logParams) throws IOException {
        if (metadata == null) {
            // The metadata for this entry was partial written
            return null;
        }

        ByteBuffer buffer = getPayloadForMetadata(channel, metadata);
        if (buffer == null) {
            return null;
        }

        if (logParams.verifyChecksum && metadata.getPayloadChecksum() != Checksum.getChecksum(buffer.array())) {
            String errorMessage = getDataCorruptionErrorMessage(
                    "Checksum mismatch detected while trying to read file", fileName, channel);
            throw new DataCorruptionException(errorMessage);
        }


        LogEntry entry;
        try {
            entry = LogEntry.parseFrom(buffer.array());
        } catch (InvalidProtocolBufferException e) {
            String errorMessage = getDataCorruptionErrorMessage(
                    "Invalid entry", fileName, channel);
            throw new DataCorruptionException(errorMessage, e);
        }
        return entry;
    }

    private static Map<String, Long> getStrLongMap(Map<UUID, Long> uuidLongMap) {
        Map<String, Long> stringLongMap = new HashMap<>();

        for (Map.Entry<UUID, Long> entry : uuidLongMap.entrySet()) {
            stringLongMap.put(entry.getKey().toString(), entry.getValue());
        }

        return stringLongMap;
    }

    @SuppressWarnings("checkstyle:abbreviationaswordinname")  // Due to deprecation
    private static Map<UUID, Long> getUUIDLongMap(Map<String, Long> stringLongMap) {
        Map<UUID, Long> uuidLongMap = new HashMap<>();

        for (Map.Entry<String, Long> entry : stringLongMap.entrySet()) {
            uuidLongMap.put(UUID.fromString(entry.getKey()), entry.getValue());
        }

        return uuidLongMap;
    }

    private static Set<String> getStrUUID(Set<UUID> uuids) {
        Set<String> strUUIds = new HashSet<>();

        for (UUID uuid : uuids) {
            strUUIds.add(uuid.toString());
        }

        return strUUIds;
    }

    static LogEntry getLogEntry(long address, LogData entry) {
        byte[] data;

        if (entry.getData() != null) {
            data = entry.getData();
        } else {
            ByteBuf buf = Unpooled.buffer();
            entry.serializePayload(buf);
            data = buf.array();
        }

        LogEntry.Builder logEntryBuilder = LogEntry.newBuilder()
                .setDataType(Types.DataType.forNumber(entry.getType().ordinal()))
                .setData(ByteString.copyFrom(data))
                .setGlobalAddress(address)
                .addAllStreams(getStrUUID(entry.getStreams()))
                .putAllBackpointers(getStrLongMap(entry.getBackpointerMap()));

        Optional<Types.DataRank> rank = createProtobufsDataRank(entry);
        rank.ifPresent(logEntryBuilder::setRank);

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

    private static Optional<Types.DataRank> createProtobufsDataRank(IMetadata entry) {
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

    @Nullable
    private static IMetadata.DataRank createDataRank(LogEntry entity) {
        if (!entity.hasRank()) {
            return null;
        }
        Types.DataRank rank = entity.getRank();
        return new IMetadata.DataRank(rank.getRank(),
                new UUID(rank.getUuidMostSignificant(), rank.getUuidLeastSignificant()));
    }

    /**
     * Attempts to write a buffer to a file channel, if write fails with an
     * IOException then the channel pointer is moved back to its original position
     * before the write
     *
     * @param channel the channel to write to
     * @param buf     the buffer to write
     * @throws IOException IO exception
     */
    static void writeByteBuffer(FileChannel channel,
                                ByteBuffer buf,
                                ResourceQuota quota) throws IOException {
        // On IOExceptions this class should be reinitialized, so consuming
        // the buffer size and failing on the write should be an issue
        quota.consume(buf.remaining());
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
    }

    static class Checksum {

        private Checksum() {
            //prevent creating instances
        }

        /**
         * Returns checksum used for log.
         *
         * @param bytes data over which to compute the checksum
         * @return checksum of bytes
         */
        public static int getChecksum(byte[] bytes) {
            Hasher hasher = Hashing.crc32c().newHasher();
            for (byte a : bytes) {
                hasher.putByte(a);
            }

            return hasher.hash().asInt();
        }

        public static int getChecksum(int num) {
            Hasher hasher = Hashing.crc32c().newHasher();
            return hasher.putInt(num).hash().asInt();
        }
    }

    /**
     * Estimate the size (in bytes) of a directory.
     * From https://stackoverflow.com/a/19869323
     */
    @VisibleForTesting
    static long estimateSize(Path directoryPath) {
        final AtomicLong size = new AtomicLong(0);
        try {
            Files.walkFileTree(directoryPath, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file,
                                                 BasicFileAttributes attrs) {
                    size.addAndGet(attrs.size());
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException exc) {
                    // Skip folders that can't be traversed
                    log.error("skipped: {}", file, exc);
                    return FileVisitResult.CONTINUE;
                }
            });

            return size.get();
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }
}
