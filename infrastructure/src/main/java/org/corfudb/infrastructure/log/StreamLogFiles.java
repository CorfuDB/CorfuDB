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
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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

    @Getter
    private final StreamLogParams logParams;
    @Getter
    private final Path logDir;

    @Getter
    private final SegmentManager segmentManager;

    private final StreamLogDataStore dataStore;

    private Set<FileChannel> channelsToSync;

    //=================Log Metadata=================
    // TODO(Maithem) this should effectively be final, but it is used
    // by a reset API that clears the state of this class, on reset
    // a new instance of this class should be created after deleting
    // the files of the old instance
    private LogMetadata logMetadata;


    /**
     * Returns a file-based stream log object.
     *
     * @param streamLogParams    stream log parameters
     * @param streamLogDataStore stream log data-store which stores persisted meta information
     */
    public StreamLogFiles(StreamLogParams streamLogParams, StreamLogDataStore streamLogDataStore) {
        logParams = streamLogParams;
        logDir = Paths.get(logParams.logPath, "log");
        channelsToSync = new HashSet<>();
        segmentManager = new SegmentManager(logParams, logDir);
        dataStore = streamLogDataStore;

        initStreamLogDirectory();
        verifyLogs();
        // Starting address initialization should happen before
        // initializing the tail segment (i.e. initializeMaxGlobalAddress)
        logMetadata = new LogMetadata();
        initializeLogMetadata();

        // This can happen if a prefix trim happens on
        // addresses that haven't been written
        if (Math.max(logMetadata.getGlobalTail(), 0L) < getTrimMark()) {
            syncTailSegment(getTrimMark() - 1);
        }
    }

    /**
     * Create stream log directory if not exists
     */
    private void initStreamLogDirectory() {
        if (logDir.toFile().exists()) {
            String corfuDir = logDir.getParent().toString();
            // If FileSystem is mounted as read-only, Corfu server cannot function.
            try {
                FileStore fs = Files.getFileStore(Paths.get(corfuDir));
                if (fs.isReadOnly()) {
                    throw new UnrecoverableCorfuError("Cannot start Corfu on a " +
                            "read-only filesystem: " + corfuDir);
                }
            } catch (IOException e) {
                throw new UnrecoverableCorfuError("Unable to retrieve Corfu " +
                        "filesystem permissions: " + corfuDir);
            }

            // Corfu dir in the filesystem must be writable for writing configuration files.
            File corfuDirFile = new File(corfuDir);
            if (!corfuDirFile.canWrite()) {
                throw new UnrecoverableCorfuError("Corfu directory is not writable " + corfuDir);
            }
            File logDirectory = new File(logDir.toString());
            if (!logDirectory.canWrite()) {
                throw new UnrecoverableCorfuError("Stream log directory not writable in " + corfuDir);
            }
            log.info("Log directory already exists: {}", logDir);
            return;
        }

        try {
            Files.createDirectories(logDir);
        } catch (IOException e) {
            throw new UnrecoverableCorfuError("Can't create stream log directory", e);
        }
    }

    /**
     * This method will scan the log (i.e. read all log segment files)
     * on this LU and create a map of stream offsets and the global
     * addresses seen.
     *
     * consecutive segments from [startSegment, endSegment]
     */
    private void initializeLogMetadata() {
        // TODO: re-visit after compaction is done
        long startingSegment = segmentManager.getSegmentOrdinal(dataStore.getStartingAddress());
        long tailSegment = dataStore.getTailSegment();

        long start = System.currentTimeMillis();

        // Scan the log in reverse, this will ease stream trim mark resolution (as we require the
        // END records of a checkpoint which are always the last entry in this stream)
        // Note: if a checkpoint END record is not found (i.e., incomplete) this data is not considered
        // for stream trim mark computation.
        for (long currentSegment = tailSegment; currentSegment >= startingSegment; currentSegment--) {
            // TODO: change this after merge segment is introduced
            long startAddress = currentSegment * logParams.recordsPerSegment + 1;
            StreamLogSegment streamSegment = segmentManager.getStreamLogSegment(startAddress);

            for (Long address : streamSegment.getKnownAddresses().keySet()) {
                // TODO: do we need to check trimmed addresses?
                LogData logEntry = read(address);
                logMetadata.update(logEntry, true);
            }

            // Close and remove the reference of the unprotected segments.
            if (streamSegment.getOrdinal() <= tailSegment - logParams.protectedSegments) {
                segmentManager.close(streamSegment.getOrdinal());
            }
        }

        long end = System.currentTimeMillis();
        log.info("initializeStreamTails: took {} ms to load {}, log start {}", end - start, logMetadata, getTrimMark());
    }

    /**
     * Write the header for a Corfu log file.
     *
     * @param fileChannel The file channel to use.
     * @param version     The version number to append to the header.
     * @param verify      Checksum verify flag
     * @throws IOException I/O exception
     */
    public static void writeHeader(FileChannel fileChannel, int version, boolean verify) throws IOException {

        LogHeader header = LogHeader.newBuilder()
                .setVersion(version)
                .setVerifyChecksum(verify)
                .build();

        ByteBuffer buf = getByteBufferWithMetaData(header);
        safeWrite(fileChannel, buf);
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

    static ByteBuffer getByteBufferWithMetaData(AbstractMessage message) {
        Metadata metadata = getMetadata(message);
        return getByteBuffer(metadata, message);
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

    private void syncTailSegment(long address) {
        // TODO(Maithem) since writing a record and setting the tail segment is not
        // an atomic operation, it is possible to set an incorrect tail segment. In
        // that case we will need to scan more than one segment
        logMetadata.updateGlobalTail(address);
        long newTailSegment = segmentManager.getSegmentOrdinal(address);

        dataStore.updateTailSegment(newTailSegment);
    }

    @Override
    public void prefixTrim(long address) {
        if (isTrimmed(address)) {
            log.warn("prefixTrim: Ignoring repeated trim {}", address);
            return;
        }

        // TODO(Maithem): Although this operation is persisted to disk,
        // the startingAddress can be lost even after the method has completed.
        // This is due to the fact that updates on the local datastore don't
        // expose disk sync functionality.
        long newStartingAddress = address + 1;
        dataStore.updateStartingAddress(newStartingAddress);
        syncTailSegment(address);
        log.debug("Trimmed prefix, new starting address {}", newStartingAddress);

        // Trim address space maps.
        logMetadata.prefixTrim(address);
    }

    private boolean isTrimmed(long address) {
        return address < dataStore.getStartingAddress();
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
    public void sync(boolean force) throws IOException {
        if (force) {
            for (FileChannel ch : channelsToSync) {
                ch.force(true);
            }
        }
        log.trace("Sync'd {} channels", channelsToSync.size());
        channelsToSync.clear();
    }

    @Override
    public synchronized void compact() {
        trimPrefix();
    }

    @Override
    public long getTrimMark() {
        return dataStore.getStartingAddress();
    }

    private void trimPrefix() {
        // Trim all segments up till the segment that contains the starting address
        // (i.e. trim only complete segments)
        long endSegment = segmentManager.getSegmentOrdinal(dataStore.getStartingAddress()) - 1;

        if (endSegment < 0) {
            log.debug("Only one segment detected, ignoring trim");
            return;
        }

        segmentManager.cleanAndClose(endSegment);

        log.info("trimPrefix: completed, end segment {}", endSegment);
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

        // Check if range overlaps more than two segments.
        if (!verifyRangeWrite(range)) {
            throw new IllegalArgumentException("Write range not consecutive or " +
                    "too large. Range size: " + range.size());
        }

        // Append to first segment.
        LogData first = range.get(0);
        StreamLogSegment firstSegment = segmentManager.getStreamLogSegment(first.getGlobalAddress());
        List<LogData> firstSegEntries = range
                .stream()
                .filter(entry -> segmentManager
                        .getSegmentOrdinal(entry.getGlobalAddress()) == firstSegment.getOrdinal())
                .collect(Collectors.toList());

        firstSegment.append(firstSegEntries);

        updateGlobalMetaData(firstSegEntries.get(firstSegEntries.size() - 1).getGlobalAddress(),
                firstSegment.getWriteChannel(), firstSegEntries);

        // Append to second segment if range spans two segments.
        if (firstSegEntries.size() < range.size()) {
            LogData last = range.get(range.size() - 1);
            StreamLogSegment lastSegment = segmentManager.getStreamLogSegment(last.getGlobalAddress());
            List<LogData> lastSegEntries = range
                    .stream()
                    .skip(firstSegEntries.size())
                    .collect(Collectors.toList());

            lastSegment.append(lastSegEntries);

            updateGlobalMetaData(lastSegEntries.get(lastSegEntries.size() - 1).getGlobalAddress(),
                    lastSegment.getWriteChannel(), lastSegEntries);
        }
    }

    @Override
    public void append(long address, LogData entry) {
        // TODO: check entry type, append to garbage segment.
        StreamLogSegment segment = segmentManager.getStreamLogSegment(address);
        segment.append(address, entry);

        updateGlobalMetaData(address, segment.getWriteChannel(), Collections.singletonList(entry));
    }

    private void updateGlobalMetaData(long lastAddress, FileChannel channelToSync, List<LogData> entries) {
        channelsToSync.add(channelToSync);
        syncTailSegment(lastAddress);
        logMetadata.update(entries);
    }

    @Override
    public LogData read(long address) {
        // TODO: deal with (return) snapshot(compaction) mark?
        StreamLogSegment segment = segmentManager.getStreamLogSegment(address);
        return segment.read(address);
    }

    @Override
    public void close() {
        segmentManager.close();
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

        segmentManager.cleanAndClose(endSegment);

        // TODO: stop compactor, also any race here involving compaction?
        dataStore.resetStartingAddress();
        dataStore.resetTailSegment();
        logMetadata = new LogMetadata();
        log.info("reset: Completed, end segment {}", endSegment);
    }

    @VisibleForTesting
    Set<FileChannel> getChannelsToSync() {
        return channelsToSync;
    }

    //================File Operation Utilities (Parsing & I/O)================//

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
            log.error("Meta data has wrong size. Actual size: {}, expected: {}",
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
            // Partial write on the metadata for the header
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
     * @param channel  file channel
     * @param metadata meta data
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
    static void safeWrite(FileChannel channel, ByteBuffer buf) throws IOException {
        long prev = channel.position();
        try {
            while (buf.hasRemaining()) {
                channel.write(buf);
            }
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
}
