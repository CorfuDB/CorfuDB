package org.corfudb.infrastructure.log;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.AtomicDouble;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.corfudb.common.compression.Codec;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.infrastructure.ResourceQuota;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.log.LogFormat.CheckpointEntryType;
import org.corfudb.infrastructure.log.LogFormat.DataType;
import org.corfudb.infrastructure.log.LogFormat.LogEntry;
import org.corfudb.infrastructure.log.LogFormat.LogHeader;
import org.corfudb.infrastructure.log.LogFormat.Metadata;
import org.corfudb.protocols.logprotocol.CheckpointEntry;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.StreamsAddressResponse;
import org.corfudb.protocols.wireprotocol.TailsResponse;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.LogUnitException;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.utils.Persistence.syncDirectory;

/**
 * This class implements the StreamLog by persisting the stream log as records in multiple files.
 * This StreamLog implementation can detect log file corruption, if checksum is enabled, otherwise
 * the checksum field will be ignored.
 *
 * <p>Created by maithem on 10/28/16.
 */

@Slf4j
public class StreamLogFiles implements StreamLog {

    public static final int METADATA_SIZE = Metadata.newBuilder()
            .setLengthChecksum(-1)
            .setPayloadChecksum(-1)
            .setLength(-1)
            .build()
            .getSerializedSize();

    public static final int VERSION = 2;
    public static final int RECORDS_PER_LOG_FILE = 10000;
    private final Path logDir;
    private final boolean verify;

    private final StreamLogDataStore dataStore;

    private ConcurrentMap<String, SegmentHandle> writeChannels;
    private final Set<FileChannel> channelsToSync;
    private final Optional<AtomicDouble> logUnitSizeBytes;
    private final Optional<AtomicLong> logUnitSizeEntries;
    private final Optional<AtomicLong> currentTrimMark;
    private final Optional<DistributionSummary> writeDistributionSummary;
    private final Optional<DistributionSummary> readDistributionSummary;
    private final Optional<AtomicLong> openSegments;
    private final Optional<Timer> fsyncTimer;
    //=================Log Metadata=================
    // TODO(Maithem) this should effectively be final, but it is used
    // by a reset API that clears the state of this class, on reset
    // a new instance of this class should be created after deleting
    // the files of the old instance
    private LogMetadata logMetadata;

    // Derived size in bytes that normal writes to the log unit are capped at.
    // This is derived as a percentage of the log's filesystem capacity.
    private final long logSizeLimit;

    // Resource quota to track the log size
    private ResourceQuota logSizeQuota;

    private final String logUnitSizeMetricName = "logunit.size";
    private final String logUnitTrimMarkMetricName = "logunit.trimmark";
    /**
     * Prevents corfu from reading and executing maintenance
     * operations (reset log unit and stream log compaction) in parallel
     */
    private final ReadWriteLock resetLock = new ReentrantReadWriteLock();

    /**
     * Returns a file-based stream log object.
     *
     * @param serverContext Context object that provides server state such as epoch,
     *                      segment and start address
     */
    public StreamLogFiles(ServerContext serverContext) {
        this(serverContext, !serverContext.getConfiguration().getVerifyChecksum());
    }

    /**
     * Returns a file-based stream log object.
     *
     * @param serverContext Context object that provides server state such as epoch,
     *                      segment and start address
     * @param noVerify      Disable checksum if true
     */
    public StreamLogFiles(ServerContext serverContext, boolean noVerify) {
        logDir = Paths.get(serverContext.getConfiguration().getLogDir());
        writeChannels = new ConcurrentHashMap<>();
        channelsToSync = new HashSet<>();
        this.verify = !noVerify;
        this.dataStore = new StreamLogDataStore(serverContext.getDataStore());

        final double logSizeLimitPercentage = serverContext.getConfiguration().getLogSizeQuota();
        if (logSizeLimitPercentage < 0.0 || 100.0 < logSizeLimitPercentage) {
            String msg = String.format("Invalid quota: quota(%f)%% must be between 0-100%%",
                    logSizeLimitPercentage);
            throw new LogUnitException(msg);
        }

        long fileSystemCapacity = initStreamLogDirectory();
        logSizeLimit = (long) (fileSystemCapacity * logSizeLimitPercentage / 100.0);

        String baseUnits = "bytes";
        logUnitSizeBytes = MeterRegistryProvider.getInstance().map(registry ->
                registry.gauge(logUnitSizeMetricName,
                        ImmutableList.of(Tag.of("unit", baseUnits)), new AtomicDouble(0)));
        logUnitSizeEntries = MeterRegistryProvider.getInstance().map(registry ->
                registry.gauge(logUnitSizeMetricName,
                        ImmutableList.of(Tag.of("unit", "entries")), new AtomicLong(0L)));
        openSegments = MeterRegistryProvider.getInstance().map(registry ->
                registry.gauge(logUnitSizeMetricName,
                        ImmutableList.of(Tag.of("unit", "segments")), new AtomicLong(0L)));
        currentTrimMark = MeterRegistryProvider.getInstance().map(registry ->
                registry.gauge(logUnitTrimMarkMetricName,
                        new AtomicLong(getTrimMark())));
        writeDistributionSummary = MeterRegistryProvider.getInstance()
                .map(registry -> DistributionSummary.builder("logunit.write.throughput")
                        .baseUnit(baseUnits).register(registry));
        readDistributionSummary = MeterRegistryProvider.getInstance()
                .map(registry -> DistributionSummary.builder("logunit.read.throughput")
                        .baseUnit(baseUnits).register(registry));

        fsyncTimer = MeterRegistryProvider.getInstance()
                .map(registry -> Timer.builder("logunit.fsync.timer")
                        .publishPercentiles(0.50, 0.99)
                        .publishPercentileHistogram(true)
                        .register(registry));
        long initialLogSize = estimateSize(logDir);
        log.info("StreamLogFiles: {} size is {} bytes, limit {}", logDir, initialLogSize, logSizeLimit);
        logSizeQuota = new ResourceQuota("LogSizeQuota", logSizeLimit);
        logSizeQuota.consume(initialLogSize);

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

    private long getStartingSegment() {
        return dataStore.getStartingAddress() / RECORDS_PER_LOG_FILE;
    }

    /**
     * Create stream log directory if not exists
     *
     * @return total capacity of the file system that owns the log files.
     */
    private long initStreamLogDirectory() {
        long fileSystemCapacity;

        try {
            if (!logDir.toFile().exists()) {
                Files.createDirectories(logDir);
            }

            String corfuDir = logDir.getParent().toString();
            FileStore corfuDirBackend = Files.getFileStore(Paths.get(corfuDir));

            File corfuDirFile = new File(corfuDir);
            if (!corfuDirFile.canWrite()) {
                throw new LogUnitException("Corfu directory is not writable " + corfuDir);
            }

            File logDirectory = new File(logDir.toString());
            if (!logDirectory.canWrite()) {
                throw new LogUnitException("Stream log directory not writable in " + corfuDir);
            }

            fileSystemCapacity = corfuDirBackend.getTotalSpace();
        } catch (IOException ioe) {
            throw new LogUnitException(ioe);
        }

        log.info("initStreamLogDirectory: initialized {}", logDir);
        return fileSystemCapacity;
    }

    /**
     * This method will scan the log (i.e. read all log segment files)
     * on this LU and create a map of stream offsets and the global
     * addresses seen.
     * <p>
     * consecutive segments from [startSegment, endSegment]
     */
    private void initializeLogMetadata() {
        long startingSegment = getStartingSegment();
        long tailSegment = dataStore.getTailSegment();

        long start = System.currentTimeMillis();
        // Scan the log in reverse, this will ease stream trim mark resolution (as we require the
        // END records of a checkpoint which are always the last entry in this stream)
        // Note: if a checkpoint END record is not found (i.e., incomplete) this data is not considered
        // for stream trim mark computation.
        for (long currentSegment = tailSegment; currentSegment >= startingSegment; currentSegment--) {
            SegmentHandle segment = getSegmentHandleForAddress(currentSegment * RECORDS_PER_LOG_FILE + 1);
            try {
                for (Long address : segment.getKnownAddresses().keySet()) {
                    // skip trimmed entries
                    if (address < dataStore.getStartingAddress()) {
                        continue;
                    }
                    LogData logEntry = read(address);
                    logUnitSizeEntries.ifPresent(AtomicLong::incrementAndGet);
                    logUnitSizeBytes.ifPresent(counter -> counter.addAndGet(logEntry.getSizeEstimate()));
                    logMetadata.update(logEntry, true);
                }
            } finally {
                segment.close();
            }
        }

        // Open segment will add entries to the writeChannels map, therefore we need to clear it
        writeChannels.clear();
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
    public void writeHeader(FileChannel fileChannel, int version, boolean verify) throws IOException {

        LogHeader header = LogHeader.newBuilder()
                .setVersion(version)
                .setVerifyChecksum(verify)
                .build();

        ByteBuffer buf = getByteBufferWithMetaData(header);
        writeByteBuffer(fileChannel, buf);
        fileChannel.force(true);
    }

    private static Metadata getMetadata(AbstractMessage message) {
        return Metadata.newBuilder()
                .setPayloadChecksum(Checksum.getChecksum(message.toByteArray()))
                .setLengthChecksum(Checksum.getChecksum(message.getSerializedSize()))
                .setLength(message.getSerializedSize())
                .build();
    }

    private static ByteBuffer getByteBuffer(Metadata metadata, AbstractMessage message) {
        ByteBuffer buf = ByteBuffer.allocate(metadata.getSerializedSize() + message.getSerializedSize());
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
    public long quotaLimitInBytes() {
        return logSizeQuota.getLimit();
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

    @Override
    public long getCommittedTail() {
        return dataStore.getCommittedTail();
    }

    @Override
    public void updateCommittedTail(long committedTail) {
        dataStore.updateCommittedTail(committedTail);
    }

    private void syncTailSegment(long address) {
        // TODO(Maithem) since writing a record and setting the tail segment is not
        // an atomic operation, it is possible to set an incorrect tail segment. In
        // that case we will need to scan more than one segment
        logMetadata.updateGlobalTail(address);
        long segment = address / RECORDS_PER_LOG_FILE;

        dataStore.updateTailSegment(segment);
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
        currentTrimMark.ifPresent(counter -> counter.set(newStartingAddress));
        // Trim address space maps.
        logMetadata.prefixTrim(address);
        logUnitSizeEntries.ifPresent(counter -> counter.addAndGet(-address));
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

            if (verify && !header.getVerifyChecksum()) {
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
                Optional<Timer.Sample> sample =
                        MicroMeterUtils.startTimer();
                ch.force(true);
                MicroMeterUtils.time(sample, "logunit.fsync.timer");
            }
        }
        log.trace("Sync'd {} channels", channelsToSync.size());
        channelsToSync.clear();
    }

    @Override
    public synchronized void compact() {
        Lock lock = resetLock.writeLock();
        lock.lock();
        try {
            trimPrefix();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long getTrimMark() {
        return dataStore.getStartingAddress();
    }

    private void trimPrefix() {
        // Trim all segments up till the segment that contains the starting address
        // (i.e. trim only complete segments)
        long endSegment = getStartingSegment() - 1;

        if (endSegment < 0) {
            log.debug("Only one segment detected, ignoring trim");
            return;
        }

        // Close segments before deleting their corresponding log files
        closeSegmentHandlers(endSegment);

        deleteFilesMatchingFilter(file -> {
            try {
                String segmentStr = file.getName().split("\\.")[0];
                return Long.parseLong(segmentStr) <= endSegment;
            } catch (Exception e) {
                log.warn("trimPrefix: ignoring file {}", file.getName());
                return false;
            }
        });

        log.info("trimPrefix: completed, end segment {}", endSegment);
    }

    private LogData getLogData(LogEntry entry) {
        ByteBuffer entryData = ByteBuffer.wrap(entry.getData().toByteArray());

        int ldCodecType = entry.hasCodecType() ? entry.getCodecType() : Codec.Type.NONE.getId();

        LogData logData = new LogData(org.corfudb.protocols.wireprotocol
                .DataType.typeMap.get((byte) entry.getDataType().getNumber()),
                Unpooled.wrappedBuffer(entryData.array()), ldCodecType);

        logData.setBackpointerMap(getUUIDLongMap(entry.getBackpointersMap()));
        logData.setGlobalAddress(entry.getGlobalAddress());

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

    //================Parsing Helper Methods================//

    /**
     * Parse the metadata field. This method should only be called
     * when a metadata field is expected.
     *
     * @param fileChannel the channel to read from
     * @return metadata field of null if it was partially written.
     * @throws IOException IO exception
     */
    private Metadata parseMetadata(FileChannel fileChannel, String segmentFile) throws IOException {
        long actualMetaDataSize = fileChannel.size() - fileChannel.position();
        if (actualMetaDataSize < METADATA_SIZE) {
            log.error("Metadata has wrong size. Actual size: {}, expected: {}",
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
            String errorMessage = getDataCorruptionErrorMessage("Can't parse metadata",
                    fileChannel, segmentFile
            );
            throw new DataCorruptionException(errorMessage, e);
        }

        if (metadata.getLengthChecksum() != Checksum.getChecksum(metadata.getLength())) {
            String errorMessage = getDataCorruptionErrorMessage("Metadata: invalid length checksum",
                    fileChannel, segmentFile
            );
            throw new DataCorruptionException(errorMessage);
        }

        return metadata;
    }

    private String getDataCorruptionErrorMessage(
            String message, FileChannel fileChannel, String segmentFile) throws IOException {
        return message +
                ". Segment File: " + segmentFile +
                ". File size: " + fileChannel.size() +
                ". File position: " + fileChannel.position() +
                ". Global tail: " + logMetadata.getGlobalTail() +
                ". Tail segment: " + dataStore.getTailSegment() +
                ". Stream tails size: " + logMetadata.getStreamTails().size();
    }

    /**
     * Read a payload given metadata.
     *
     * @param fileChannel channel to read the payload from
     * @param metadata    the metadata that is written before the payload
     * @return ByteBuffer for the payload
     * @throws IOException IO exception
     */
    private ByteBuffer getPayloadForMetadata(FileChannel fileChannel, Metadata metadata) throws IOException {
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
    private LogHeader parseHeader(FileChannel channel, String segmentFile) throws IOException {
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
            String errorMessage = getDataCorruptionErrorMessage("Invalid metadata checksum",
                    channel, segmentFile
            );
            throw new DataCorruptionException(errorMessage);
        }

        LogHeader header;

        try {
            header = LogHeader.parseFrom(buffer.array());
        } catch (InvalidProtocolBufferException e) {
            String errorMessage = getDataCorruptionErrorMessage("Invalid header",
                    channel, segmentFile
            );
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
    private LogEntry parseEntry(FileChannel channel, Metadata metadata, String fileName)
            throws IOException {

        if (metadata == null) {
            // The metadata for this entry was partial written
            return null;
        }

        ByteBuffer buffer = getPayloadForMetadata(channel, metadata);
        if (buffer == null) {
            // partial write on the entry
            // rewind the channel position to point before
            // the metadata field for this partially written payload
            channel.position(channel.position() - METADATA_SIZE);
            return null;
        }

        if (verify && metadata.getPayloadChecksum() != Checksum.getChecksum(buffer.array())) {
            String errorMessage = getDataCorruptionErrorMessage(
                    "Checksum mismatch detected while trying to read file",
                    channel, fileName
            );
            throw new DataCorruptionException(errorMessage);
        }


        LogEntry entry;
        try {
            entry = LogEntry.parseFrom(buffer.array());
        } catch (InvalidProtocolBufferException e) {
            String errorMessage = getDataCorruptionErrorMessage("Invalid entry",
                    channel, fileName
            );
            throw new DataCorruptionException(errorMessage, e);
        }
        return entry;
    }

    /**
     * Reads an address space from a log file into a SegmentHandle.
     *
     * @param segment Object containing state for the segment to be read
     */
    private void readAddressSpace(SegmentHandle segment) throws IOException {
        FileChannel fileChannel = segment.getWriteChannel();
        fileChannel.position(0);

        LogHeader header = parseHeader(fileChannel, segment.getFileName());
        if (header == null) {
            log.warn("Couldn't find log header for {}, creating new header.", segment.getFileName());
            writeHeader(fileChannel, VERSION, verify);
            return;
        }

        while (fileChannel.size() - fileChannel.position() > 0) {
            long channelOffset = fileChannel.position();
            Metadata metadata = parseMetadata(fileChannel, segment.getFileName());
            LogEntry entry = parseEntry(fileChannel, metadata, segment.getFileName());

            if (entry == null) {
                // Metadata or Entry were partially written
                log.warn("Malformed entry, metadata {} in file {}", metadata, segment.getFileName());

                // Note that after rewinding the channel pointer, it is important to truncate
                // any bytes that were written. This is required to avoid an ambiguous case
                // where a subsequent write (after a failed write) succeeds but writes less
                // bytes than the partially written buffer. In that case, the log unit can't
                // determine if the bytes correspond to a partially written buffer that needs
                // to be ignored, or if the bytes correspond to a corrupted metadata field.
                fileChannel.truncate(fileChannel.position());
                fileChannel.force(true);
                return;
            }

            AddressMetaData addressMetadata = new AddressMetaData(
                    metadata.getPayloadChecksum(),
                    metadata.getLength(),
                    channelOffset + METADATA_SIZE
            );

            segment.getKnownAddresses().put(entry.getGlobalAddress(), addressMetadata);
        }
    }

    /**
     * Read a log entry in a file.
     *
     * @param segment The file handle to use.
     * @param address The address of the entry.
     * @return The log unit entry at that address, or NULL if there was no entry.
     */
    private LogData readRecord(SegmentHandle segment, long address) throws IOException {

        FileChannel fileChannel = segment.getReadChannel();

        AddressMetaData metaData = segment.getKnownAddresses().get(address);
        if (metaData == null) {
            return null;
        }

        try {
            ByteBuffer entryBuf = ByteBuffer.allocate(metaData.length);
            fileChannel.read(entryBuf, metaData.offset);
            LogData logData = getLogData(LogEntry.parseFrom(entryBuf.array()));
            readDistributionSummary.ifPresent(summary -> summary.record(metaData.length));
            return logData;
        } catch (InvalidProtocolBufferException e) {
            String errorMessage = getDataCorruptionErrorMessage("Invalid entry",
                    fileChannel, segment.getFileName()
            );
            throw new DataCorruptionException(errorMessage, e);
        }
    }

    @Nullable
    private FileChannel getChannel(String filePath, boolean readOnly) throws IOException {
        if (readOnly) {
            if (!new File(filePath).exists()) {
                throw new FileNotFoundException(filePath);
            }

            return FileChannel.open(
                    FileSystems.getDefault().getPath(filePath),
                    EnumSet.of(StandardOpenOption.READ)
            );
        }

        try {
            EnumSet<StandardOpenOption> options = EnumSet.of(
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE_NEW
            );
            FileChannel channel = FileChannel.open(FileSystems.getDefault().getPath(filePath), options);

            // First time creating this segment file, need to sync the parent directory
            File segFile = new File(filePath);
            syncDirectory(segFile.getParent());
            return channel;
        } catch (FileAlreadyExistsException ex) {
            return FileChannel.open(
                    FileSystems.getDefault().getPath(filePath),
                    EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE)
            );
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
    SegmentHandle getSegmentHandleForAddress(long address) {
        long segment = address / RECORDS_PER_LOG_FILE;

        String filePath = logDir + File.separator;
        filePath += segment;
        filePath += ".log";

        SegmentHandle handle = writeChannels.computeIfAbsent(filePath, a -> {
            FileChannel writeCh = null;
            FileChannel readCh = null;

            try {
                writeCh = getChannel(a, false);
                readCh = getChannel(a, true);

                SegmentHandle sh = new SegmentHandle(segment, writeCh, readCh, a);
                openSegments.ifPresent(counter -> counter.incrementAndGet());
                // The first time we open a file we should read to the end, to load the
                // map of entries we already have.
                // Once the segment address space is loaded, it should be ready to accept writes.
                readAddressSpace(sh);
                return sh;
            } catch (IOException e) {
                log.error("Error opening file {}", a, e);
                IOUtils.closeQuietly(writeCh);
                IOUtils.closeQuietly(readCh);
                throw new IllegalStateException(e);
            } catch (RuntimeException ex) {
                //Prevents file resources leaks in case of any RuntimeException.
                IOUtils.closeQuietly(writeCh);
                IOUtils.closeQuietly(readCh);
                throw ex;
            }
        });

        handle.retain();
        return handle;
    }

    private Map<String, Long> getStrLongMap(Map<UUID, Long> uuidLongMap) {
        Map<String, Long> stringLongMap = new HashMap<>();

        for (Map.Entry<UUID, Long> entry : uuidLongMap.entrySet()) {
            stringLongMap.put(entry.getKey().toString(), entry.getValue());
        }

        return stringLongMap;
    }

    @SuppressWarnings("checkstyle:abbreviationaswordinname")  // Due to deprecation
    private Map<UUID, Long> getUUIDLongMap(Map<String, Long> stringLongMap) {
        Map<UUID, Long> uuidLongMap = new HashMap<>();

        for (Map.Entry<String, Long> entry : stringLongMap.entrySet()) {
            uuidLongMap.put(UUID.fromString(entry.getKey()), entry.getValue());
        }

        return uuidLongMap;
    }

    @SuppressWarnings("checkstyle:abbreviationaswordinname") // Due to deprecation
    private Set<String> getStrUUID(Set<UUID> uuids) {
        Set<String> strUUIds = new HashSet<>();

        for (UUID uuid : uuids) {
            strUUIds.add(uuid.toString());
        }

        return strUUIds;
    }


    private LogEntry getLogEntry(long address, LogData entry) {
        ByteBuffer data = ByteBuffer.wrap(entry.getData() == null ? new byte[0] : entry.getData());

        LogEntry.Builder logEntryBuilder = LogEntry.newBuilder()
                .setDataType(DataType.forNumber(entry.getType().ordinal()))
                .setCodecType(entry.getPayloadCodecType().getId())
                .setData(ByteString.copyFrom(data))
                .setGlobalAddress(address)
                .addAllStreams(getStrUUID(entry.getStreams()))
                .putAllBackpointers(getStrLongMap(entry.getBackpointerMap()));

        if (entry.getClientId() != null && entry.getThreadId() != null) {
            logEntryBuilder.setClientIdMostSignificant(
                    entry.getClientId().getMostSignificantBits());
            logEntryBuilder.setClientIdLeastSignificant(
                    entry.getClientId().getLeastSignificantBits());
            logEntryBuilder.setThreadId(entry.getThreadId());
        }

        if (entry.hasCheckpointMetadata()) {
            logEntryBuilder.setCheckpointEntryType(
                    CheckpointEntryType.forNumber(
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

    /**
     * Write a list of LogData entries to the log file.
     *
     * @param segment segment handle to the logfile
     * @param entries list of LogData entries to write.
     * @return A map of AddressMetaData for the written records
     * @throws IOException IO exception
     */
    private Map<Long, AddressMetaData> writeRecords(SegmentHandle segment,
                                                    List<LogData> entries) throws IOException {
        Map<Long, AddressMetaData> recordsMap = new HashMap<>();

        List<ByteBuffer> entryBuffs = new ArrayList<>();
        int totalBytes = 0;

        List<Metadata> metadataList = new ArrayList<>();

        for (LogData curr : entries) {
            LogEntry logEntry = getLogEntry(curr.getGlobalAddress(), curr);
            Metadata metadata = getMetadata(logEntry);
            metadataList.add(metadata);
            ByteBuffer record = getByteBuffer(metadata, logEntry);
            totalBytes += record.limit();
            entryBuffs.add(record);
        }

        ByteBuffer allRecordsBuf = ByteBuffer.allocate(totalBytes);
        long size = allRecordsBuf.remaining();

        for (int ind = 0; ind < entryBuffs.size(); ind++) {
            long channelOffset = segment.getWriteChannel().position()
                    + allRecordsBuf.position() + METADATA_SIZE;
            allRecordsBuf.put(entryBuffs.get(ind));
            Metadata metadata = metadataList.get(ind);
            recordsMap.put(entries.get(ind).getGlobalAddress(),
                    new AddressMetaData(metadata.getPayloadChecksum(),
                            metadata.getLength(), channelOffset));
        }

        allRecordsBuf.flip();
        writeByteBuffer(segment.getWriteChannel(), allRecordsBuf);
        channelsToSync.add(segment.getWriteChannel());
        // Sync the global and stream tail(s)
        // TODO(Maithem): on ioexceptions the StreamLogFiles needs to be reinitialized
        syncTailSegment(entries.get(entries.size() - 1).getGlobalAddress());
        logMetadata.update(entries);

        logUnitSizeBytes.ifPresent(counter -> counter.addAndGet(size));
        writeDistributionSummary.ifPresent(summary -> summary.record(size));
        logUnitSizeEntries.ifPresent(counter -> counter.addAndGet(entries.size()));
        return recordsMap;
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
    private void writeByteBuffer(FileChannel channel, ByteBuffer buf) throws IOException {
        // On IOExceptions this class should be reinitialized, so consuming
        // the buffer size and failing on the write should be an issue
        logSizeQuota.consume(buf.remaining());
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
    }

    /**
     * Write a log entry record to a file.
     *
     * @param segment The file handle to use.
     * @param address The address of the entry.
     * @param entry   The LogData to append.
     * @return Returns metadata for the written record
     */
    private AddressMetaData writeRecord(SegmentHandle segment, long address,
                                        LogData entry) throws IOException {

        LogEntry logEntry = getLogEntry(address, entry);
        Metadata metadata = getMetadata(logEntry);

        ByteBuffer record = getByteBuffer(metadata, logEntry);
        long size = record.remaining();
        long channelOffset;

        channelOffset = segment.getWriteChannel().position() + METADATA_SIZE;
        writeByteBuffer(segment.getWriteChannel(), record);
        channelsToSync.add(segment.getWriteChannel());
        syncTailSegment(address);
        logMetadata.update(entry, false);

        logUnitSizeBytes.ifPresent(counter -> counter.addAndGet(size));
        writeDistributionSummary.ifPresent(summary -> summary.record(size));
        logUnitSizeEntries.ifPresent(counter -> counter.incrementAndGet());
        return new AddressMetaData(metadata.getPayloadChecksum(), metadata.getLength(), channelOffset);
    }

    private long getSegment(LogData entry) {
        return entry.getGlobalAddress() / RECORDS_PER_LOG_FILE;
    }

    /**
     * Pre-process a range of entries to be written. This includes
     * removing trimmed entries and advancing the trim mark appropriately.
     *
     * @param range range of entries
     * @return A subset of range; entries to be written
     */
    private List<LogData> preprocess(List<LogData> range) {
        List<LogData> processed = new ArrayList<>();
        for (LogData curr : range) {
            // TODO(Maithem) Add an extra check to make
            // sure that trimmed entries don't alternate
            // with non-trimmed entries
            if (curr.isTrimmed()) {
                // We don't need to write trimmed entries
                // because we already track the trim mark
                prefixTrim(curr.getGlobalAddress());
                continue;
            }

            if (isTrimmed(curr.getGlobalAddress())) {
                // This is the case where the client tries to
                // write a range of addresses, but before the
                // request starts, a prefix trim request executes
                // on this logging unit, as a consequence trimming
                // all of, or part of the range.
                continue;
            } else {
                processed.add(curr);
            }
        }
        return processed;
    }

    /**
     * This method verifies that a range of entries doesn't
     * span more than two segments and that the log addresses
     * are ordered sequentially.
     *
     * @param range entries to verify
     * @return return true if the range is valid.
     */
    private boolean verify(List<LogData> range) {

        // Make sure that entries are ordered sequentially
        long firstAddress = range.get(0).getGlobalAddress();
        for (int x = 1; x < range.size(); x++) {
            if (range.get(x).getGlobalAddress() != firstAddress + x) {
                return false;
            }
        }

        // Check if the range spans more than two segments
        long lastAddress = range.get(range.size() - 1).getGlobalAddress();
        long firstSegment = firstAddress / RECORDS_PER_LOG_FILE;
        long endSegment = lastAddress / RECORDS_PER_LOG_FILE;

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

        Lock lock = resetLock.readLock();
        lock.lock();

        try {
            Set<Long> result = new HashSet<>();
            for (long address = rangeStart; address <= rangeEnd; address++) {
                if (getSegmentHandleForAddress(address).getKnownAddresses().containsKey(address)) {
                    result.add(address);
                }
            }
            return result;
        } finally {
            lock.unlock();
        }
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
            throw new IllegalArgumentException("Write range too large: " + entries.size());
        }

        // check if the entries range cross a segment
        LogData first = entries.get(0);
        LogData last = entries.get(entries.size() - 1);
        SegmentHandle firstSh = getSegmentHandleForAddress(first.getGlobalAddress());
        SegmentHandle lastSh = getSegmentHandleForAddress(last.getGlobalAddress());

        // Extract all addresses associated with the provided write range.
        Set<Long> pendingWrites = range.stream()
                .map(ILogData::getGlobalAddress).collect(Collectors.toSet());

        // See if the provided range overlaps with any of the previously written entries.
        Set<Long> segOneOverlap = Sets.intersection(pendingWrites,
                firstSh.getKnownAddresses().keySet());
        Set<Long> segTwoOverlap = Sets.intersection(pendingWrites,
                lastSh.getKnownAddresses().keySet());
        if (!segOneOverlap.isEmpty() || !segTwoOverlap.isEmpty()) {
            log.error("Overlapping addresses detected: {}, {}", segOneOverlap, segTwoOverlap);
            throw new OverwriteException(OverwriteCause.SAME_DATA);
        }

        List<LogData> segOneEntries = new ArrayList<>();
        List<LogData> segTwoEntries = new ArrayList<>();

        for (LogData curr : entries) {
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
        if (isTrimmed(address)) {
            throw new OverwriteException(OverwriteCause.TRIM);
        }

        SegmentHandle segment = getSegmentHandleForAddress(address);

        try {
            // make sure the entry doesn't currently exist...
            // (probably need a faster way to do this - high watermark?)
            if (segment.getKnownAddresses().containsKey(address)
                    || segment.getTrimmedAddresses().contains(address)) {
                OverwriteCause overwriteCause = getOverwriteCauseForAddress(address, entry);
                log.trace("Disk_write[{}]: overwritten exception, cause: {}", address, overwriteCause);
                throw new OverwriteException(overwriteCause);
            } else {
                AddressMetaData addressMetaData = writeRecord(segment, address, entry);
                segment.getKnownAddresses().put(address, addressMetaData);
            }
            log.trace("Disk_write[{}]: Written to disk.", address);
        } catch (IOException e) {
            log.error("Disk_write[{}]: Exception", address, e);
            throw new RuntimeException(e);
        } finally {
            segment.release();
        }
    }

    @Override
    public LogData read(long address) {
        Lock lock = resetLock.readLock();
        lock.lock();

        try {
            if (isTrimmed(address)) {
                return LogData.getTrimmed(address);
            }
            SegmentHandle segment = getSegmentHandleForAddress(address);

            try {
                if (segment.getPendingTrims().contains(address)) {
                    return LogData.getTrimmed(address);
                }
                return readRecord(segment, address);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                segment.release();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean contains(long address) throws TrimmedException {
        // auto commit client is expected to get TrimmedException and
        // retry as this indicates commit counter is falling behind.
        if (isTrimmed(address)) {
            throw new TrimmedException();
        }

        if (address <= getCommittedTail()) {
            return true;
        }

        Lock lock = resetLock.readLock();
        lock.lock();

        try {
            SegmentHandle segment = getSegmentHandleForAddress(address);
            return segment.getKnownAddresses().containsKey(address);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        for (SegmentHandle fh : writeChannels.values()) {
            fh.close();
        }

        writeChannels = new ConcurrentHashMap<>();
    }

    /**
     * Closes all segment handlers up to and including the handler for the endSegment.
     *
     * @param endSegment The segment index of the last segment up to (including) the end segment.
     */
    @VisibleForTesting
    void closeSegmentHandlers(long endSegment) {
        for (SegmentHandle sh : writeChannels.values()) {
            if (sh.getSegment() > endSegment) {
                continue;
            }

            closeSegment(sh);
            openSegments.ifPresent(counter -> counter.decrementAndGet());
        }
    }

    private void closeAllSegmentHandlers() {
        for (SegmentHandle sh : writeChannels.values()) {
            closeSegment(sh);
        }
    }

    @VisibleForTesting
    void closeSegment(SegmentHandle sh) {
        if (sh.getRefCount() != 0) {
            log.warn("closeSegmentHandlers: Segment {} is trimmed, but refCount is {}, attempting to trim anyways",
                    sh.getSegment(), sh.getRefCount()
            );
        }
        channelsToSync.remove(sh.getWriteChannel());
        sh.close();
        writeChannels.remove(sh.getFileName());
    }

    /**
     * Deletes all files matching the given filter.
     *
     * @param fileFilter File filter to delete files.
     */
    private void deleteFilesMatchingFilter(FileFilter fileFilter) {
        int numFiles = 0;
        long freedBytes = 0;
        File dir = logDir.toFile();
        File[] files = dir.listFiles(fileFilter);
        if (files == null) {
            return;
        }

        for (File file : files) {
            long delta = file.length();

            if (!file.delete()) {
                log.error("deleteFilesMatchingFilter: Couldn't delete file {}", file.getName());
            } else {
                freedBytes += delta;
                numFiles++;
            }
        }
        logSizeQuota.release(freedBytes);
        final long freedBytesFinal = freedBytes;
        logUnitSizeBytes.ifPresent(counter -> counter.addAndGet(-freedBytesFinal));
        log.info("deleteFilesMatchingFilter: completed, deleted {} files, freed {} bytes", numFiles, freedBytes);
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
        log.warn("Reset. Global Tail:{}", logMetadata.getGlobalTail());

        Lock lock = resetLock.writeLock();
        lock.lock();

        try {

            // Close segments before deleting their corresponding log files
            closeAllSegmentHandlers();

            deleteFilesMatchingFilter(file -> true);

            dataStore.resetStartingAddress();
            dataStore.resetTailSegment();
            logMetadata = new LogMetadata();

            logSizeQuota = new ResourceQuota("LogSizeQuota", logSizeLimit);
            cleanUpGauges();
            log.info("reset: Completed");
        } finally {
            lock.unlock();
        }
    }

    private void cleanUpGauges() {
        String unitTag = "unit";
        ImmutableList.of(Tags.of(unitTag, "bytes"), Tags.of(unitTag, "entries"), Tags.of(unitTag, "segments"))
                .forEach(tags -> MeterRegistryProvider
                        .deregisterServerMeter(logUnitSizeMetricName, tags, Meter.Type.GAUGE));
        MeterRegistryProvider.deregisterServerMeter(logUnitTrimMarkMetricName, Tags.empty(),
                Meter.Type.GAUGE);
    }

    @VisibleForTesting
    Set<FileChannel> getChannelsToSync() {
        return channelsToSync;
    }

    @VisibleForTesting
    Collection<SegmentHandle> getOpenSegmentHandles() {
        return writeChannels.values();
    }

    public static class Checksum {

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
