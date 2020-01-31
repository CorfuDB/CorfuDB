package org.corfudb.infrastructure.log;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.corfudb.format.Types.LogEntry;
import org.corfudb.format.Types.LogHeader;
import org.corfudb.format.Types.Metadata;
import org.corfudb.infrastructure.ResourceQuota;
import org.corfudb.infrastructure.log.MultiReadWriteLock.AutoCloseableLock;
import org.corfudb.protocols.wireprotocol.LogData;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.corfudb.infrastructure.log.StreamLogFiles.getByteBuffer;
import static org.corfudb.infrastructure.log.StreamLogFiles.getLogEntry;
import static org.corfudb.infrastructure.log.StreamLogFiles.getMetadata;
import static org.corfudb.infrastructure.log.StreamLogFiles.parseEntry;
import static org.corfudb.infrastructure.log.StreamLogFiles.parseHeader;
import static org.corfudb.infrastructure.log.StreamLogFiles.parseMetadata;
import static org.corfudb.infrastructure.log.StreamLogFiles.writeByteBuffer;
import static org.corfudb.infrastructure.log.StreamLogFiles.writeHeader;
import static org.corfudb.infrastructure.log.StreamLogParams.METADATA_SIZE;
import static org.corfudb.infrastructure.log.StreamLogParams.VERSION;
import static org.corfudb.infrastructure.utils.Persistence.syncDirectory;

/**
 * An abstract Log Segment class, implements common file channel operations,
 * including opening channels, writing to channels and iterating, etc.
 * <p>
 * Created by WenbinZhu on 5/28/19.
 */
@Slf4j
@Getter
public abstract class AbstractLogSegment implements
        Iterable<AbstractLogSegment.IndexedLogEntry> {

    protected final long ordinal;

    @NonNull
    protected final StreamLogParams logParams;

    @NonNull
    protected final String filePath;

    @NonNull
    protected final ResourceQuota logSizeQuota;

    @NonNull
    protected final CompactionMetadata compactionMetaData;

    @NonNull
    protected FileChannel writeChannel;

    @NonNull
    protected FileChannel readChannel;

    protected AtomicInteger referenceCount = new AtomicInteger(0);

    private volatile boolean closing = false;

    AbstractLogSegment(long ordinal, StreamLogParams logParams,
                       String filePath, ResourceQuota logSizeQuota,
                       CompactionMetadata compactionMetaData) {
        this.ordinal = ordinal;
        this.logParams = logParams;
        this.filePath = filePath;
        this.logSizeQuota = logSizeQuota;
        this.compactionMetaData = compactionMetaData;

        try {
            this.writeChannel = getChannel(filePath, false);
            this.readChannel = getChannel(filePath, true);
        } catch (IOException ioe) {
            log.error("Error opening file {}", filePath, ioe);
            IOUtils.closeQuietly(writeChannel);
            IOUtils.closeQuietly(readChannel);
            throw new IllegalStateException(ioe);
        }
    }

    /**
     * Given an address in this segment, read the corresponding
     * log entry.
     *
     * @param address address to read from the log
     * @return log entry if it exists, otherwise return null
     */
    public abstract LogData read(long address);

    /**
     * Append an entry to the log segment file.
     *
     * @param address address of append entry
     * @param entry   entry to append to the file
     */
    public abstract void append(long address, LogData entry);

    /**
     * Append a range of consecutive entries ordered by addresses
     * to the log segment file.
     *
     * @param entries entries to append to the file
     */
    public abstract void append(List<LogData> entries);

    /**
     * Append list of possibly compacted entries to the log segment
     * file, which ignores the global committed tail.
     *
     * @param entries entries to append to the file
     */
    public abstract void appendCompacted(List<LogData> entries);

    /**
     * Reads the entire address space of this segment file
     * and update the in-memory metadata if needed. If this
     * is a new segment, a log header will be appended.
     */
    public abstract void loadAddressSpace();

    /**
     * Retain the segment, increase the reference count by one.
     */
    public void retain() {
        referenceCount.incrementAndGet();
    }

    /**
     * Release the segment, decrease the reference count by one.
     */
    public void release() {
        int refCount = referenceCount.decrementAndGet();
        if (closing && refCount <= 0) {
            close();
        }
    }

    /**
     * Write a log entry record to a file.
     *
     * @param address the address of the entry
     * @param logData the LogData to append
     * @return metadata for the written record
     */
    AddressMetaData writeRecord(long address, LogData logData) throws IOException {
        LogEntry logEntry = getLogEntry(address, logData);
        Metadata metadata = getMetadata(logEntry);

        ByteBuffer record = getByteBuffer(metadata, logEntry);
        long channelOffset;

        channelOffset = writeChannel.position() + METADATA_SIZE;
        writeByteBuffer(writeChannel, record, logSizeQuota);

        return new AddressMetaData(metadata.getPayloadChecksum(), metadata.getLength(), channelOffset);
    }

    /**
     * Write a list of LogData entries to the log file.
     *
     * @param entries list of LogData entries to write.
     * @return A map of AddressMetaData for the written records
     * @throws IOException IO exception
     */
    Map<Long, AddressMetaData> writeRecords(List<LogData> entries) throws IOException {
        Map<Long, AddressMetaData> recordsMap = new HashMap<>();

        int totalBytes = 0;
        List<Metadata> metadataList = new ArrayList<>();
        List<ByteBuffer> entryBuffs = new ArrayList<>();

        for (LogData curr : entries) {
            LogEntry logEntry = getLogEntry(curr.getGlobalAddress(), curr);
            Metadata metadata = getMetadata(logEntry);
            metadataList.add(metadata);
            ByteBuffer record = getByteBuffer(metadata, logEntry);
            entryBuffs.add(record);
            totalBytes += record.limit();
        }

        ByteBuffer allRecordsBuf = ByteBuffer.allocate(totalBytes);
        long prevPosition = writeChannel.position();
        for (int i = 0; i < entries.size(); i++) {
            long channelOffset = prevPosition + allRecordsBuf.position() + METADATA_SIZE;
            allRecordsBuf.put(entryBuffs.get(i));
            Metadata metadata = metadataList.get(i);
            AddressMetaData addressMetaData = new AddressMetaData(
                    metadata.getPayloadChecksum(), metadata.getLength(), channelOffset);
            recordsMap.put(entries.get(i).getGlobalAddress(), addressMetaData);
        }

        allRecordsBuf.flip();
        writeByteBuffer(writeChannel, allRecordsBuf, logSizeQuota);

        return recordsMap;
    }

    /**
     * Returns an iterator over the log entries of this segment.
     *
     * @return a {@link SegmentIterator}
     */
    @Nonnull
    @Override
    public Iterator<IndexedLogEntry> iterator() {
        return new SegmentIterator();
    }

    /**
     * Sync the log file to secondary storage.
     */
    public void sync() {
        try {
            if (writeChannel.isOpen()) {
                writeChannel.force(true);
            }
        } catch (ClosedChannelException cce) {
            throw new ClosedSegmentException(cce);
        } catch (IOException ioe) {
            log.error("sync: Can't flush updates for file: {}", filePath, ioe);
            throw new RuntimeException(ioe);
        }
    }

    /**
     * Request to close the segment. If the reference count is zero,
     * close immediately and free the resources, otherwise the segment
     * will be closed later once it is released and not referenced.
     *
     * @param force if true, force close the segment without checking
     *              reference count.
     */
    public void close(boolean force) {
        closing = true;
        int refCount = referenceCount.get();
        if (force || refCount <= 0) {
            if (refCount < 0) {
                log.warn("close: Segment {} reference count: {} < 0, " +
                        "might be a bug.", filePath, refCount);
            }
            close();
        }
    }

    /**
     * Close the segment, releasing the resources if any.
     */
    private synchronized void close() {
        if (writeChannel.isOpen() || readChannel.isOpen()) {
            try {
                if (writeChannel.isOpen()) {
                    writeChannel.force(true);
                }
            } catch (IOException ioe) {
                log.debug("close: Cannot force updates write channel for file: {}", filePath, ioe);
            } finally {
                IOUtils.closeQuietly(writeChannel);
                IOUtils.closeQuietly(readChannel);
            }
        }
    }

    /**
     * Opens the file channel to manipulate the file, creating a new file if
     * it does not exist. If the readOnly parameter is set, the channel will
     * be opened in read-only mode.
     *
     * @param filePath the path of the file to open
     * @param readOnly if the file should be opened only for read
     * @return the file channel to manipulate the specified file
     */
    private FileChannel getChannel(String filePath, boolean readOnly) throws IOException {
        if (readOnly) {
            if (!new File(filePath).exists()) {
                throw new FileNotFoundException(filePath);
            }
            return FileChannel.open(
                    FileSystems.getDefault().getPath(filePath), StandardOpenOption.READ);
        }

        try {
            EnumSet<StandardOpenOption> options = EnumSet.of(
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE_NEW);
            FileChannel channel = FileChannel.open(FileSystems.getDefault().getPath(filePath), options);

            // First time creating this segment file, need to sync the parent directory.
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
     * An iterator over the indexed log entries of the segment, which contain each
     * log entry by its appended order and its corresponding indexing metadata.
     * <p>
     * This iterator is incremental, which means each iteration on same segment
     * class instance will continue from the offset left from the last iteration.
     * <p>
     * If we need to iterate from beginning, a new segment class instance should
     * be created.
     */
    class SegmentIterator implements Iterator<IndexedLogEntry> {

        // If this is a new segment that does not contain a header.
        private boolean hasHeader;
        // Total size of the file before the iteration starts.
        private long fileSize;

        // Offset of this entry in the log file.
        long channelOffset;
        // Checksum and length metadata of this entry.
        Metadata metadata;
        // The actual log entry.
        LogEntry logEntry;

        SegmentIterator() {
            try {
                hasHeader = true;
                // Do not adjust writeChannel position as this class is intended to do
                // incremental traversing (next call will continue from the last call).
                fileSize = writeChannel.size();

                if (writeChannel.position() == 0) {
                    LogHeader header = parseHeader(writeChannel, filePath);
                    if (header == null) {
                        hasHeader = false;
                        log.warn("SegmentIterator: Couldn't find log header for {}, " +
                                "creating new header.", filePath);
                        writeHeader(writeChannel, logSizeQuota, VERSION, logParams.verifyChecksum);
                    }
                }
            } catch (ClosedChannelException cce) {
                log.warn("SegmentIterator: Segment channel closed. Segment: {}, file: {}", ordinal, filePath);
                throw new ClosedSegmentException(cce);
            } catch (IOException ioe) {
                log.error("SegmentIterator: Error reading file: {}", filePath, ioe);
                throw new IllegalStateException(ioe);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext() {
            try {
                if (!hasHeader || writeChannel.position() >= fileSize) {
                    return false;
                }

                channelOffset = writeChannel.position();
                metadata = parseMetadata(writeChannel, filePath);
                logEntry = parseEntry(writeChannel, metadata, filePath, logParams);

                if (logEntry == null) {
                    // Metadata or Entry were partially written.
                    log.warn("Malformed entry, metadata {} in file {}", metadata, filePath);
                    // Partial write on the entry, rewind the channel position to point before
                    // the metadata field for this partially written payload.
                    writeChannel.position(channelOffset);
                    // Note that after rewinding the channel pointer, it is important to truncate
                    // any bytes that were written. This is required to avoid an ambiguous case
                    // where a subsequent write (after a failed write) succeeds but writes less
                    // bytes than the partially written buffer. In that case, the log unit can't
                    // determine if the bytes correspond to a partially written buffer that needs
                    // to be ignored, or if the bytes correspond to a corrupted metadata field.
                    writeChannel.truncate(writeChannel.position());
                    writeChannel.force(true);
                    return false;
                }

                return true;

            } catch (ClosedChannelException cce) {
                log.warn("SegmentIterator: Segment channel closed. Segment: {}, file: {}", ordinal, filePath);
                throw new ClosedSegmentException(cce);
            } catch (IOException ioe) {
                log.error("SegmentIterator: Error reading file: {}", filePath, ioe);
                throw new IllegalStateException(ioe);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public IndexedLogEntry next() {
            return new IndexedLogEntry(channelOffset + METADATA_SIZE, metadata, logEntry);
        }
    }

    /**
     * A log entry with its corresponding indexing information.
     */
    @AllArgsConstructor
    class IndexedLogEntry {

        // Offset of this entry in the log file.
        long entryOffset;

        // Checksum and length metadata of this entry.
        Metadata entryMetadata;

        // The actual log entry.
        LogEntry logEntry;
    }
}