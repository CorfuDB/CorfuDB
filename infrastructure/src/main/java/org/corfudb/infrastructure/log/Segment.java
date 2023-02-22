package org.corfudb.infrastructure.log;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.infrastructure.ResourceQuota;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.DataCorruptionException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.corfudb.infrastructure.log.SegmentUtils.getByteBuffer;
import static org.corfudb.infrastructure.log.SegmentUtils.getLogData;
import static org.corfudb.infrastructure.log.SegmentUtils.getLogEntry;
import static org.corfudb.infrastructure.log.SegmentUtils.getMetadata;
import static org.corfudb.infrastructure.log.SegmentUtils.getSegmentHeader;
import static org.corfudb.infrastructure.utils.Crc32c.getChecksum;
import static org.corfudb.infrastructure.utils.Persistence.syncDirectory;

/**
 *
 * A segment is a dense range of sequence numbers that are mapped to log entries. Each segment
 * is backed by a single file and maintains a mapping from (sequence number) -> (file offset, payload length).
 * A segment maintains an in-memory hash index to file locations to speed up random reads and lookups.
 *
 * The concurrency model supported by a segment is a single writer, multiple readers model where
 * write operations must be owned by a single thread, while multiple threads can concurrently do
 * read operations.
 *
 * @author Maithem
 */
@Slf4j
public class Segment {
    public static final int METADATA_SIZE = LogFormat.Metadata.newBuilder()
            .setLengthChecksum(-1)
            .setPayloadChecksum(-1)
            .setLength(-1)
            .build()
            .getSerializedSize();

    public static final int VERSION = 2;

    final long id;

    @NonNull
    private FileChannel writeChannel;

    @NonNull
    private FileChannel readChannel;

    @NonNull
    String segmentFilePath;

    private boolean isDirty;

    private final Map<Long, AddressMetaData> knownAddresses = new ConcurrentHashMap<>();

    private int refCount = 0;

    private final ResourceQuota logSize;

    public Segment(long segmentId, Path segmentsDir, ResourceQuota logSize) {
        this.id = segmentId;
        this.segmentFilePath = segmentsDir + File.separator + segmentId + ".log";
        this.isDirty = false;
        this.logSize = logSize;
        // Open and load a segment file, or create one if it doesn't exist.
        // Once the segment address space is loaded, it should be ready to accept writes.
        try {
            this.writeChannel = getChannel(this.segmentFilePath, false);
            this.readChannel = getChannel(this.segmentFilePath, true);
            loadSegmentIndex();
        } catch (IOException e) {
            log.error("Error opening file {}", segmentFilePath, e);
            IOUtils.closeQuietly(writeChannel);
            IOUtils.closeQuietly(readChannel);
            throw new IllegalStateException(e);
        } catch (RuntimeException e) {
            IOUtils.closeQuietly(writeChannel);
            IOUtils.closeQuietly(readChannel);
            throw e;
        }
    }

    /**
     * Loads a segment index from file, or just creates a new segment if there doesn not exist a corresponding
     * segment file
     *
     * @throws IOException
     */
    private void loadSegmentIndex() throws IOException {

        writeChannel.position(0);

        LogFormat.LogHeader header = parseHeader(writeChannel);
        if (header == null) {
            log.warn("Couldn't find log header for {}, creating new header.", segmentFilePath);
            writeBuffer(getSegmentHeader(VERSION));
            return;
        }

        if (header.getVersion() != VERSION) {
            String msg = String.format("Log version %s for %s should match the LogUnit log version %s",
                    header.getVersion(), segmentFilePath, VERSION);
            throw new IllegalStateException(msg);
        }

        while (writeChannel.size() - writeChannel.position() > 0) {
            long channelOffset = writeChannel.position();
            LogFormat.Metadata metadata = parseMetadata(writeChannel);
            LogFormat.LogEntry entry = parseEntry(writeChannel, metadata);

            if (entry == null) {
                // Metadata or Entry were partially written
                log.warn("Malformed entry, metadata {} in file {}", metadata, segmentFilePath);

                // Note that after rewinding the channel pointer, it is important to truncate
                // any bytes that were written. This is required to avoid an ambiguous case
                // where a subsequent write (after a failed write) succeeds but writes less
                // bytes than the partially written buffer. In that case, the log unit can't
                // determine if the bytes correspond to a partially written buffer that needs
                // to be ignored, or if the bytes correspond to a corrupted metadata field.
                writeChannel.truncate(writeChannel.position());
                writeChannel.force(true);
                return;
            }

            knownAddresses.put(entry.getGlobalAddress(),
                    new AddressMetaData(metadata.getLength(),
                            channelOffset + METADATA_SIZE));
        }
    }

    /**
     * Get a set of all the written addresses in this segment
     * @return A set of sequence numbers
     */
    public Set<Long> getAddresses() {
        return knownAddresses.keySet();
    }

    /**
     * Check if the segment has been flushed to secondary storage
     */
    public boolean isDirty() {
        return this.isDirty;
    }

    /**
     * Fsync the segment.
     */
    public void flush() throws IOException {
        writeChannel.force(true);
        isDirty = false;
    }

    /**
     * Check if the segment contains a particular address
     */
    public boolean contains(long address) {
        return knownAddresses.containsKey(address);
    }

    /**
     * Read the log data for a particular address in this segment
     * @param address sequence number to read
     * @return log entry that is mapped to the address sequence
     * @throws IOException
     */
    public LogData read(long address) throws IOException {
        AddressMetaData addressMetaData = knownAddresses.get(address);
        if (addressMetaData == null) {
            return null;
        }

        int length = addressMetaData.length;
        long offset = addressMetaData.offset;

        try {
            ByteBuffer entryBuf = ByteBuffer.allocate(length);
            readChannel.read(entryBuf, offset);
            LogData logData = getLogData(LogFormat.LogEntry.parseFrom(entryBuf.array()));
            MicroMeterUtils.measure(length, "logunit.read.throughput");
            return logData;
        } catch (InvalidProtocolBufferException e) {
            String errorMessage = getDataCorruptionErrorMessage("Invalid entry",
                    readChannel, segmentFilePath
            );
            throw new DataCorruptionException(errorMessage, e);
        }
    }

    /**
     * Write log data to this segment for a following sequence
     * @param address the sequence to bind the log data to
     * @param logdata the log data to write
     * @return the number of bytes written
     * @throws IOException
     */
    public long write(long address, LogData logdata) throws IOException {

        LogFormat.LogEntry logEntry = getLogEntry(address, logdata);
        LogFormat.Metadata metadata = getMetadata(logEntry);

        ByteBuffer buffer = getByteBuffer(metadata, logEntry);
        long size = buffer.remaining();
        long channelOffset;

        channelOffset = writeChannel.position() + METADATA_SIZE;
        writeBuffer(buffer);
        AddressMetaData addressMetaData = new AddressMetaData(metadata.getLength(), channelOffset);
        knownAddresses.put(address, addressMetaData);
        return size;
    }

    /**
     * Write a list of LogData entries to the log file.
     *
     * @param entries list of LogData entries to write.
     * @return A map of AddressMetaData for the written records (change ret to bytes written)
     * @throws IOException IO exception
     */
    public long write(List<LogData> entries) throws IOException {
        Map<Long, AddressMetaData> recordsMap = new HashMap<>();

        List<ByteBuffer> entryBuffs = new ArrayList<>();
        int totalBytes = 0;

        List<LogFormat.Metadata> metadataList = new ArrayList<>();

        for (LogData curr : entries) {
            LogFormat.LogEntry logEntry = getLogEntry(curr.getGlobalAddress(), curr);
            LogFormat.Metadata metadata = getMetadata(logEntry);
            metadataList.add(metadata);
            ByteBuffer buf = getByteBuffer(metadata, logEntry);
            totalBytes += buf.limit();
            entryBuffs.add(buf);
        }

        ByteBuffer allRecordsBuf = ByteBuffer.allocate(totalBytes);
        long size = allRecordsBuf.remaining();

        for (int ind = 0; ind < entryBuffs.size(); ind++) {
            long channelOffset = writeChannel.position()
                    + allRecordsBuf.position() + METADATA_SIZE;
            allRecordsBuf.put(entryBuffs.get(ind));
            LogFormat.Metadata metadata = metadataList.get(ind);
            recordsMap.put(entries.get(ind).getGlobalAddress(),
                    new AddressMetaData(metadata.getLength(), channelOffset));
        }

        allRecordsBuf.flip();
        writeBuffer(allRecordsBuf);

        knownAddresses.putAll(recordsMap);

        return size;
    }

    /**
     * Attempts to write a buffer to a file channel, if write fails with an
     * IOException then the channel pointer is moved back to its original position
     * before the write
     *
     * @param buf the buffer to write
     * @throws IOException IO exception
     */
    private void writeBuffer(ByteBuffer buf) throws IOException {
        long numBytes = buf.remaining();
        Preconditions.checkArgument(numBytes > 0);
        while (buf.hasRemaining()) {
            writeChannel.write(buf);
        }
        logSize.consume(numBytes);
        isDirty = true;
    }

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

        FileChannel channel = null;

        try {
            EnumSet<StandardOpenOption> options = EnumSet.of(
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE_NEW
            );
            channel = FileChannel.open(FileSystems.getDefault().getPath(filePath), options);

            // First time creating this segment file, need to sync the parent directory
            File segFile = new File(filePath);
            syncDirectory(segFile.getParent());
            return channel;
        } catch (FileAlreadyExistsException ex) {
            return FileChannel.open(
                    FileSystems.getDefault().getPath(filePath),
                    EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE)
            );
        } catch (IOException ioe) {
            IOUtils.closeQuietly(channel);
            throw ioe;
        }
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
    private LogFormat.Metadata parseMetadata(FileChannel fileChannel) throws IOException {
        long actualMetaDataSize = fileChannel.size() - fileChannel.position();
        if (actualMetaDataSize < METADATA_SIZE) {
            log.warn("Metadata has wrong size. Actual size: {}, expected: {}",
                    actualMetaDataSize, METADATA_SIZE
            );
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(METADATA_SIZE);
        fileChannel.read(buf);
        buf.flip();

        LogFormat.Metadata metadata;

        try {
            metadata = LogFormat.Metadata.parseFrom(buf.array());
        } catch (InvalidProtocolBufferException e) {
            String errorMessage = getDataCorruptionErrorMessage("Can't parse metadata",
                    fileChannel, segmentFilePath
            );
            throw new DataCorruptionException(errorMessage, e);
        }

        if (metadata.getLengthChecksum() != getChecksum(metadata.getLength())) {
            String errorMessage = getDataCorruptionErrorMessage("Metadata: invalid length checksum",
                    fileChannel, segmentFilePath
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
                ". File position: " + fileChannel.position();
    }

    /**
     * Read a payload given metadata.
     *
     * @param fileChannel channel to read the payload from
     * @param metadata    the metadata that is written before the payload
     * @return ByteBuffer for the payload
     * @throws IOException IO exception
     */
    private ByteBuffer getPayloadForMetadata(FileChannel fileChannel, LogFormat.Metadata metadata) throws IOException {
        if (fileChannel.size() - fileChannel.position() < metadata.getLength()) {
            return null;
        }

        ByteBuffer buf = ByteBuffer.allocate(metadata.getLength());
        fileChannel.read(buf);
        buf.flip();
        return buf;
    }

    /**
     * Parse an entry.
     *
     * @param channel  file channel
     * @param metadata meta data
     * @return an log entry
     * @throws IOException IO exception
     */
    private LogFormat.LogEntry parseEntry(FileChannel channel, LogFormat.Metadata metadata)
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

        if (metadata.getPayloadChecksum() != getChecksum(buffer.array())) {
            String errorMessage = getDataCorruptionErrorMessage(
                    "Checksum mismatch detected while trying to read file",
                    channel, segmentFilePath
            );
            throw new DataCorruptionException(errorMessage);
        }


        LogFormat.LogEntry entry;
        try {
            entry = LogFormat.LogEntry.parseFrom(buffer.array());
        } catch (InvalidProtocolBufferException e) {
            String errorMessage = getDataCorruptionErrorMessage("Invalid entry",
                    channel, segmentFilePath
            );
            throw new DataCorruptionException(errorMessage, e);
        }
        return entry;
    }

    /**
     * Parse the logfile header, or create it, or recreate it if it was
     * partially written.
     *
     * @param channel file channel
     * @return log header
     * @throws IOException IO exception
     */
    private LogFormat.LogHeader parseHeader(FileChannel channel) throws IOException {
        LogFormat.Metadata metadata = parseMetadata(channel);
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

        if (getChecksum(buffer.array()) != metadata.getPayloadChecksum()) {
            String errorMessage = getDataCorruptionErrorMessage("Invalid metadata checksum",
                    channel, segmentFilePath
            );
            throw new DataCorruptionException(errorMessage);
        }

        LogFormat.LogHeader header;

        try {
            header = LogFormat.LogHeader.parseFrom(buffer.array());
        } catch (InvalidProtocolBufferException e) {
            String errorMessage = getDataCorruptionErrorMessage("Invalid header",
                    channel, segmentFilePath
            );
            throw new DataCorruptionException(errorMessage, e);
        }

        return header;
    }

    public synchronized void retain() {
        refCount++;
    }

    public synchronized void release() {
        if (refCount == 0) {
            throw new IllegalStateException("refCount cannot be less than 0, segment " + id);
        }
        refCount--;
    }

    public void close() {

        Set<FileChannel> channels = new HashSet<>(
                Arrays.asList(writeChannel, readChannel)
        );

        for (FileChannel channel : channels) {
            try {
                channel.force(true);
            } catch (IOException e) {
                log.error("Can't force updates for {}", segmentFilePath, e);
            } finally {
                IOUtils.closeQuietly(channel);
            }
        }

        if (refCount != 0) {
            log.warn("closeSegmentHandlers: Segment {} is trimmed, but refCount is {}, attempting to trim anyways",
                    segmentFilePath, refCount);
        }
    }

    @VisibleForTesting
    FileChannel getWriteChannel() {
        return writeChannel;
    }
}
