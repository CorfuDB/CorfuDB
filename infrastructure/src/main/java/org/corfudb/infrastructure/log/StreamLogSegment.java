package org.corfudb.infrastructure.log;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.corfudb.format.Types;
import org.corfudb.format.Types.LogEntry;
import org.corfudb.format.Types.Metadata;
import org.corfudb.infrastructure.ResourceQuota;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.corfudb.infrastructure.log.StreamLog.assertAppendPermittedUnsafe;
import static org.corfudb.infrastructure.log.StreamLog.getOverwriteCauseForAddress;
import static org.corfudb.infrastructure.log.StreamLogFiles.getByteBuffer;
import static org.corfudb.infrastructure.log.StreamLogFiles.getDataCorruptionErrorMessage;
import static org.corfudb.infrastructure.log.StreamLogFiles.getLogData;
import static org.corfudb.infrastructure.log.StreamLogFiles.getLogEntry;
import static org.corfudb.infrastructure.log.StreamLogFiles.getMetadata;
import static org.corfudb.infrastructure.log.StreamLogFiles.parseEntry;
import static org.corfudb.infrastructure.log.StreamLogFiles.parseHeader;
import static org.corfudb.infrastructure.log.StreamLogFiles.parseMetadata;
import static org.corfudb.infrastructure.log.StreamLogFiles.writeByteBuffer;
import static org.corfudb.infrastructure.log.StreamLogFiles.writeHeader;
import static org.corfudb.infrastructure.log.StreamLogParams.METADATA_SIZE;
import static org.corfudb.infrastructure.log.StreamLogParams.VERSION;

/**
 * The global log is partition into segments, each segment contains a range of consecutive
 * addresses. Accessing the address space for a particular segment happens through this class.
 *
 * @author Maithem
 */
@Slf4j
@Getter
class StreamLogSegment extends AbstractLogSegment {

    // Entry index map of the global logical address to physical offset in this file.
    private final Map<Long, AddressMetaData> knownAddresses = new ConcurrentHashMap<>();

    StreamLogSegment(long segmentAddress, String fileName,
                     StreamLogParams logParams,
                     ResourceQuota logSizeQuota,
                     SegmentMetaData segmentMetaData) {
        super(segmentAddress, fileName, logParams, logSizeQuota, segmentMetaData);
    }

    /**
     * Read a log entry in a stream log segment.
     *
     * @param address the address of the entry to read
     * @return the log unit entry at that address, or NULL if there is no entry
     */
    public LogData read(long address) {
        try {
            // TODO: re-visit after compaction is implemented:
            // TODO: add tombstone check
            return readRecord(address);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read a log entry in a file.
     *
     * @param address the address of the entry to read
     * @return the log unit entry at that address, or NULL if there is no entry
     */
    private LogData readRecord(long address) throws IOException {
        AddressMetaData metaData = knownAddresses.get(address);
        if (metaData == null) {
            return null;
        }

        try {
            ByteBuffer entryBuf = ByteBuffer.allocate(metaData.length);
            readChannel.read(entryBuf, metaData.offset);
            return getLogData(LogEntry.parseFrom(entryBuf.array()));
        } catch (InvalidProtocolBufferException e) {
            String errorMessage = getDataCorruptionErrorMessage(
                    "Invalid entry", filePath, writeChannel);
            throw new DataCorruptionException(errorMessage, e);
        }
    }

    /**
     * Append an entry to the stream log segment file.
     *
     * @param address address of append entry
     * @param entry entry to append to the file
     */
    public void append(long address, LogData entry) {
        // TODO: if not keeping tombstone, check for trimmed addresses.
        try {
            // Check if the entry exists.
            if (knownAddresses.containsKey(address)) {
                processOverwriteEntry(address, entry);
            }

            AddressMetaData addressMetaData = writeRecord(address, entry);
            knownAddresses.put(address, addressMetaData);
            segmentMetaData.updatePayloadStats(Collections.singletonList(entry));
            log.trace("append[{}]: Written one entry to disk.", address);

        } catch (IOException e) {
            log.error("append[{}]: IOException", address, e);
            throw new RuntimeException(e);
        }
    }

    private void processOverwriteEntry(long address, LogData entry) {
        LogData currentEntry = read(address);
        // Check rank if using quorum replication.
        if (entry.getRank() != null) {
            // The method below might throw DataOutrankedException or ValueAdoptedException.
            assertAppendPermittedUnsafe(address, currentEntry, entry);
        } else {
            OverwriteCause overwriteCause = getOverwriteCauseForAddress(currentEntry, entry);
            log.trace("append[{}]: Overwritten exception, cause: {}", address, overwriteCause);
            throw new OverwriteException(overwriteCause);
        }
    }

    /**
     * Write a log entry record to a file.
     *
     * @param address the address of the entry
     * @param entry   the LogData to append
     * @return metadata for the written record
     */
    private AddressMetaData writeRecord(long address, LogData entry) throws IOException {
        // TODO: add lock to sync delta during rewrite segment.
        LogEntry logEntry = getLogEntry(address, entry);
        Metadata metadata = getMetadata(logEntry);

        ByteBuffer record = getByteBuffer(metadata, logEntry);

        long channelOffset;
        channelOffset = writeChannel.position() + METADATA_SIZE;
        writeByteBuffer(writeChannel, record, logSizeQuota);

        return new AddressMetaData(metadata.getPayloadChecksum(), metadata.getLength(), channelOffset);
    }

    /**
     * Append a range of consecutive entries ordered by addresses to the
     * stream log segment file.
     *
     * @param entries entries to append to the file
     */
    public void append(List<LogData> entries) {
        // TODO: if not keeping tombstone, check for trimmed addresses.
        try {
            // Check if any entry exists.
            for (LogData entry : entries) {
                long address = entry.getGlobalAddress();
                if (knownAddresses.containsKey(address)) {
                    processOverwriteEntry(address, entry);
                }
            }

            Map<Long, AddressMetaData> addressMetaData = writeRecords(entries);
            knownAddresses.putAll(addressMetaData);
            segmentMetaData.updatePayloadStats(entries);
            log.trace("append[{}]: Written entries to disk.", entries);

        } catch (IOException e) {
            log.error("append[{}]: IOException", entries, e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Write a list of LogData entries to the log file.
     *
     * @param entries list of LogData entries to write.
     * @return A map of AddressMetaData for the written records
     * @throws IOException IO exception
     */
    private Map<Long, AddressMetaData> writeRecords(List<LogData> entries) throws IOException {
        // TODO: add lock to sync delta during rewrite segment.
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
     * Reads the whole address space of this segment file and update the in-memory
     * index of each entry read.
     */
    void readAddressSpace() throws IOException {
        // TODO: re-visit after compaction is implemented
        writeChannel.position(0);

        Types.LogHeader header = parseHeader(writeChannel, filePath);
        if (header == null) {
            log.warn("Couldn't find log header for {}, creating new header.", filePath);
            writeHeader(writeChannel, logSizeQuota, VERSION, logParams.verifyChecksum);
            return;
        }

        while (writeChannel.size() - writeChannel.position() > 0) {
            long channelOffset = writeChannel.position();
            Metadata metadata = parseMetadata(writeChannel, filePath);
            LogEntry entry = parseEntry(writeChannel, metadata, filePath, logParams);

            if (entry == null) {
                // Metadata or Entry were partially written
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
                return;
            }

            AddressMetaData addressMetadata = new AddressMetaData(
                    metadata.getPayloadChecksum(),
                    metadata.getLength(),
                    channelOffset + METADATA_SIZE);
            knownAddresses.put(entry.getGlobalAddress(), addressMetadata);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        try {
            writeChannel.force(true);
        } catch (IOException e) {
            log.debug("StreamLogSegment: Can't force updates write channel for file: {}", filePath, e);
        } finally {
            IOUtils.closeQuietly(readChannel);
            IOUtils.closeQuietly(scanChannel);
            IOUtils.closeQuietly(writeChannel);
        }
    }
}