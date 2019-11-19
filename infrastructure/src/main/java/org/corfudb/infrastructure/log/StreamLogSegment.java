package org.corfudb.infrastructure.log;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.format.Types.LogEntry;
import org.corfudb.format.Types.Metadata;
import org.corfudb.infrastructure.ResourceQuota;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OverwriteCause;
import org.corfudb.runtime.exceptions.OverwriteException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.corfudb.infrastructure.log.StreamLog.assertAppendPermittedUnsafe;
import static org.corfudb.infrastructure.log.StreamLog.getOverwriteCauseForAddress;
import static org.corfudb.infrastructure.log.StreamLogFiles.getDataCorruptionErrorMessage;
import static org.corfudb.infrastructure.log.StreamLogFiles.getLogData;

/**
 * The global log is partition into segments, each segment contains a range of consecutive
 * addresses. Accessing the address space for a particular segment happens through this class.
 *
 * @author Maithem
 */
@Slf4j
@Getter
class StreamLogSegment extends AbstractLogSegment {

    private final StreamLogDataStore dataStore;

    // Entry index map of the global logical address to physical offset in this file.
    private final Map<Long, AddressMetaData> knownAddresses = new ConcurrentHashMap<>();

    StreamLogSegment(long ordinal, StreamLogParams logParams,
                     String filePath, ResourceQuota logSizeQuota,
                     CompactionMetadata compactionMetaData,
                     StreamLogDataStore dataStore) {
        super(ordinal, logParams, filePath, logSizeQuota, compactionMetaData);
        this.dataStore = dataStore;
    }

    /**
     * Given an address in this segment, read the corresponding
     * stream log entry.
     *
     * @param address the address of the entry to read
     * @return stream log entry if it exists, otherwise return null
     */
    @Override
    public LogData read(long address) {
        try {
            // Check if the entry exists.
            if (!contains(address)) {
                return null;
            }

            // If the address exists but not found in index,
            // it means the entire address was compacted.
            AddressMetaData metaData = knownAddresses.get(address);
            if (metaData == null) {
                return LogData.getCompacted(address);
            }

            return readRecord(metaData);
        } catch (ClosedChannelException cce) {
            log.warn("Segment channel closed. Segment: {}, file: {}", ordinal, filePath);
            throw new ClosedSegmentException(cce);
        } catch (IOException ioe) {
            log.error("read[{}]: IOException when reading an entry.", address, ioe);
            throw new RuntimeException(ioe);
        } finally {
            release();
        }
    }

    /**
     * Read a log entry in a file.
     *
     * @param metaData the AddressMetaData that contains the file index
     * @return the log unit entry at that address, or NULL if there is no entry
     */
    private LogData readRecord(AddressMetaData metaData) throws IOException {
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
     * @param entry   entry to append to the file
     */
    @Override
    public void append(long address, LogData entry) {
        try {
            // Check if the entry exists.
            if (contains(address)) {
                processOverwriteEntry(address, entry);
            }

            AddressMetaData addressMetaData = writeRecord(address, entry);
            knownAddresses.put(address, addressMetaData);
            compactionMetaData.updateTotalPayloadSize(Collections.singletonList(entry));
            log.trace("append[{}]: Written one entry to disk.", address);

        } catch (ClosedChannelException cce) {
            log.warn("append[{}]: Segment channel closed. Segment: {}, file: {}",
                    entry, ordinal, filePath);
            throw new ClosedSegmentException(cce);
        } catch (IOException ioe) {
            log.error("append[{}]: IOException when writing an entry.", address, ioe);
            throw new RuntimeException(ioe);
        } finally {
            release();
        }
    }

    /**
     * Append a range of consecutive entries ordered by addresses to the
     * stream log segment file.
     *
     * @param entries entries to append to the file
     */
    @Override
    public void append(List<LogData> entries) {
        try {
            if (entries.isEmpty()) {
                return;
            }

            // Check if the entry exists.
            if (containsAny(entries)) {
                log.debug("append: Overwritten exception, entries: {}", entries);
                throw new OverwriteException(OverwriteCause.SAME_DATA);
            }

            Map<Long, AddressMetaData> addressMetaData = writeRecords(entries);
            knownAddresses.putAll(addressMetaData);
            compactionMetaData.updateTotalPayloadSize(entries);
            log.trace("append: Written entries to disk: {}", entries);

        } catch (ClosedChannelException cce) {
            log.warn("append: Segment channel closed. Segment: {}, file: {}", ordinal, filePath);
            throw new ClosedSegmentException(cce);
        } catch (IOException ioe) {
            log.error("append: IOException when writing entries: {}", entries, ioe);
            throw new RuntimeException(ioe);
        } finally {
            release();
        }
    }

    /**
     * Append list of possibly compacted entries to the log segment
     * file, which does not check for overwrite as this is only being
     * called during segment rewrite.
     *
     * @param entries entries to append to the file
     */
    @Override
    public void appendCompacted(List<LogData> entries) {
        try {
            if (entries.isEmpty()) {
                return;
            }
            Map<Long, AddressMetaData> addressMetaData = writeRecords(entries);
            knownAddresses.putAll(addressMetaData);
            compactionMetaData.updateTotalPayloadSize(entries);
            log.trace("appendCompacted: Written entries to disk: {}", entries);
        } catch (IOException ioe) {
            log.error("appendCompacted: IOException when writing entries: {}", entries, ioe);
            throw new RuntimeException(ioe);
        }
    }

    /**
     * Check if the entry exists (not a hole) in this segment, including compacted.
     */
    boolean contains(long address) {
        // Addresses less than or equal to committedTail are consolidated and could be
        // compacted, if an address is not in index, then it is a hole if and only if
        // it is greater than committedTail.
        return knownAddresses.containsKey(address) || address <= dataStore.getCommittedTail();
    }

    /**
     * Check if any entry in a list exists (not a hole) in this segment, including compacted.
     */
    private boolean containsAny(List<LogData> entries) {
        // Assumes the entries are ordered by address, which is checked by upper layer.
        if (entries.get(entries.size() - 1).getGlobalAddress() <= dataStore.getCommittedTail()) {
            return true;
        }
        return entries.stream().anyMatch(entry -> knownAddresses.containsKey(entry.getGlobalAddress()));
    }

    private void processOverwriteEntry(long address, LogData entry) {
        LogData currentEntry = read(address);
        // Check rank if using quorum replication.
        if (entry.getRank() != null) {
            // The method below might throw DataOutrankedException or ValueAdoptedException.
            assertAppendPermittedUnsafe(address, currentEntry, entry);
        } else {
            OverwriteCause overwriteCause = getOverwriteCauseForAddress(currentEntry, entry);
            log.debug("append[{}]: Overwritten exception, cause: {}", address, overwriteCause);
            throw new OverwriteException(overwriteCause);
        }
    }

    /**
     * Loads the entire address space of this segment file and
     * update the in-memory index of each entry read. If this
     * is a new segment, a log header will be appended.
     */
    @Override
    public void loadAddressSpace() {
        for (IndexedLogEntry indexedEntry : this) {
            Metadata metadata = indexedEntry.entryMetadata;
            AddressMetaData addressMetadata = new AddressMetaData(
                    metadata.getPayloadChecksum(),
                    metadata.getLength(),
                    indexedEntry.entryOffset);

            knownAddresses.put(indexedEntry.logEntry.getGlobalAddress(), addressMetadata);
            LogData logData = getLogData(indexedEntry.logEntry);
            compactionMetaData.updateTotalPayloadSize(Collections.singletonList(logData));
        }
    }
}