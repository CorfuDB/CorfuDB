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
import org.roaringbitmap.longlong.Roaring64NavigableMap;

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

    // A lock for all StreamLogSegment instances.
    @Getter
    private static final MultiReadWriteLock segmentLock = new MultiReadWriteLock();

    private final StreamLogDataStore dataStore;

    // Entry index map of the global logical address to physical offset in this file.
    private final Map<Long, AddressMetaData> knownAddresses = new ConcurrentHashMap<>();

    // A bitmap contains the addresses whose corresponding LogData was entirely compacted.
    private Roaring64NavigableMap compactedAddresses = new Roaring64NavigableMap();

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
            return readRecord(address);
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
     * @param address the address of the entry to read
     * @return the log unit entry at that address, or NULL if there is no entry
     */
    private LogData readRecord(long address) throws IOException {
        AddressMetaData metaData = knownAddresses.get(address);
        if (metaData == null && !compactedAddresses.contains(address)) {
            return null;
        } else if (metaData == null) {
            return LogData.getCompacted(address);
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
     * @param entry   entry to append to the file
     */
    @Override
    public void append(long address, LogData entry) {
        try {
            // Check if the entry exists.
            if (entryExists(address)) {
                processOverwriteEntry(address, entry);
            }

            AddressMetaData addressMetaData = writeRecord(address, entry, segmentLock);
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

            // Check if any entry exists.
            if (anyEntryExists(entries)) {
                log.debug("append: Overwritten exception, entries: {}", entries);
                throw new OverwriteException(OverwriteCause.SAME_DATA);
            }

            Map<Long, AddressMetaData> addressMetaData = writeRecords(entries, segmentLock);
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

    private boolean entryExists(long address) {
        return knownAddresses.containsKey(address) || compactedAddresses.contains(address);
    }

    private boolean anyEntryExists(List<LogData> entries) {
        return entries.stream().anyMatch(entry -> entryExists(entry.getGlobalAddress()));
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
        compactedAddresses = dataStore.getCompactedAddresses(ordinal);
    }
}