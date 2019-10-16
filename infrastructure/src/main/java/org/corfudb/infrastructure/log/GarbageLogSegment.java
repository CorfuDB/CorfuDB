package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ResourceQuota;
import org.corfudb.protocols.logprotocol.SMRGarbageEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static org.corfudb.infrastructure.log.StreamLogFiles.getLogData;

/**
 * Garbage log segment, has one-to-one mapping to a stream log segment.
 * This segment does not support random reads to the backing file, all
 * access to the garbage information go through an in-memory map which
 * keeps all the garbage information.
 * <p>
 * Created by WenbinZhu on 5/28/19.
 */
@Slf4j
public class GarbageLogSegment extends AbstractLogSegment {

    // A file lock for all GarbageLogSegment instances.
    @Getter
    static final MultiReadWriteLock segmentLock = new MultiReadWriteLock();

    @Getter
    @Setter
    private Map<Long, SMRGarbageEntry> garbageEntryMap = new ConcurrentHashMap<>();

    GarbageLogSegment(long ordinal, StreamLogParams logParams,
                      String filePath, ResourceQuota logSizeQuota,
                      CompactionMetadata compactionMetaData) {
        super(ordinal, logParams, filePath, logSizeQuota, compactionMetaData);
    }

    /**
     * loads the entire address space of this segment file
     * and update the in-memory garbage information. If this
     * is a new segment, a log header will be appended.
     */
    @Override
    public void loadAddressSpace() {
        for (IndexedLogEntry indexedEntry : this) {
            LogData logData = getLogData(indexedEntry.logEntry);
            SMRGarbageEntry garbageEntry = (SMRGarbageEntry) logData.getPayload(null);
            mergeGarbageEntry(logData.getGlobalAddress(), garbageEntry);
        }
        compactionMetaData.updateGarbageSize(new ArrayList<>(garbageEntryMap.values()));
    }

    /**
     * TODO: Add comments
     */
    @Override
    public void append(long address, LogData entry) {
        try {
            SMRGarbageEntry garbageEntry = (SMRGarbageEntry) entry.getPayload(null);
            SMRGarbageEntry uniqueGarbageEntry = mergeGarbageEntry(address, garbageEntry);

            // If no new garbage entry, don't append to garbage log file.
            if (uniqueGarbageEntry.isEmpty()) {
                return;
            }

            // If there are duplicates in the garbage entry to append, create a new
            // one with only the unique parts.
            if (uniqueGarbageEntry.getGarbageRecordCount() < garbageEntry.getGarbageRecordCount()) {
                entry.resetPayload(uniqueGarbageEntry);
            }

            writeRecord(address, entry, segmentLock);
            compactionMetaData.updateGarbageSize(Collections.singletonList(uniqueGarbageEntry));
            log.trace("append[{}]: Written one garbage entry to disk.", address);

        } catch (ClosedChannelException cce) {
            log.warn("append[{}]: Segment channel closed. Segment: {}, file: {}",
                    entry, ordinal, filePath);
            throw new ClosedSegmentException(cce);
        } catch (IOException ioe) {
            log.error("append[{}]: IOException when writing a garbage entry.", address, ioe);
            throw new RuntimeException(ioe);
        } finally {
            release();
        }
    }

    /**
     * Append a list of entries ordered by addresses to the garbage log segment file.
     *
     * @param entries entries to append to the file
     */
    @Override
    public void append(List<LogData> entries) {
        try {
            if (entries.isEmpty()) {
                return;
            }

            List<SMRGarbageEntry> uniqueGarbageEntries = new ArrayList<>();
            List<LogData> uniqueGarbageLogData = new ArrayList<>();

            for (LogData entry : entries) {
                SMRGarbageEntry garbageEntry = (SMRGarbageEntry) entry.getPayload(null);
                SMRGarbageEntry uniqueGarbageEntry = mergeGarbageEntry(
                        entry.getGlobalAddress(), garbageEntry);

                // If no new garbage info, don't append to garbage log file.
                if (uniqueGarbageEntry.isEmpty()) {
                    continue;
                }

                // If there are duplicates in the garbage entry to append, create a new
                // one with only the unique parts.
                if (uniqueGarbageEntry.getGarbageRecordCount() < garbageEntry.getGarbageRecordCount()) {
                    entry = new LogData(DataType.GARBAGE, uniqueGarbageEntry);
                }

                uniqueGarbageEntries.add(uniqueGarbageEntry);
                uniqueGarbageLogData.add(entry);
            }

            writeRecords(uniqueGarbageLogData, segmentLock);
            compactionMetaData.updateGarbageSize(uniqueGarbageEntries);

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
     * Merge the new garbage entry into the garbage entry map and return the de-duplicated
     * new garbage entry.
     *
     * @param address         global address of the garbage info entry.
     * @param newGarbageEntry new garbage entry sent from client.
     * @return de-duplicated new garbage entry after merging.
     */
    private SMRGarbageEntry mergeGarbageEntry(long address, SMRGarbageEntry newGarbageEntry) {
        // Use atomicReference as a container to bypass final variable restriction in lambda expression.
        AtomicReference<SMRGarbageEntry> uniqueGarbageEntry = new AtomicReference<>(newGarbageEntry);

        garbageEntryMap.compute(address, (addr, garbageEntry) -> {
            if (garbageEntry == null) {
                uniqueGarbageEntry.set(newGarbageEntry);
                return newGarbageEntry;
            }
            uniqueGarbageEntry.set(garbageEntry.merge(newGarbageEntry));
            return garbageEntry;
        });

        return uniqueGarbageEntry.get();
    }

    /**
     * Given an address in this segment, read the corresponding
     * garbage log entry.
     *
     * @param address address to read from the log
     * @return garbage log entry if it exists, otherwise return null
     */
    @Override
    public LogData read(long address) {
        try {
            SMRGarbageEntry garbageEntry = garbageEntryMap.get(address);
            if (garbageEntry == null || garbageEntry.getGarbageRecordCount() == 0) {
                return null;
            }

            LogData ld = new LogData(DataType.GARBAGE, garbageEntry);
            ld.setGlobalAddress(address);
            return ld;
        } finally {
            release();
        }
    }
}
