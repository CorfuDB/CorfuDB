package org.corfudb.infrastructure.log;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.log.AbstractLogSegment.IndexedLogEntry;
import org.corfudb.infrastructure.log.MultiReadWriteLock.AutoCloseableLock;
import org.corfudb.protocols.logprotocol.SMRGarbageEntry;
import org.corfudb.protocols.logprotocol.SMRGarbageRecord;
import org.corfudb.protocols.logprotocol.SMRLogEntry;
import org.corfudb.protocols.logprotocol.SMRRecord;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.Token;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.log.CompactionMetadata.currCompactionUpperBound;
import static org.corfudb.infrastructure.log.StreamLogFiles.getLogData;

/**
 * The stream log compactor reclaims disk spaces by leveraging
 * the garbage information identified by the runtime.
 * <p>
 * Created by WenbinZhu on 5/22/19.
 */
@Slf4j
public class StreamLogCompactor {

    // Flush the file to disk after writing this number of entries
    // to the compaction output file to avoid large IO bursts and
    // making space in page cache for normal reads/writes.
    // TODO: Do we need to expose this as a configurable parameter?
    private static final int COMPACTION_FLUSH_BATCH_SIZE = 100;

    // When compacting the garbage log segment, control the size
    // of a garbage entries batch to be rewritten.
    private static final int GARBAGE_COMPACTION_BATCH_SIZE = 10;

    private final CompactionPolicy compactionPolicy;

    private final SegmentManager segmentManager;

    private final StreamLogDataStore dataStore;

    private final LogMetadata logMetadata;

    private final ScheduledExecutorService compactionScheduler;

    private final ExecutorService compactionWorker;

    StreamLogCompactor(StreamLogParams logParams, CompactionPolicy compactionPolicy,
                       SegmentManager segmentManager, StreamLogDataStore dataStore,
                       LogMetadata logMetadata) {
        this.compactionPolicy = compactionPolicy;
        this.segmentManager = segmentManager;
        this.dataStore = dataStore;
        this.logMetadata = logMetadata;

        compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("LogUnit-Compactor-%d")
                        .build());
        compactionWorker = Executors.newFixedThreadPool(logParams.compactorWorkers);

        compactionScheduler.scheduleAtFixedRate(this::compact, logParams.compactorInitialDelay,
                logParams.compactorPeriod, logParams.compactorTimeUnit);
    }

    private void compact() {
        // TODO: update LogMetaData stream address space after compaction
        // TODO: after a segment is compacted, before deleting in-memory garbage decision,
        // need to update local datastore for per-stream compaction mark to file: ordinal.CM.ds
        try {
            // Get the segments that should be compacted according to compaction policy.
            List<Long> segmentOrdinals = compactionPolicy
                    .getSegmentsToCompact(segmentManager.getCompactibleSegments());
            log.info("Segments to compact: {}", segmentOrdinals);

            // Prepare the compaction tasks.
            List<Callable<Object>> tasks = segmentOrdinals
                    .stream()
                    .map(ordinal -> Executors.callable(() -> compactSegment(ordinal)))
                    .collect(Collectors.toList());

            // Get the results when all tasks are finished, ignoring failures.
            log.info("Started compaction tasks.");
            compactionWorker.invokeAll(tasks);
            log.info("Finished all compaction tasks in this cycle.");

        } catch (Throwable t) {
            // Suppress exceptions to avoid scheduling being terminated.
            log.error("Exception during compaction cycle", t);
        }
    }

    private void compactSegment(Long ordinal) {
        StreamLogSegment inputStreamSegment = null;
        StreamLogSegment outputStreamSegment = null;
        GarbageLogSegment inputGarbageSegment = null;
        GarbageLogSegment outputGarbageSegment = null;

        try {
            // Get a new CompactionMetadata container for the output segments.
            CompactionMetadata compactionMetaData = segmentManager.newCompactionMetadata(ordinal);
            // Open a new segment that does not contain indexing metadata to save time and memory
            // as this segment is the compaction input which is only needed for linear scanning.
            inputStreamSegment = segmentManager.newCompactionInputStreamSegment(ordinal);
            outputStreamSegment = segmentManager.newCompactionOutputStreamSegment(ordinal, compactionMetaData);
            inputGarbageSegment = segmentManager.newCompactionInputGarbageSegment(ordinal);
            outputGarbageSegment = segmentManager.newCompactionOutputGarbageSegment(ordinal, compactionMetaData);

            // Initial compaction of the StreamLogSegment.
            CompactionFeedback compactionFeedback = compactStreamSegment(
                    inputStreamSegment, outputStreamSegment, inputGarbageSegment);

            // We do not lock in the previous step to minimize the waiting time of hole fill writers.
            try (AutoCloseableLock ignored =
                         StreamLogSegment.getSegmentLock().acquireWriteLock(ordinal)) {
                // Sync the delta updates during the initial compaction.
                syncSegmentUpdate(inputStreamSegment, outputStreamSegment);
                // Update compaction marks before remapping to ensure following
                // reads can return up-to-date compaction marks.
                dataStore.updateCompactionMarks(compactionFeedback.compactionMarks);
                segmentManager.remapCompactedSegment(inputStreamSegment, outputStreamSegment);
                logMetadata.pruneStreamSpace(compactionFeedback.addressesToPrune);
            }

            // Close input and output stream segment channels.
            closeSegments(inputStreamSegment, outputStreamSegment);
            log.info("Finished compacting stream segment: {}", inputStreamSegment.getFilePath());

            // Initial compaction of the GarbageLogSegment.
            compactGarbageSegment(inputGarbageSegment,
                    outputGarbageSegment, compactionFeedback.garbageEntriesToPrune);

            // We do not lock in the previous step to minimize the waiting time of garbage writers.
            try (AutoCloseableLock ignored =
                         GarbageLogSegment.getSegmentLock().acquireWriteLock(ordinal)) {
                // Sync the delta updates during the initial compaction.
                syncSegmentUpdate(inputGarbageSegment, outputGarbageSegment);
                segmentManager.remapCompactedSegment(inputGarbageSegment, outputGarbageSegment);
            }

            // Close input and output garbage segment channels.
            closeSegments(inputGarbageSegment, outputGarbageSegment);
            log.info("Finished compacting garbage segment: {}", inputGarbageSegment.getFilePath());

        } catch (Throwable t) {
            log.error("compactSegment: encountered an exception, " +
                    "compaction on segment: {} might be uncompleted.", ordinal, t);
            closeSegments(inputStreamSegment, outputStreamSegment, inputGarbageSegment, outputGarbageSegment);
        }
    }

    private CompactionFeedback compactStreamSegment(StreamLogSegment inputStreamSegment,
                                                    StreamLogSegment outputStreamSegment,
                                                    GarbageLogSegment inputGarbageSegment) {
        log.info("Compacting stream log segment: {}", inputStreamSegment.getFilePath());
        int flushBatchCount = 0;
        Map<Long, SMRGarbageEntry> garbageInfoMap = inputGarbageSegment.getGarbageEntryMap();
        CompactionFeedback compactionFeedback = new CompactionFeedback();

        // Scan the input StreamLogSegment and compact entries.
        for (IndexedLogEntry indexedEntry : inputStreamSegment) {
            LogData logData = getLogData(indexedEntry.logEntry);
            long address = logData.getGlobalAddress();
            LogData newLogData = compactLogEntry(logData, address,
                    garbageInfoMap.get(address), compactionFeedback);
            outputStreamSegment.append(address, newLogData);

            // Flush the compaction output file after a batch to avoid IO burst.
            if (++flushBatchCount >= COMPACTION_FLUSH_BATCH_SIZE) {
                outputStreamSegment.sync();
                flushBatchCount = 0;
            }
        }

        outputStreamSegment.sync();

        return compactionFeedback;
    }

    private LogData compactLogEntry(LogData logData, long address,
                                    SMRGarbageEntry garbageEntry,
                                    CompactionFeedback compactionFeedback) {
        // If entry already compacted or no garbage info, return the same LogData.
        if (logData.getType() == DataType.COMPACTED || garbageEntry == null) {
            return logData;
        }

        // If payload is not SMRLogEntry, return the same LogData.
        Object payload = logData.getPayload(null);
        if (!(payload instanceof SMRLogEntry)) {
            log.trace("compactLogEntry: LogData payload is not of type SMRLogEntry, skip");
            return logData;
        }

        SMRLogEntry smrLogEntry = (SMRLogEntry) payload;
        SMRLogEntry compactedEntry = new SMRLogEntry();

        // Check each stream and every update in the stream.
        for (UUID streamId : smrLogEntry.getStreams()) {
            // No garbage information on this stream.
            if (garbageEntry.getGarbageRecords(streamId) == null) {
                compactedEntry.addTo(streamId, smrLogEntry.getSMRUpdates(streamId));
                continue;
            }

            // Check each SMR update in this stream.
            List<SMRRecord> smrRecords = smrLogEntry.getSMRUpdates(streamId);
            List<SMRRecord> compactedRecords = new ArrayList<>();
            for (int i = 0; i < smrRecords.size(); i++) {
                SMRRecord smrRecord = smrRecords.get(i);
                // If the record is already compacted, do not compact again.
                if (smrRecord.isCompacted()) {
                    compactedRecords.add(smrRecord);
                    compactionFeedback.addGarbageRecordToPrune(address, streamId, i);
                    continue;
                }

                SMRGarbageRecord garbageRecord = garbageEntry.getGarbageRecord(streamId, i);
                // If no corresponding garbage record -OR- the marker of garbage exceeds the
                // compaction mark upper bound set by policy, do not compact this record.
                if (garbageRecord == null || garbageRecord.getMarkerAddress() > currCompactionUpperBound) {
                    compactedRecords.add(smrRecord);
                    continue;
                }

                // If the record should be compacted, we put an empty (compacted) record.
                // It cannot simply be discarded as the we need to preserve the order
                // of the SMR updates so clients can correctly mark garbage afterwards.
                compactedRecords.add(SMRRecord.COMPACTED_RECORD);
                // Record this garbage record to later prune the GarbageLogSegment.
                compactionFeedback.addGarbageRecordToPrune(address, streamId, i);
                // Calculate the largest marker address for this stream in this segment.
                // After this segment is compacted, use this to update global compaction mark.
                compactionFeedback.updateCompactionMark(streamId, garbageRecord.getMarkerAddress());
            }

            // If all records of this stream are compacted, do not add to the entry,
            // but needs to update entry's metadata.
            if (compactedRecords.stream().allMatch(SMRRecord::isCompacted)) {
                logData.getBackpointerMap().remove(streamId);
                logData.getCompactedStreams().add(streamId);
                // Record address and stream id to later prune stream address space and garbage entries.
                compactionFeedback.addAddressToPrune(streamId, address);
                compactionFeedback.addGarbageRecordToPrune(address, streamId);
            } else {
                compactedEntry.addTo(streamId, compactedRecords);
            }
        }

        // If no stream updates in this entry after compaction, return a compacted LogData.
        if (compactedEntry.getStreams().isEmpty()) {
            logData = LogData.getCompacted(logData.getMetadataMap());
            compactionFeedback.addGarbageRecordToPrune(address);
        } else {
            // Reset the logData payload with the compacted entry.
            logData.resetPayload(compactedEntry);
        }

        return logData;
    }

    /**
     * Sync the delta updates from the {@code inputSegment} to {@code outputSegment}.
     */
    private void syncSegmentUpdate(AbstractLogSegment inputSegment,
                                   AbstractLogSegment outputSegment) {
        try {
            log.debug("Syncing delta update for segment: {}", inputSegment.getFilePath());
            FileChannel inputChannel = inputSegment.getWriteChannel();
            FileChannel outputChannel = outputSegment.getWriteChannel();
            long position = inputChannel.position();
            long count = inputChannel.size() - position;

            while (count > 0) {
                // transferTo may not transfer all requested bytes.
                long transferred = inputChannel.transferTo(position, count, outputChannel);
                position += transferred;
                count -= transferred;
            }
            outputSegment.sync();

        } catch (IOException e) {
            log.error("syncSegmentUpdate: Encountered IO exception.", e);
            throw new RuntimeException(e);
        }
    }

    private void compactGarbageSegment(GarbageLogSegment inputGarbageSegment,
                                       GarbageLogSegment outputGarbageSegment,
                                       Map<Long, Map<UUID, List<Integer>>> entriesToPrune) {
        log.info("Compacting stream log segment: {}", inputGarbageSegment.getFilePath());
        int flushBatchCount = 0;
        List<LogData> batch = new ArrayList<>();

        for (IndexedLogEntry indexedEntry : inputGarbageSegment) {
            LogData logData = getLogData(indexedEntry.logEntry);
            long address = logData.getGlobalAddress();

            SMRGarbageEntry payload = (SMRGarbageEntry) logData.getPayload(null);
            Map<UUID, List<Integer>> entryToPrune = entriesToPrune.get(address);

            // If this entry was already used to compact stream entry, prune it.
            payload.prune(entryToPrune);
            // If the entire stream entry was compacted, discard the entire garbage entry.
            if (payload.getGarbageRecordCount() == 0) {
                continue;
            }

            // Reset the logData payload with the prune garbage entry.
            logData.resetPayload(payload);

            // Write batch to the output segment.
            batch.add(logData);
            if (batch.size() >= GARBAGE_COMPACTION_BATCH_SIZE) {
                outputGarbageSegment.append(batch);
                flushBatchCount += batch.size();
                batch = new ArrayList<>();

                // Flush the compaction output file after a batch to avoid IO burst.
                if (++flushBatchCount >= COMPACTION_FLUSH_BATCH_SIZE) {
                    outputGarbageSegment.sync();
                    flushBatchCount = 0;
                }
            }
        }
    }

    private void closeSegments(AbstractLogSegment... segments) {
        for (AbstractLogSegment segment : segments) {
            if (segment != null) {
                segment.close();
            }
        }
    }

    public void shutdown() {
        // TODO: replenish this.
        compactionWorker.shutdownNow();
        compactionScheduler.shutdownNow();
    }

    /**
     * Feedback information returned after compacting a stream log segment, can be
     * used to prune metadata like garbage entries in {@code GarbageLogSegment} and
     * stream address spaces in {@code LogMetadata}.
     */
    private class CompactionFeedback {

        private Map<Long, Map<UUID, List<Integer>>> garbageEntriesToPrune = new HashMap<>();
        private Map<UUID, List<Long>> addressesToPrune = new HashMap<>();
        private Map<UUID, Long> compactionMarks = new HashMap<>();

        private void addGarbageRecordToPrune(long address, UUID streamId, int index) {
            Map<UUID, List<Integer>> entries = garbageEntriesToPrune.computeIfAbsent(
                    address, addr -> new HashMap<>());
            List<Integer> indexes = entries.computeIfAbsent(streamId, id -> new ArrayList<>());
            indexes.add(index);
        }

        @SuppressWarnings("unchecked")
        private void addGarbageRecordToPrune(long address, UUID streamId) {
            Map<UUID, List<Integer>> entries = garbageEntriesToPrune.computeIfAbsent(
                    address, addr -> new HashMap<>());
            entries.put(streamId, Collections.EMPTY_LIST);
        }

        @SuppressWarnings("unchecked")
        private void addGarbageRecordToPrune(long address) {
            garbageEntriesToPrune.put(address, Collections.EMPTY_MAP);
        }

        private void addAddressToPrune(UUID streamId, long address) {
            List<Long> addresses = addressesToPrune.computeIfAbsent(streamId, id -> new ArrayList<>());
            addresses.add(address);
        }

        private void updateCompactionMark(UUID streamId, long address) {
            compactionMarks.compute(streamId, (id, cm) -> {
                if (cm == null) {
                    return address;
                }
                return Math.max(cm, address);
            });
        }
    }
}
