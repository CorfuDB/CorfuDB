package org.corfudb.infrastructure.log;

import com.google.common.annotations.VisibleForTesting;
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
import org.corfudb.runtime.view.Address;

import javax.annotation.Nullable;
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
import java.util.concurrent.TimeUnit;
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

    // Flush the file to disk after writing this number of batch
    // to the compaction output file to avoid large IO bursts and
    // making space in page cache for normal reads/writes.
    private static final int COMPACTION_FLUSH_BATCH_SIZE = 10;

    // When compacting the stream log segment, control the size
    // of batch of entries to be rewritten.
    private static final int STREAM_COMPACTION_BATCH_SIZE = 10;

    // When compacting the garbage log segment, control the size
    // of batch of entries to be rewritten.
    private static final int GARBAGE_COMPACTION_BATCH_SIZE = 20;

    private final StreamLogParams logParams;

    private final CompactionPolicy compactionPolicy;

    private final SegmentManager segmentManager;

    private final StreamLogDataStore dataStore;

    private final LogMetadata logMetadata;

    private final ScheduledExecutorService compactionScheduler;

    private final ExecutorService compactionWorker;

    StreamLogCompactor(StreamLogParams logParams, CompactionPolicy compactionPolicy,
                       SegmentManager segmentManager, StreamLogDataStore dataStore,
                       LogMetadata logMetadata) {
        this.logParams = logParams;
        this.compactionPolicy = compactionPolicy;
        this.segmentManager = segmentManager;
        this.dataStore = dataStore;
        this.logMetadata = logMetadata;

        compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("LogUnit-Compactor-%d")
                        .build());
        compactionWorker = Executors.newFixedThreadPool(logParams.compactionWorkers);
    }

    public void start() {
        compactionScheduler.scheduleAtFixedRate(this::compact,
                logParams.compactionInitialDelayMin, logParams.compactionPeriodMin, TimeUnit.MINUTES);
    }

    @VisibleForTesting
    public void compact() {
        try {
            // Get the segments that should be compacted according to compaction policy.
            List<Long> segmentOrdinals = compactionPolicy
                    .getSegmentsToCompact(segmentManager.getCompactibleSegments());

            if (segmentOrdinals.isEmpty()) {
                log.info("No segments to compact, skip");
                return;
            }

            log.info("Segments to compact: {}", segmentOrdinals);
            // Prepare the compaction tasks.
            List<Callable<Object>> tasks = segmentOrdinals
                    .stream()
                    .map(ordinal -> Executors.callable(() -> compactSegment(ordinal)))
                    .collect(Collectors.toList());

            // Get the results when all tasks are finished, ignoring failures.
            log.info("Launched compaction tasks, compacting {} segments.", tasks.size());
            long startTime = System.currentTimeMillis();
            compactionWorker.invokeAll(tasks);
            long span = System.currentTimeMillis() - startTime;
            log.info("Compaction tasks finished, took {} ms to compact {} segments", span, tasks.size());

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

            log.debug("Started compacting stream log segment: {}", inputStreamSegment.getFilePath());
            long startTime = System.currentTimeMillis();
            // Initial compaction of the StreamLogSegment.
            CompactionFeedback compactionFeedback = compactStreamSegment(
                    inputStreamSegment, outputStreamSegment, inputGarbageSegment);

            // Update global compaction mark before remapping to ensure following
            // reads can return the most up-to-date compaction mark.
            dataStore.updateGlobalCompactionMark(compactionFeedback.segmentCompactionMark);
            logMetadata.pruneStreamSpace(compactionFeedback.addressesToPrune);
            segmentManager.remapCompactedSegment(inputStreamSegment, outputStreamSegment);

            // Close input and output stream segment channels.
            closeSegments(inputStreamSegment, outputStreamSegment);
            long span = System.currentTimeMillis() - startTime;
            log.debug("Compacted stream log segment: {}, took {} ms", inputStreamSegment.getFilePath(), span);

            log.debug("Started compacting garbage log segment: {}", inputStreamSegment.getFilePath());
            startTime = System.currentTimeMillis();
            // This lock is need as garbage segment can be concurrently written while compacting.
            // But the lock should not take long time as garbage segment is small (several MB).
            try (AutoCloseableLock ignored = GarbageLogSegment.getSegmentLock().acquireWriteLock(ordinal)) {
                compactGarbageSegment(inputGarbageSegment, outputGarbageSegment,
                        compactionFeedback.garbageEntriesToPrune);
                segmentManager.remapCompactedSegment(inputGarbageSegment, outputGarbageSegment);
            }

            // Close input and output garbage segment channels.
            closeSegments(inputGarbageSegment, outputGarbageSegment);
            span = System.currentTimeMillis() - startTime;
            log.debug("Compacted garbage log segment: {}, took {} ms", inputGarbageSegment.getFilePath(), span);

        } catch (Throwable t) {
            log.error("compactSegment: encountered an exception, " +
                    "compaction on segment: {} might be uncompleted.", ordinal, t);
            closeSegments(inputStreamSegment, outputStreamSegment, inputGarbageSegment, outputGarbageSegment);
            throw t;
        }
    }

    private CompactionFeedback compactStreamSegment(StreamLogSegment inputStreamSegment,
                                                    StreamLogSegment outputStreamSegment,
                                                    GarbageLogSegment inputGarbageSegment) {
        int flushBatchCount = 0;
        List<LogData> batch = new ArrayList<>();
        Map<Long, SMRGarbageEntry> garbageInfoMap = inputGarbageSegment.getGarbageEntryMap();
        CompactionFeedback compactionFeedback = new CompactionFeedback();

        // Scan the input StreamLogSegment and compact entries.
        for (IndexedLogEntry indexedEntry : inputStreamSegment) {
            LogData logData = getLogData(indexedEntry.logEntry);
            long address = logData.getGlobalAddress();
            LogData newLogData = compactStreamLogEntry(logData, address,
                    garbageInfoMap.get(address), compactionFeedback);
            // If the entire LogData is compacted, do not append to new segment.
            if (newLogData != null) {
                batch.add(newLogData);
            }

            // Write batch to the output segment.
            flushBatchCount = writeBatchToCompactionOutput(batch,
                    STREAM_COMPACTION_BATCH_SIZE, flushBatchCount, outputStreamSegment);
        }
        writeBatchToCompactionOutput(batch, outputStreamSegment);

        return compactionFeedback;
    }

    @Nullable
    private LogData compactStreamLogEntry(LogData logData, long address,
                                          SMRGarbageEntry garbageEntry,
                                          CompactionFeedback compactionFeedback) {
        // If it's a hole, return null, which will not be written to new output file.
        if (logData.getType() == DataType.HOLE) {
            return null;
        }

        // Ignore entries that are not DATA type.
        if (logData.getType() != DataType.DATA) {
            return logData;
        }

        // If no garbage info on this address return the same LogData.
        if (garbageEntry == null) {
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
            List<byte[]> smrRecords = smrLogEntry.getSerializedSMRUpdates(streamId);
            // No garbage information on this stream.
            if (garbageEntry.getGarbageRecords(streamId) == null) {
                compactedEntry.addSerialized(streamId, smrRecords);
                continue;
            }

            // Check each SMR update in this stream.
            List<byte[]> compactedRecords = new ArrayList<>();
            for (int i = 0; i < smrRecords.size(); i++) {
                byte[] smrRecord = smrRecords.get(i);
                // If the record is already compacted, do not compact again.
                if (SMRRecord.isCompacted(smrRecord)) {
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
                compactedRecords.add(SMRRecord.COMPACTED_RECORD_SERIALIZED);
                // Record this garbage record to later prune the GarbageLogSegment.
                compactionFeedback.addGarbageRecordToPrune(address, streamId, i);
                // Calculate the largest marker address in this segment.
                // After this segment is compacted, use this to update global compaction mark.
                compactionFeedback.updateSegmentCompactionMark(garbageRecord.getMarkerAddress());
            }

            // If all records of this stream are compacted, do not add to the entry,
            // but needs to update entry's metadata.
            if (compactedRecords.stream().allMatch(SMRRecord::isCompacted)) {
                logData.getBackpointerMap().remove(streamId);
                // Record address and stream id to later prune stream address space and garbage entries.
                compactionFeedback.addAddressToPrune(streamId, address);
                compactionFeedback.addGarbageRecordToPrune(address, streamId);
            } else {
                compactedEntry.addSerialized(streamId, compactedRecords);
            }
        }

        // If no stream updates in this entry after compaction, return null.
        if (compactedEntry.getStreams().isEmpty()) {
            compactionFeedback.addGarbageRecordToPrune(address);
            return null;
        }

        // Reset the logData payload with the compacted entry.
        logData.resetPayload(compactedEntry);
        return logData;
    }

    private void compactGarbageSegment(GarbageLogSegment inputGarbageSegment,
                                       GarbageLogSegment outputGarbageSegment,
                                       Map<Long, Map<UUID, List<Integer>>> entriesToPrune) {
        int flushBatchCount = 0;
        List<LogData> batch = new ArrayList<>();

        for (Map.Entry<Long, SMRGarbageEntry> mapEntry :
                inputGarbageSegment.getGarbageEntryMap().entrySet()) {
            long address = mapEntry.getKey();
            SMRGarbageEntry entry = mapEntry.getValue();
            Map<UUID, List<Integer>> entryToPrune = entriesToPrune.get(address);

            // If this entry was already used to compact stream entry, prune it.
            entry.prune(entryToPrune);
            // If the entire stream entry was compacted, discard the entire garbage entry.
            if (entry.getGarbageRecordCount() == 0) {
                continue;
            }

            // Reset the logData payload with the prune garbage entry.
            LogData newLogData = new LogData(DataType.GARBAGE, entry);
            batch.add(newLogData);

            // Write batch to the output segment.
            flushBatchCount = writeBatchToCompactionOutput(batch,
                    GARBAGE_COMPACTION_BATCH_SIZE, flushBatchCount, outputGarbageSegment);
        }

        writeBatchToCompactionOutput(batch, outputGarbageSegment);
    }

    private int writeBatchToCompactionOutput(List<LogData> batch,
                                             int batchSize, int flushBatchCount,
                                             AbstractLogSegment compactionOutput) {
        if (batch.size() >= batchSize) {
            compactionOutput.appendCompacted(batch);
            flushBatchCount++;
            batch.clear();

            // Flush the compaction output file after a batch to avoid IO burst.
            if (flushBatchCount >= COMPACTION_FLUSH_BATCH_SIZE) {
                compactionOutput.sync();
                flushBatchCount = 0;
            }
        }

        return flushBatchCount;
    }

    private void writeBatchToCompactionOutput(List<LogData> batch,
                                              AbstractLogSegment compactionOutput) {
        compactionOutput.appendCompacted(batch);
        compactionOutput.sync();
    }

    private void closeSegments(AbstractLogSegment... segments) {
        for (AbstractLogSegment segment : segments) {
            if (segment != null) {
                segment.close(true);
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

        // Used to prune garbage pointers.
        private Map<Long, Map<UUID, List<Integer>>> garbageEntriesToPrune = new HashMap<>();
        // Used to prune stream address bitmap.
        private Map<UUID, List<Long>> addressesToPrune = new HashMap<>();
        // Used to update global compaction mark.
        private long segmentCompactionMark = Address.NON_ADDRESS;

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

        private void updateSegmentCompactionMark(long address) {
            segmentCompactionMark = Math.max(address, segmentCompactionMark);
        }
    }
}
