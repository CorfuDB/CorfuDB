package org.corfudb.infrastructure.log;

import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ResourceQuota;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.log.StreamLogParams.RECORDS_PER_SEGMENT;

/**
 * A segment manger which manages stream and garbage segment mappings.
 * <p>
 * Created by WenbinZhu on 5/28/19.
 */
@Slf4j
public class SegmentManager {

    // Suffix of the stream log segment files.
    private static final String STREAM_SEGMENT_FILE_SUFFIX = ".log";
    // Suffix of the garbage log segment files.
    private static final String GARBAGE_SEGMENT_FILE_SUFFIX = ".grb";
    // Suffix of the segment files for compaction output.
    private static final String SEGMENT_COMPACTING_FILE_SUFFIX = ".cpt";

    // Log unit configuration parameters.
    private final StreamLogParams logParams;
    private final Path logDir;
    private final ResourceQuota logSizeQuota;
    private final StreamLogDataStore dataStore;

    // Segment indexes and metadata.
    private final SegmentIndex<StreamLogSegment> streamSegmentIndex = new SegmentIndex<>();
    private final SegmentIndex<GarbageLogSegment> garbageSegmentIndex = new SegmentIndex<>();
    private final Map<SegmentId, CompactionStats> compactionStatsMap = new ConcurrentHashMap<>();

    SegmentManager(StreamLogParams logParams, Path logDir,
                   ResourceQuota logSizeQuota, StreamLogDataStore dataStore) {
        this.logParams = logParams;
        this.logDir = logDir;
        this.logSizeQuota = logSizeQuota;
        this.dataStore = dataStore;
    }

    /**
     * Create an artificial unmerged segment ID by global address.
     *
     * @param globalAddress the global address
     * @return ID of an unmerged segment
     */
    private SegmentId createSegmentId(long globalAddress) {
        long start = globalAddress / RECORDS_PER_SEGMENT * RECORDS_PER_SEGMENT;
        return new SegmentId(start, start + RECORDS_PER_SEGMENT);
    }

    /**
     * Get the existing stream log segment that is responsible for storing
     * data associated with the provided address, creating a new one if the
     * corresponding segment is not present.
     *
     * @param address the address to open
     * @return the stream log segment for that address
     */
    StreamLogSegment getStreamLogSegment(long address) {
        SegmentId segmentId = createSegmentId(address);
        return getStreamLogSegmentById(segmentId);
    }

    /**
     * Get the existing stream log segment that is responsible for storing
     * data associated with the provided segment ID, creating a new one if
     * the segment is not present.
     *
     * @param segmentId ID of the segment
     * @return the stream log segment for the segment ID
     */
    StreamLogSegment getStreamLogSegmentById(SegmentId segmentId) {
        StreamLogSegment segment = streamSegmentIndex.getSegmentById(
                segmentId,
                sid -> getLogSegment(sid, StreamLogSegment.class)
        );
        segment.retain();

        return segment;
    }

    /**
     * Get the existing stream log segment that is responsible for storing
     * garbage decision associated with the provided segment ID, creating
     * a new one if the segment is not present.
     *
     * @param address the address to open
     * @return the garbage log segment for the address
     */
    GarbageLogSegment getGarbageLogSegment(long address) {
        SegmentId segmentId = createSegmentId(address);
        return getGarbageLogSegmentById(segmentId);
    }

    /**
     * Get the existing garbage log segment that is responsible for storing
     * garbage decision associated with the provided segment ID, creating a
     * new one if the segment is not present.
     *
     * @param segmentId ID of the segment
     * @return the garbage log segment for the segment ID
     */
    GarbageLogSegment getGarbageLogSegmentById(SegmentId segmentId) {
        GarbageLogSegment segment = garbageSegmentIndex.getSegmentById(
                segmentId,
                sid -> getLogSegment(sid, GarbageLogSegment.class));
        segment.retain();

        return segment;
    }

    @SuppressWarnings("unchecked")
    private <T extends AbstractLogSegment> T getLogSegment(SegmentId segmentId, Class<T> segmentType) {
        String filePath = getSegmentFilePath(segmentId, segmentType, false);
        CompactionStats metaData = getCompactionStats(segmentId);

        AbstractLogSegment segment;
        if (segmentType.equals(StreamLogSegment.class)) {
            segment = new StreamLogSegment(segmentId, logParams, filePath, logSizeQuota, metaData, dataStore);
        } else if (segmentType.equals(GarbageLogSegment.class)) {
            segment = new GarbageLogSegment(segmentId, logParams, filePath, logSizeQuota, metaData);
        } else {
            throw new IllegalArgumentException("getLogSegment: Unknown segment type: " + segmentType);
        }

        // The first time we open a file we should read to the end, to load the map
        // of entries we already have. Once the segment address space is loaded, it
        // should be ready to accept writes.
        segment.loadAddressSpace();
        return (T) segment;
    }

    /**
     * Generate the segment file path, can be a regular log segment file or a
     * compaction output file for compactor to rewrite an existing segment.
     */
    private <T extends AbstractLogSegment> String getSegmentFilePath(
            SegmentId segmentId, Class<T> segmentType, boolean isCompactionOutput) {

        String fileSuffix;
        if (segmentType.equals(StreamLogSegment.class)) {
            fileSuffix = STREAM_SEGMENT_FILE_SUFFIX;
        } else if (segmentType.equals(GarbageLogSegment.class)) {
            fileSuffix = GARBAGE_SEGMENT_FILE_SUFFIX;
        } else {
            throw new IllegalArgumentException("getSegmentFilePath: " +
                    "Unknown segment type: " + segmentType);
        }

        if (isCompactionOutput) {
            fileSuffix += SEGMENT_COMPACTING_FILE_SUFFIX;
        }

        return Paths.get(logDir.toString(), segmentId + fileSuffix).toString();
    }

    private CompactionStats getCompactionStats(SegmentId segmentId) {
        return compactionStatsMap.computeIfAbsent(
                segmentId,
                sid -> new CompactionStats(segmentId));
    }

    /**
     * Create a new compaction stats container for the segment ID.
     *
     * @param segmentId the ID of the segment the new metadata is associated with
     * @return the new compaction stats container for the segment
     */
    CompactionStats createCompactionStats(SegmentId segmentId) {
        return new CompactionStats(segmentId);
    }

    /**
     * Create a new stream log segment object representing an existing segment file
     * that is used for compaction input. The compactor uses this segment object to
     * scan the underlying segment file and perform a rewrite.
     *
     * @param segmentId the ID of the compaction input stream segment
     * @return a new stream log segment representing an existing segment file
     */
    StreamLogSegment createCompactionInputStreamSegment(SegmentId segmentId) {
        String filePath = getSegmentFilePath(segmentId, StreamLogSegment.class, false);

        if (!Files.exists(Paths.get(filePath))) {
            String msg = String.format("Compaction input stream segment file: " +
                    "%s not exist.", filePath);
            log.error(msg);
            throw new IllegalStateException(msg);
        }

        return new StreamLogSegment(segmentId, logParams, filePath, logSizeQuota, null, dataStore);
    }

    /**
     * Create a new stream log segment represent a new segment file that is used for
     * compaction output. The compactor scans existing segment files and rewrite to
     * the underlying file represented by this segment.
     *
     * @param segmentId the ID of the compaction output stream segment
     * @param cpStats   the compaction stats associated with this segment
     * @return a new stream log segment representing a new segment file
     */
    StreamLogSegment createCompactionOutputStreamSegment(SegmentId segmentId,
                                                         CompactionStats cpStats) {
        String filePath = getSegmentFilePath(segmentId, StreamLogSegment.class, true);
        deleteExistingCompactionOutputFile(filePath);

        // Open a new segment and write header.
        StreamLogSegment streamLogSegment = new StreamLogSegment(
                segmentId, logParams, filePath, logSizeQuota, cpStats, dataStore);
        streamLogSegment.loadAddressSpace();

        return streamLogSegment;
    }

    /**
     * Create a new garbage log segment object representing an existing segment file
     * that is used for compaction input. The compactor uses this segment object to
     * scan the underlying segment file and perform a rewrite.
     *
     * @param segmentId the ID of the compaction input garbage segment
     * @return a new garbage log segment representing an existing segment file
     */
    GarbageLogSegment createCompactionInputGarbageSegment(SegmentId segmentId) {
        String filePath = getSegmentFilePath(segmentId, GarbageLogSegment.class, false);

        if (!Files.exists(Paths.get(filePath))) {
            String msg = String.format("Compaction input garbage " +
                    "segment file: %s not exist.", filePath);
            log.error(msg);
            throw new IllegalStateException(msg);
        }

        GarbageLogSegment curGarbageSegment = getGarbageLogSegmentById(segmentId);
        GarbageLogSegment newGarbageSegment =
                new GarbageLogSegment(segmentId, logParams, filePath, logSizeQuota, null);
        newGarbageSegment.setGarbageEntryMap(curGarbageSegment.getGarbageEntryMap());

        return newGarbageSegment;
    }

    /**
     * Create a new garbage log segment represent a new segment file that is used for
     * compaction output. The compactor scans existing segment files and rewrite to
     * the underlying file represented by this segment.
     *
     * @param segmentId the ID of the compaction output garbage segment
     * @param cpStats   the compaction stats associated with this segment
     * @return a new garbage log segment representing a new segment file
     */
    GarbageLogSegment createCompactionOutputGarbageSegment(SegmentId segmentId,
                                                           CompactionStats cpStats) {
        String filePath = getSegmentFilePath(segmentId, GarbageLogSegment.class, true);
        deleteExistingCompactionOutputFile(filePath);

        // Open a new segment and write header.
        GarbageLogSegment garbageLogSegment = new GarbageLogSegment(
                segmentId, logParams, filePath, logSizeQuota, cpStats);
        garbageLogSegment.loadAddressSpace();

        return garbageLogSegment;
    }

    /**
     * Delete an existing compaction output file, this files was created in some
     * previous compaction cycle, but was not finished for various reasons.
     */
    private void deleteExistingCompactionOutputFile(String filePath) {
        try {
            if (Files.deleteIfExists(Paths.get(filePath))) {
                log.info("SegmentManager: Deleted existing compaction output " +
                        "segment file: {}", filePath);
            }
        } catch (IOException ioe) {
            log.error("SegmentManager: IO exception while deleting existing " +
                    "compaction output segment file: {}", filePath);
            throw new RuntimeException(ioe);
        }
    }

    /**
     * Delete all existing compaction output files, these files were created in some
     * previous compaction cycle, but were not finished for various reasons.
     */
    void deleteExistingCompactionOutputFiles() {
        File[] files = logDir.toFile().listFiles((dir, name) ->
                name.endsWith(SEGMENT_COMPACTING_FILE_SUFFIX));

        if (files == null) {
            log.debug("SegmentManager: listFiles() returns null");
            return;
        }

        for (File file : files) {
            if (file.delete()) {
                log.debug("SegmentManager: Deleted existing compaction output " +
                        "segment file: {}", file);
            }
        }
    }

    /**
     * Get a list of segments that can be selected for compaction.
     * All the stream log segments are sorted by their start address and then
     * returned, excluding the last N segments where N is the number of protected
     * segments specified in the stream log parameter.
     *
     * @return a list of segments that can be selected for compaction
     */
    List<CompactionStats> getCompactibleSegments() {
        // Do not compact any segment if state transfer is required and not finished.
        if (dataStore.getRequireStateTransfer()) {
            return Collections.emptyList();
        }

        // Take a snapshot of the current compaction stats map.
        // Since only the consolidated segments can be compacted, the number
        // of entries in these segments should not change, so using shallow
        // copies of compaction stats should be fine.
        List<CompactionStats> metaDataCopy = compactionStatsMap
                .values()
                .stream()
                .sorted(Comparator.comparing(CompactionStats::getSegmentId))
                .collect(Collectors.toList());

        // Do not compact the protected segments that have highest ordinals.
        if (metaDataCopy.size() <= logParams.protectedSegments) {
            return Collections.emptyList();
        }
        for (int i = 0; i < logParams.protectedSegments; i++) {
            metaDataCopy.remove(metaDataCopy.size() - 1);
        }

        // Compact only the consolidated segments that falls below committed tail.
        List<CompactionStats> compactibleSegments = metaDataCopy
                .stream()
                .filter(meta -> meta.getSegmentId().lessThan(dataStore.getCommittedTail()))
                .collect(Collectors.toList());
        log.info("getCompactibleSegments: compactible segments: {}", compactibleSegments
                .stream()
                .map(CompactionStats::getSegmentId).collect(Collectors.toList()));

        return compactibleSegments;
    }

    /**
     * Close the old segment and rename the new segment file to the old segment file.
     * After this call, subsequent or pending calls to acquire a segment would return the new segment.
     * NOTE: the caller needs to make sure that there are no concurrent writers by either locking
     * or ensuring the segment is fully committed and cannot be written to.
     */
    void remapCompactedSegment(AbstractLogSegment oldSegment, AbstractLogSegment newSegment) {
        long ordinal = newSegment.getOrdinal();
        LoadingCache<Long, ? extends AbstractLogSegment> segmentCache;

        if (newSegment instanceof StreamLogSegment) {
            segmentCache = streamSegmentCache;
        } else if (newSegment instanceof GarbageLogSegment) {
            segmentCache = garbageSegmentCache;
        } else {
            throw new IllegalArgumentException("remapCompactedSegment: Unknown segment class: "
                    + newSegment.getClass());
        }

        segmentCache.asMap().compute(ordinal, (ord, segment) -> {
            if (segment != null) {
                // Close segment to fail the on-going operations with
                // ClosedSegmentException and they will be retried.
                segment.close(true);
            }

            Path oldPath = Paths.get(oldSegment.getFilePath());
            Path newPath = Paths.get(newSegment.getFilePath());
            try {
                long oldFileSize = Files.size(oldPath);
                // Rename and replace the old file with the new file.
                Files.move(newPath, oldPath, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
                // Update log size quota to remove the space taken by the old file.
                logSizeQuota.release(oldFileSize);

            } catch (IOException e) {
                log.error("remapCompactedSegment: exception when renaming file " +
                        "{} to {}", newPath, oldPath, e);
                throw new RuntimeException(e);
            }

            // Update the compaction stats map with a new one.
            compactionStatsMap.put(ordinal, newSegment.getCompactionStats());

            // Remove the segment mapping.
            return null;
        });
    }

    /**
     * Close and evict the segment from the index.
     *
     * @param segmentId ID of the segment to close
     */
    void close(SegmentId segmentId) {
        streamSegmentIndex.invalidate(segmentId);
        garbageSegmentIndex.invalidate(segmentId);
    }

    /**
     * Close and evict all the segments from the cache.
     */
    public void close() {
        streamSegmentIndex.invalidateAll();
        garbageSegmentIndex.invalidateAll();
    }

    /**
     * Close and evict all the segments from the cache,
     * as well as delete all the underlying segment files.
     */
    void cleanAndClose() {
        close();
        deleteFiles();
    }

    private void deleteFiles() {
        int numFiles = 0;
        long freedBytes = 0;
        File dir = logDir.toFile();
        File[] files = dir.listFiles();

        if (files == null) {
            return;
        }

        for (File file : files) {
            long delta = file.length();

            if (!file.delete()) {
                log.error("deleteFiles: Couldn't delete file {}", file.getName());
            } else {
                freedBytes += delta;
                numFiles++;
            }
        }
        logSizeQuota.release(freedBytes);
        log.info("deleteFiles: completed, deleted {} files, freed {} bytes", numFiles, freedBytes);
    }
}
