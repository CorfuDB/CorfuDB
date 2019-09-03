package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A segment manger which manages stream and garbage segment mappings.
 * <p>
 * Created by WenbinZhu on 5/28/19.
 */
@Slf4j
@RequiredArgsConstructor
public class SegmentManager {

    // TODO: add max open segments restrict, maybe with an auxiliary ordered list?
    // Maximum number of open segments kept in memory.
    private static final int MAX_OPEN_SEGMENTS = 20;

    // Suffix of the stream log segment files.
    private static final String STREAM_SEGMENT_FILE_SUFFIX = ".log";
    // Suffix of the garbage log segment files.
    private static final String GARBAGE_SEGMENT_FILE_SUFFIX = ".grb";
    // Suffix of the segment files for compaction output.
    private static final String SEGMENT_COMPACTING_FILE_SUFFIX = ".cpt";

    private final StreamLogParams logParams;

    private final Path logDir;

    private final ResourceQuota logSizeQuota;

    private final StreamLogDataStore dataStore;

    @Getter
    private final Map<Long, StreamLogSegment> streamLogSegments = new ConcurrentHashMap<>();

    @Getter
    private final Map<Long, GarbageLogSegment> garbageLogSegments = new ConcurrentHashMap<>();

    // TODO: replace the old CompactionMetadata when compaction finishes.
    // A CompactionMetadata is not removed for the whole life time of log unit.
    @Getter
    private final Map<Long, CompactionMetadata> segmentCompactionMetadata = new ConcurrentHashMap<>();

    long getSegmentOrdinal(long globalAddress) {
        return globalAddress / logParams.RECORDS_PER_SEGMENT;
    }

    private <T extends AbstractLogSegment> String getSegmentFilePath(
            long ordinal, Class<T> segmentType, boolean forCompaction) {
        String fileSuffix;

        if (segmentType.equals(StreamLogSegment.class)) {
            fileSuffix = STREAM_SEGMENT_FILE_SUFFIX;
        } else if (segmentType.equals(GarbageLogSegment.class)) {
            fileSuffix = GARBAGE_SEGMENT_FILE_SUFFIX;
        } else {
            throw new IllegalArgumentException("getSegmentFilePath: " +
                    "Unknown segment type: " + segmentType);
        }

        if (forCompaction) {
            fileSuffix += SEGMENT_COMPACTING_FILE_SUFFIX;
        }

        return Paths.get(logDir.toString(), ordinal + fileSuffix).toString();
    }

    /**
     * Get the opened stream log segment that is responsible for storing the
     * data associated with the provided address, creating a new one if the
     * segment is not present and opened in the current cached mapping.
     *
     * @param address the address to open
     * @return the stream log segment for that address
     */
    StreamLogSegment getStreamLogSegment(long address) {
        long ordinal = getSegmentOrdinal(address);
        return getLogSegment(ordinal, StreamLogSegment.class, streamLogSegments);
    }

    /**
     * Get the opened stream log segment that is responsible for storing the
     * data associated with the provided segment ordinal, creating a new one
     * if the segment is not present and opened in the current cached mapping.
     *
     * @param ordinal the ordinal of the segment
     * @return the stream log segment for that ordinal
     */
    StreamLogSegment getStreamLogSegmentByOrdinal(long ordinal) {
        return getLogSegment(ordinal, StreamLogSegment.class, streamLogSegments);
    }

    /**
     * Get the opened garbage log segment that is responsible for storing the garbage
     * information associated with the provided address, creating a new one if the
     * segment is not present and opened in the current cached mapping.
     *
     * @param address the address to open
     * @return the garbage log segment for that address
     */
    GarbageLogSegment getGarbageLogSegment(long address) {
        long ordinal = getSegmentOrdinal(address);
        return getLogSegment(ordinal, GarbageLogSegment.class, garbageLogSegments);
    }

    @SuppressWarnings("unchecked")
    private <T extends AbstractLogSegment> T getLogSegment(long ordinal,
                                                           Class<T> segmentType,
                                                           Map<Long, T> segmentMap) {
        return segmentMap.computeIfAbsent(ordinal, ord -> {
            String filePath = getSegmentFilePath(ordinal, segmentType, false);
            CompactionMetadata metaData = getCompactionMetaData(ordinal);

            AbstractLogSegment segment;
            if (segmentType.equals(StreamLogSegment.class)) {
                segment = new StreamLogSegment(ordinal, logParams, filePath, logSizeQuota, metaData, dataStore);
            } else if (segmentType.equals(GarbageLogSegment.class)) {
                segment = new GarbageLogSegment(ordinal, logParams, filePath, logSizeQuota, metaData);
            } else {
                throw new IllegalArgumentException("getLogSegment: Unknown segment type: " + segmentType);
            }

            // The first time we open a file we should read to the end, to load the map
            // of entries we already have. Once the segment address space is loaded, it
            // should be ready to accept writes.
            segment.loadAddressSpace();
            return (T) segment;
        });
    }

    private CompactionMetadata getCompactionMetaData(long ordinal) {
        return segmentCompactionMetadata.computeIfAbsent(ordinal, seg ->
                new CompactionMetadata(ordinal));
    }

    /**
     * Get a new compaction metadata for the segment.
     *
     * @param ordinal the ordinal of the segment the metadata is associated with
     * @return the compaction metadata for the segment
     */
    CompactionMetadata newCompactionMetadata(long ordinal) {
        return new CompactionMetadata(ordinal);
    }

    /**
     * TODO: add comments
     */
    StreamLogSegment newCompactionInputStreamSegment(long ordinal) {
        String filePath = getSegmentFilePath(ordinal, StreamLogSegment.class, false);

        if (!Files.exists(Paths.get(filePath))) {
            String msg = String.format("Compaction input stream segment file: " +
                    "%s not exist.", filePath);
            log.error(msg);
            throw new IllegalStateException(msg);
        }

        return new StreamLogSegment(ordinal, logParams, filePath, logSizeQuota, null, dataStore);
    }

    /**
     * TODO: add comments
     */
    StreamLogSegment newCompactionOutputStreamSegment(long ordinal,
                                                      CompactionMetadata metaData) {
        String filePath = getSegmentFilePath(ordinal, StreamLogSegment.class, true);
        deleteExistingCompactionOutputFile(filePath);

        // Open a new segment and write header.
        StreamLogSegment streamLogSegment = new StreamLogSegment(
                ordinal, logParams, filePath, logSizeQuota, metaData, dataStore);
        streamLogSegment.loadAddressSpace();

        return streamLogSegment;
    }

    /**
     * TODO: add comments.
     */
    GarbageLogSegment newCompactionInputGarbageSegment(long ordinal) {
        String filePath = getSegmentFilePath(ordinal, GarbageLogSegment.class, false);

        if (!Files.exists(Paths.get(filePath))) {
            String msg = String.format("Compaction input garbage " +
                    "segment file: %s not exist.", filePath);
            log.error(msg);
            throw new IllegalStateException(msg);
        }

        GarbageLogSegment curGarbageSegment =
                getLogSegment(ordinal, GarbageLogSegment.class, garbageLogSegments);
        GarbageLogSegment newGarbageSegment =
                new GarbageLogSegment(ordinal, logParams, filePath, logSizeQuota, null);
        newGarbageSegment.setGarbageEntryMap(curGarbageSegment.getGarbageEntryMap());

        return newGarbageSegment;
    }

    GarbageLogSegment newCompactionOutputGarbageSegment(long ordinal,
                                                        CompactionMetadata metaData) {
        String filePath = getSegmentFilePath(ordinal, GarbageLogSegment.class, true);
        deleteExistingCompactionOutputFile(filePath);

        // Open a new segment and write header.
        GarbageLogSegment garbageLogSegment = new GarbageLogSegment(
                ordinal, logParams, filePath, logSizeQuota, metaData);
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
    List<CompactionMetadata> getCompactibleSegments() {
        // Take a snapshot of the current segmentCompactionMetadata.
        Map<Long, CompactionMetadata> metaDataMapCopy = new HashMap<>(segmentCompactionMetadata);

        if (metaDataMapCopy.size() <= logParams.protectedSegments) {
            return Collections.emptyList();
        }

        return metaDataMapCopy
                .values()
                .stream()
                .sorted(Comparator.comparingLong(CompactionMetadata::getOrdinal))
                .limit(metaDataMapCopy.size() - logParams.protectedSegments)
                .collect(Collectors.toList());
    }

    /**
     * Close the old segment and rename the new segment file to the old segment file.
     * After this call, subsequent or pending calls to acquire a segment would return the new segment.
     */
    void remapCompactedSegment(AbstractLogSegment oldSegment, AbstractLogSegment newSegment) {
        long ordinal = newSegment.getOrdinal();
        Map<Long, ? extends AbstractLogSegment> segmentMap;

        if (newSegment instanceof StreamLogSegment) {
            segmentMap = streamLogSegments;
        } else if (newSegment instanceof GarbageLogSegment) {
            segmentMap = garbageLogSegments;
        } else {
            throw new IllegalArgumentException("remapCompactedSegment: Unknown segment class: "
                    + newSegment.getClass());
        }

        segmentMap.compute(ordinal, (ord, segment) -> {
            // Close the segment to fail on-going operations with ClosedSegmentException.
            if (segment != null) {
                segment.close();
            }

            Path oldPath = Paths.get(oldSegment.getFilePath());
            Path newPath = Paths.get(newSegment.getFilePath());
            try {
                // Rename and replace the old file with the new file.
                Files.move(newPath, oldPath, StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                log.error("remapCompactedSegment: exception when renaming file " +
                        "{} to {}", newPath, oldPath, e);
                throw new RuntimeException(e);
            }

            // Update the compactionMetadata map with the new CompactionMetadata.
            segmentCompactionMetadata.put(ordinal, newSegment.getCompactionMetaData());

            // Remove the segment mapping.
            return null;
        });
    }

    void close(long segmentOrdinal) {
        close(streamLogSegments, segmentOrdinal);
        close(garbageLogSegments, segmentOrdinal);
    }

    private void close(Map<Long, ? extends AbstractLogSegment> segmentMap, long segmentOrdinal) {
        segmentMap.computeIfPresent(segmentOrdinal, (ordinal, segment) -> {
            segment.close();
            return null;
        });
    }

    /**
     * Only to support prefix trim, will be removed later.
     */
    void cleanAndClose(long endSegment) {
        cleanAndClose(streamLogSegments, endSegment);
        cleanAndClose(garbageLogSegments, endSegment);

        deleteFiles(endSegment);
    }

    private void cleanAndClose(Map<Long, ? extends AbstractLogSegment> segmentMap, long endSegment) {
        List<AbstractLogSegment> segments = segmentMap.values()
                .stream()
                .filter(segment -> segment.getOrdinal() <= endSegment)
                .collect(Collectors.toList());

        segments.forEach(segment -> {
            segment.close();
            segmentMap.remove(segment.getOrdinal());
        });
    }

    private void deleteFiles(long endSegment) {
        int numFiles = 0;
        long freedBytes = 0;
        File dir = logDir.toFile();
        File[] files = dir.listFiles(file -> {
            try {
                String segmentNum = file.getName().split("\\.")[0];
                return Long.parseLong(segmentNum) <= endSegment;
            } catch (Exception e) {
                log.warn("deleteFiles: ignoring file {}", file.getName());
                return false;
            }
        });

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

    /**
     * Close all managed log segments.
     */
    public void close() {
        streamLogSegments.values().forEach(StreamLogSegment::close);
        garbageLogSegments.values().forEach(GarbageLogSegment::close);
        streamLogSegments.clear();
        garbageLogSegments.clear();
        segmentCompactionMetadata.clear();
    }
}
