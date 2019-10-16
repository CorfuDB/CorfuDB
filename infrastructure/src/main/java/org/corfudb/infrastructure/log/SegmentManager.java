package org.corfudb.infrastructure.log;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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

    private final StreamLogParams logParams;

    private final Path logDir;

    private final ResourceQuota logSizeQuota;

    private final StreamLogDataStore dataStore;

    private final LoadingCache<Long, StreamLogSegment> streamSegmentCache;

    private final LoadingCache<Long, GarbageLogSegment> garbageSegmentCache;

    // A CompactionMetadata is not removed for the whole life time of log unit.
    private final Map<Long, CompactionMetadata> segmentCompactionMetadata = new ConcurrentHashMap<>();

    SegmentManager(StreamLogParams logParams, Path logDir,
                   ResourceQuota logSizeQuota, StreamLogDataStore dataStore) {
        this.logParams = logParams;
        this.logDir = logDir;
        this.logSizeQuota = logSizeQuota;
        this.dataStore = dataStore;
        this.streamSegmentCache = Caffeine.newBuilder()
                .maximumSize(logParams.maxOpenStreamSegments)
                .removalListener(getRemovalListener())
                .build(ordinal -> getLogSegment(ordinal, StreamLogSegment.class));
        this.garbageSegmentCache = Caffeine.newBuilder()
                .maximumSize(logParams.maxOpenGarbageSegments)
                .removalListener(getRemovalListener())
                .build(ordinal -> getLogSegment(ordinal, GarbageLogSegment.class));
    }

    private <T extends AbstractLogSegment> RemovalListener<Long, T> getRemovalListener() {
        return (key, value, cause) -> {
            if (value == null) {
                return;
            }
            if (cause == RemovalCause.SIZE) {
                value.close(false);
            } else if (cause == RemovalCause.EXPLICIT) {
                value.close(true);
            } else {
                log.error("RemovalListener: unexpected removal cause: {}", cause);
            }
        };
    }

    private <T extends AbstractLogSegment> String getSegmentFilePath(
            long ordinal, Class<T> segmentType, boolean isCompactionOutput) {
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

        return Paths.get(logDir.toString(), ordinal + fileSuffix).toString();
    }

    /**
     * Get the ordinal of the segment that the global address belongs to.
     *
     * @param globalAddress the global address
     * @return ordinal of the segment
     */
    long getSegmentOrdinal(long globalAddress) {
        return globalAddress / StreamLogParams.RECORDS_PER_SEGMENT;
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
        return getStreamLogSegmentByOrdinal(ordinal);
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
        StreamLogSegment segment = streamSegmentCache.get(ordinal);
        Objects.requireNonNull(segment).retain();
        return segment;
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
        return getGarbageLogSegmentByOrdinal(ordinal);
    }

    /**
     * Get the opened garbage log segment that is responsible for storing the
     * data associated with the provided segment ordinal, creating a new one
     * if the segment is not present and opened in the current cached mapping.
     *
     * @param ordinal the ordinal of the segment
     * @return the garbage log segment for that ordinal
     */
    GarbageLogSegment getGarbageLogSegmentByOrdinal(long ordinal) {
        GarbageLogSegment segment = garbageSegmentCache.get(ordinal);
        Objects.requireNonNull(segment).retain();
        return segment;
    }

    @SuppressWarnings("unchecked")
    private <T extends AbstractLogSegment> T getLogSegment(long ordinal, Class<T> segmentType) {
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

        GarbageLogSegment curGarbageSegment = getGarbageLogSegmentByOrdinal(ordinal);
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
            // Close the segment to fail on-going operations with ClosedSegmentException.
            if (segment != null) {
                // Force close the segment without checking reference count, let
                // any on-going channel operation fail with ClosedChannelException.
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

            // Update the compactionMetadata map with the new CompactionMetadata.
            segmentCompactionMetadata.put(ordinal, newSegment.getCompactionMetaData());

            // Remove the segment mapping.
            return null;
        });
    }

    /**
     * Close and remove the segment from the cache.
     *
     * @param ordinal ordinal of the segment to close
     */
    void close(long ordinal) {
        streamSegmentCache.invalidate(ordinal);
        garbageSegmentCache.invalidate(ordinal);
    }

    /**
     * Close and remove all the segments from the cache.
     */
    public void close() {
        streamSegmentCache.invalidateAll();
        garbageSegmentCache.invalidateAll();
        segmentCompactionMetadata.clear();
    }

    /**
     * Close and remove all the segments from the cache,
     * delete the underlying files.
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
