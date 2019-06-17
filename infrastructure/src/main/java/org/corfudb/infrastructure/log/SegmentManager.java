package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ResourceQuota;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A segment manger which manages stream and garbage segment mappings.
 * // TODO: add unit tests.
 * <p>
 * Created by WenbinZhu on 5/28/19.
 */
@Slf4j
@RequiredArgsConstructor
public class SegmentManager {

    // Stream log segment file suffix.
    private static final String STREAM_SEGMENT_FILE_SUFFIX = ".log";
    // Garbage log segment file suffix.
    private static final String GARBAGE_SEGMENT_FILE_SUFFIX = ".grb";

    private final StreamLogParams logParams;

    private final Path logDir;

    private final ResourceQuota logSizeQuota;

    @Getter
    private final Map<Long, StreamLogSegment> streamLogSegments = new ConcurrentHashMap<>();

    @Getter
    private final Map<Long, GarbageLogSegment> garbageLogSegments = new ConcurrentHashMap<>();

    // TODO: replace the old SegmentMetaData when compaction finishes.
    @Getter
    private final Map<Long, SegmentMetaData> segmentMetaDataMap = new ConcurrentHashMap<>();

    public long getSegmentOrdinal(long globalAddress) {
        return globalAddress / logParams.recordsPerSegment;
    }

    /**
     * Get the opened stream log segment that is responsible for storing the
     * data associated with the provided address, creating a new one if the
     * segment is not present and opened in the current cached mapping.
     *
     * @param address the address to open
     * @return the stream log segment for that address
     */
    public StreamLogSegment getStreamLogSegment(long address) {
        long ordinal = getSegmentOrdinal(address);

        return streamLogSegments.computeIfAbsent(ordinal, file -> {
            String fileName = ordinal + STREAM_SEGMENT_FILE_SUFFIX;
            String filePath = Paths.get(logDir.toString(), fileName).toString();
            SegmentMetaData segmentMetaData = segmentMetaDataMap.computeIfAbsent(
                    ordinal, seg -> new SegmentMetaData(ordinal));
            StreamLogSegment segment = new StreamLogSegment(
                    ordinal, filePath, logParams, logSizeQuota, segmentMetaData);
            try {
                // The first time we open a file we should read to the end, to load the
                // map of entries we already have.
                // Once the segment address space is loaded, it should be ready to accept writes.
                segment.readAddressSpace();
                return segment;
            } catch (IOException e) {
                log.error("Error reading file: {}", filePath, e);
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * Get the opened garbage log segment that is responsible for storing the garbage
     * information associated with the provided address, creating a new one if the
     * segment is not present and opened in the current cached mapping.
     *
     * @param address the address to open
     * @return the garbage log segment for that address
     */
    public GarbageLogSegment getGarbageLogSegment(long address) {
        throw new NotImplementedException();
    }

    /**
     * Get a list of segments that can be selected for compaction.
     * All the stream log segments are sorted by their start address and then
     * returned, excluding the last N segments where N is the number of protected
     * segments specified in the stream log parameter.
     *
     * @return a list of segments that can be selected for compaction.
     */
    public List<SegmentMetaData> getCompactibleSegments() {
        // TODO: Any race conditions?
        // Take a snapshot of the current segmentMetaDataMap.
        Map<Long, SegmentMetaData> metaDataMapCopy = new HashMap<>(segmentMetaDataMap);

        if (metaDataMapCopy.size() <= logParams.protectedSegments) {
            return Collections.emptyList();
        }

        return metaDataMapCopy
                .values()
                .stream()
                .sorted(Comparator.comparingLong(SegmentMetaData::getOrdinal))
                .limit(metaDataMapCopy.size() - logParams.protectedSegments)
                .collect(Collectors.toList());
    }

    public void close(long segmentOrdinal) {
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
     * Only for prefix trim, will be removed.
     */
    public void cleanAndClose(long endSegment) {
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
                log.warn("trimPrefix: ignoring file {}", file.getName());
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
        segmentMetaDataMap.clear();
    }

    // TODO: Add merge methods if merge is needed.
}
