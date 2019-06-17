package org.corfudb.infrastructure.log;

import lombok.AllArgsConstructor;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by WenbinZhu on 5/22/19.
 */
@AllArgsConstructor
public class GarbageSizeCompactionPolicy implements CompactionPolicy {

    private final StreamLogParams params;

    /**
     * Simply sort the segments by their garbage size, and return the segments
     * with the most amount garbage size, if they reach the predefined threshold.
     * The number of segment returned will not exceed a predefined limit.
     *
     * @param compactibleSegments unprotected segments that can be selected for compaction
     * @return a list of segments selected for compaction
     */
    @Override
    public List<SegmentMetaData> getSegmentsToCompact(List<SegmentMetaData> compactibleSegments) {
        // TODO: Add total garbage size threshold check.
        return compactibleSegments
                .stream()
                .sorted(Comparator.comparingLong(SegmentMetaData::getGarbagePayloadSize).reversed())
                .filter(metaData -> metaData.getGarbagePayloadSizeMB() >= params.segmentGarbageSizeThresholdMB
                        || metaData.getGarbageRatio() >= params.segmentGarbageRatioThreshold)
                .limit(params.maxSegmentsForCompaction)
                .collect(Collectors.toList());
    }
}
