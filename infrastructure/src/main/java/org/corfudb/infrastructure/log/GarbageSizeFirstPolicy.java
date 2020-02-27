package org.corfudb.infrastructure.log;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ResourceQuota;

import java.nio.file.FileStore;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A compaction policy that first considers the garbage size,
 * making no guarantee on the length of snapshot history.
 * <p>
 * Created by WenbinZhu on 5/22/19.
 */
@Slf4j
public class GarbageSizeFirstPolicy extends AbstractCompactionPolicy {

    GarbageSizeFirstPolicy(StreamLogParams params,
                           ResourceQuota logSizeQuota,
                           FileStore fileStore) {
        super(params, logSizeQuota, fileStore);
    }

    /**
     * Returns a list of grouped segments selected for compaction. The input
     * segment group list is first sorted the by the aggregated garbage size
     * and the groups whose garbage size or ratio is below the predefined
     * threshold are filtered out.
     *
     * @param compactibleSegments unprotected segments that can be compacted
     * @return a list of grouped segments IDs that are selected for compaction
     */
    @Override
    public List<List<SegmentId>> getSegmentsToCompact(List<CompactionStats> compactibleSegments) {
        // Group all the segments for potential segment merges.
        List<GroupCompactionStats> segmentGroups = formSegmentGroups(compactibleSegments);

        // Force compaction to override the policy if out of disk quota.
        if (requireForceCompaction(params, fileStore, logSizeQuota, segmentGroups)) {
            log.info("Force compaction needed, ignoring garbage threshold check.");
            return getSegmentsToForceCompact(segmentGroups);
        }

        return segmentGroups
                .stream()
                .sorted(Comparator.comparing(GroupCompactionStats::getTotalGarbageSizeMB).reversed())
                .filter(group -> group.getTotalGarbageSizeMB() > params.segmentGarbageSizeThresholdMB
                        || group.getGarbageRatio() > params.segmentGarbageRatioThreshold)
                .map(GroupCompactionStats::getSegmentIds)
                .collect(Collectors.toList());
    }
}
