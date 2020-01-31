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
     * Simply sort the segments by their garbage size, and return the segments
     * with the most amount garbage size, if they reach the predefined threshold.
     * The number of segment returned will not exceed a user specified limit.
     *
     * @param compactibleSegments unprotected segments that can be selected for compaction
     * @return a list of ordinals whose associated segments are selected for compaction
     */
    @Override
    public List<Long> getSegmentsToCompact(List<CompactionMetadata> compactibleSegments) {
        // Force compaction to override the policy if out of disk quota.
        if (requireForceCompaction(params, fileStore, logSizeQuota, compactibleSegments)) {
            log.info("Force compaction needed, ignoring garbage threshold check.");
            return getSegmentsToForceCompact(compactibleSegments);
        }

        return compactibleSegments
                .stream()
                .sorted(Comparator.comparing(CompactionMetadata::getTotalGarbageSizeMB).reversed())
                .filter(metaData -> metaData.getTotalGarbageSizeMB() > params.segmentGarbageSizeThresholdMB
                        || metaData.getGarbageRatio() > params.segmentGarbageRatioThreshold)
                .limit(params.maxSegmentsForCompaction)
                .map(CompactionMetadata::getOrdinal)
                .collect(Collectors.toList());
    }
}
