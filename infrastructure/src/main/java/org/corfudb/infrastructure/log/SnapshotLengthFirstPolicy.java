package org.corfudb.infrastructure.log;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ResourceQuota;
import org.corfudb.runtime.view.Address;

import java.nio.file.FileStore;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A compaction policy that first tries to guarantee a fixed length of
 * snapshot history (transactions will be guaranteed to travel back to
 * a snapshot X amount of time ago). This guarantee could be violated
 * if there is too much garbage or short of space.
 * <p>
 * Created by WenbinZhu on 6/24/19.
 */
@Slf4j
public class SnapshotLengthFirstPolicy extends AbstractCompactionPolicy {

    private final LogMetadata logMetadata;

    SnapshotLengthFirstPolicy(StreamLogParams params,
                              ResourceQuota logSizeQuota,
                              FileStore fileStore,
                              LogMetadata logMetadata) {
        super(params, logSizeQuota, fileStore);
        this.logMetadata = logMetadata;
    }

    /**
     * TODO: add comments
     *
     * @param compactibleSegments metadata of unprotected segments that
     * @return a list of ordinals whose associated segments are selected for compaction.
     */
    @Override
    public List<Long> getSegmentsToCompact(List<CompactionMetadata> compactibleSegments) {
        // Force compaction to override the policy if out of disk quota.
        if (requireForceCompaction(params, fileStore, logSizeQuota)) {
            log.info("Force compaction needed, ignoring compaction policy.");
            return getSegmentsToForceCompact(compactibleSegments);
        }

        // Skip compaction for the first cycle after startup.
        if (CompactionMetadata.getNextCompactionUpperBound() == Address.MAX) {
            CompactionMetadata.setNextCompactionUpperBound(logMetadata.getGlobalTail());
            return Collections.emptyList();
        }

        List<Long> segmentsToCompact = compactibleSegments
                .stream()
                .sorted(Comparator.comparing(CompactionMetadata::getBoundedGarbageSizeMB).reversed())
                .filter(metaData -> metaData.getBoundedGarbageSizeMB() >= params.segmentGarbageSizeThresholdMB
                        || metaData.getBoundedGarbageRatio() >= params.segmentGarbageRatioThreshold)
                .limit(params.maxSegmentsForCompaction)
                .map(CompactionMetadata::getOrdinal)
                .collect(Collectors.toList());

        // Set currCompactionUpperBound to nextCompactionUpperBound, which is set in the previous
        // cycle, so that when compaction starts for this cycle, entries that would move its stream's
        // compaction marks after currCompactionUpperBound will not be trimmed, guaranteeing compaction
        // marks not moving for X amount of time, where X is the compaction cycle interval.
        CompactionMetadata.setCurrCompactionUpperBound(CompactionMetadata.getNextCompactionUpperBound());
        CompactionMetadata.setNextCompactionUpperBound(logMetadata.getGlobalTail());

        return segmentsToCompact;
    }
}
