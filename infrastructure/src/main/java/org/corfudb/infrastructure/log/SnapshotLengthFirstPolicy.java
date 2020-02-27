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
     * Returns a list of grouped segments selected for compaction. This policy
     * not only considers garbage distribution, but also guarantees a fixed
     * length of snapshot history. The current compaction cycle makes sure that
     * the entries being compacted could not move the compaction mark beyond
     * a boundary with is set to the global tail captured in last cycle.
     *
     * @param compactibleSegments unprotected segments that can be compacted
     * @return a list of grouped segments IDs that are selected for compaction
     */
    @Override
    public List<List<SegmentId>> getSegmentsToCompact(List<CompactionStats> compactibleSegments) {
        // Group all the segments for potential segment merges.
        List<GroupCompactionStats> segmentGroups = formSegmentGroups(compactibleSegments);

        // Force compaction to override the garbage ratio check if out of disk quota.
        if (requireForceCompaction(params, fileStore, logSizeQuota, segmentGroups)) {
            long nextCompactionUpperBound = newCompactionUpperBound();
            // Check if log unit is initialized and has data.
            if (Address.isAddress(nextCompactionUpperBound)) {
                // Set currCompactionUpperBound to current global tail to increase the
                // number of entries that could be compacted in this cycle.
                CompactionStats.setCurrCompactionUpperBound(nextCompactionUpperBound);
                CompactionStats.setNextCompactionUpperBound(nextCompactionUpperBound);
                log.info("Force compaction needed, ignoring garbage threshold check, " +
                                "set currCompactionUpperBound={}, nextCompactionUpperBound={}",
                        CompactionStats.getCurrCompactionUpperBound(),
                        CompactionStats.getNextCompactionUpperBound());
                return getSegmentsToForceCompact(segmentGroups);
            }
            return Collections.emptyList();
        }

        // Skip compaction for the first cycle after startup.
        if (CompactionStats.getNextCompactionUpperBound() == Address.MAX) {
            long nextCompactionUpperBound = newCompactionUpperBound();
            // Check if log unit is initialized and has data.
            if (Address.isAddress(nextCompactionUpperBound)) {
                CompactionStats.setNextCompactionUpperBound(nextCompactionUpperBound);
            }
            return Collections.emptyList();
        }

        List<List<SegmentId>> segmentsToCompact =
                segmentGroups
                .stream()
                .sorted(Comparator.comparing(GroupCompactionStats::getBoundedGarbageSizeMB).reversed())
                .filter(group -> group.getBoundedGarbageSizeMB() > params.segmentGarbageSizeThresholdMB
                        || group.getBoundedGarbageRatio() > params.segmentGarbageRatioThreshold)
                .limit(params.maxSegmentsForCompaction)
                .map(GroupCompactionStats::getSegmentIds)
                .collect(Collectors.toList());

        // Set currCompactionUpperBound to nextCompactionUpperBound, which is set in the previous
        // cycle, so that when compaction starts for this cycle, entries that would move its stream's
        // compaction marks after currCompactionUpperBound will not be compacted, guaranteeing that
        // compaction mark does not move for X amount of time, where X is the compaction cycle interval.
        CompactionStats.setCurrCompactionUpperBound(CompactionStats.getNextCompactionUpperBound());
        CompactionStats.setNextCompactionUpperBound(newCompactionUpperBound());
        log.info("getSegmentsToCompact: set currCompactionUpperBound={}, nextCompactionUpperBound={}",
                CompactionStats.getCurrCompactionUpperBound(), CompactionStats.getNextCompactionUpperBound());

        return segmentsToCompact;
    }

    /**
     * Return the next compaction upper bound which would be used in the
     * next compaction cycle to filter out the entries which would move
     * the compaction mark after this bound.
     */
    private long newCompactionUpperBound() {
        return logMetadata.getGlobalTail();
    }
}
