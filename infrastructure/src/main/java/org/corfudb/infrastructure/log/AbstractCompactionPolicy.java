package org.corfudb.infrastructure.log;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ResourceQuota;

import java.io.IOException;
import java.nio.file.FileStore;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.log.StreamLogParams.RECORDS_PER_SEGMENT;

/**
 * Abstract compaction policy that provides
 * some common methods.
 * <p>
 * Created by WenbinZhu on 8/11/19.
 */
@Slf4j
@RequiredArgsConstructor
public abstract class AbstractCompactionPolicy implements CompactionPolicy {

    protected final StreamLogParams params;
    protected final ResourceQuota logSizeQuota;
    protected final FileStore fileStore;

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract List<List<SegmentId>> getSegmentsToCompact(List<CompactionStats> compactibleSegments);

    /**
     * Returns a list of grouped segment IDs for compaction regardless of the policy.
     * <p>
     * The input list is first filtered so that only groups that needs merge (group
     * size more than one) or groups that contains garbage are going to be selected.
     * After that groups are sorted by their garbage size so that groups with more
     * garbage size will be compacted first to reclaim space more efficiently.
     */
    List<List<SegmentId>> getSegmentsToForceCompact(List<GroupCompactionStats> segmentGroups) {
        return segmentGroups
                .stream()
                .filter(group -> group.size() > 1 || group.getTotalGarbageSizeMB() > 0.0)
                .sorted(Comparator.comparingDouble(GroupCompactionStats::getTotalGarbageSizeMB).reversed())
                .map(GroupCompactionStats::getSegmentIds)
                .collect(Collectors.toList());
    }

    /**
     * Group the input compactible segments based on the criteria that the total number
     * of entries in the group does not exceed the capacity of a single segment.
     */
    List<GroupCompactionStats> formSegmentGroups(List<CompactionStats> compactibleSegments) {
        List<GroupCompactionStats> segmentGroups = new ArrayList<>();
        int next = 0;
        while (next < compactibleSegments.size()) {
            int groupEntryCount = 0;
            GroupCompactionStats group = new GroupCompactionStats();
            while (next < compactibleSegments.size() && (groupEntryCount +=
                    compactibleSegments.get(next).getStreamEntryCount()) <= RECORDS_PER_SEGMENT) {
                group.add(compactibleSegments.get(next++));
            }
            // This should not happen, as number of entries in the segment cannot
            // exceed RECORDS_PER_SEGMENT, this is just a safety guard.
            if (group.size() == 0) {
                log.error("Error: segment entry count exceeds RECORDS_PER_SEGMENT!");
                group.add(compactibleSegments.get(next++));
            }
            segmentGroups.add(group);
        }

        return segmentGroups;
    }

    /**
     * Check if a force compaction is needed, which ignore the current policy.
     */
    boolean requireForceCompaction(StreamLogParams logParams,
                                   FileStore fileStore,
                                   ResourceQuota logSizeQuota,
                                   List<GroupCompactionStats> segmentGroups) {
        return quotaExceeded(logParams, fileStore, logSizeQuota)
                || garbageSizeExceeds(logParams, segmentGroups);
    }

    /**
     * If disk quota exceeds log unit configuration.
     */
    private boolean quotaExceeded(StreamLogParams logParams,
                                  FileStore fileStore,
                                  ResourceQuota logSizeQuota) {
        try {
            // Since both logSizeQuota and fileStore are only capable of getting an
            // estimation of the available partition space, we check both for safety.
            double fileStoreUsedRatio = 1.0 -
                    ((double) fileStore.getUsableSpace() / fileStore.getTotalSpace());
            return !logSizeQuota.hasAvailable() ||
                    fileStoreUsedRatio > logParams.logSizeQuotaPercentage / 100.0;
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
    }

    /**
     * If total garbage size exceeds the configured threshold.
     */
    private boolean garbageSizeExceeds(StreamLogParams logParams,
                                       List<GroupCompactionStats> segmentGroups) {
        double totalGarbageSize = segmentGroups.stream()
                .map(GroupCompactionStats::getTotalGarbageSizeMB)
                .reduce(0.0, Double::sum);
        return totalGarbageSize > logParams.totalGarbageSizeThresholdMB;
    }
}
