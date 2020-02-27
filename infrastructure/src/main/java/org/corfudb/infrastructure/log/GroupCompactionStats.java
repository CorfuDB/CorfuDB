package org.corfudb.infrastructure.log;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

/**
 * Compaction Stats for a segment group.
 *
 * Created by WenbinZhu on 2/24/20.
 */
public class GroupCompactionStats {

    // IDs of the segmentIds in the group.
    @Getter
    private List<SegmentId> segmentIds = new ArrayList<>();

    // Aggregated total payload size in bytes.
    private long totalPayloadSize;

    // Aggregated total garbage payload size in bytes.
    private long totalGarbageSize;

    // Aggregated size of the garbage in bytes whose marker
    // addresses are bounded by nextCompactionUpperBound.
    private long boundedGarbageSize;

    void add(CompactionStats compactionStats) {
        segmentIds.add(compactionStats.getSegmentId());
        totalGarbageSize += compactionStats.getTotalGarbageSize();
        totalGarbageSize += compactionStats.getTotalGarbageSize();
        boundedGarbageSize += compactionStats.getBoundedGarbageSize();
    }

    /**
     * Get the number of segmentIds included in this group.
     *
     * @return number of segmentIds in this group
     */
    int size() {
        return segmentIds.size();
    }

    /**
     * Get the total size of the garbage in this group in MB.
     *
     * @return total size of the garbage payloads in MB
     */
    double getTotalGarbageSizeMB() {
        return (double) totalGarbageSize / 1024 / 1024;
    }

    /**
     * Get the size of the bounded garbage in this group in MB.
     *
     * @return total size of the garbage payloads in MB
     */
    double getBoundedGarbageSizeMB() {
        return (double) boundedGarbageSize / 1024 / 1024;
    }

    /**
     * Get the garbage ratio of this group, which is defined by
     * total size of garbage payload  / total payload size.
     *
     * @return garbage ratio of this group
     */
    double getGarbageRatio() {
        return (double) totalGarbageSize / totalPayloadSize;
    }

    /**
     * Get the bounded garbage ratio of this group, which is defined
     * by size of bounded garbage payload  / total payload size.
     *
     * @return bounded garbage ratio of this group
     */
    double getBoundedGarbageRatio() {
        return (double) boundedGarbageSize / totalPayloadSize;
    }
}
