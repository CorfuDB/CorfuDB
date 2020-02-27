package org.corfudb.infrastructure.log;

import java.util.List;

/**
 * The interface of stream log compaction policy, which decides
 * which log segments will be selected for compaction.
 * <p>
 * Created by WenbinZhu on 5/22/19.
 */
public interface CompactionPolicy {

    enum CompactionPolicyType {
        GARBAGE_SIZE_FIRST,
        SNAPSHOT_LENGTH_FIRST,
    }

    /**
     * Returns a list of grouped segments selected for compaction. Different
     * compaction policy may return different groups. Grouping is based on
     * the criteria that the total number of entries in the group does not
     * exceed the capacity of a single segment.
     *
     * @param compactibleSegments a list of segment compaction stats ordered by
     *                            segment ID that are allowed to be selected
     *                            for compaction
     * @return a list of grouped segments selected for compaction
     */
    List<List<SegmentId>> getSegmentsToCompact(List<CompactionStats> compactibleSegments);
}
