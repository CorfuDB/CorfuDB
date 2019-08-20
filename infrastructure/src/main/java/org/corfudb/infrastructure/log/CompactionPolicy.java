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
     * Returns a list of segments selected for compaction based on
     * the compaction policy implementation.
     *
     * @param compactibleSegments metadata of unprotected segments that
     *                            can be selected for compaction
     * @return a list of ordinals whose associated segments are selected for compaction.
     */
    List<Long> getSegmentsToCompact(List<CompactionMetadata> compactibleSegments);
}
