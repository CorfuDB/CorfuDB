package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * A segment manger which manages stream and garbage segment mappings.
 *
 * Created by WenbinZhu on 5/28/19.
 */
@RequiredArgsConstructor
public class SegmentManager {

    private final StreamLogParams logParams;

    @Getter
    private final Map<Long, StreamLogSegment> streamLogSegments = new ConcurrentHashMap<>();

    @Getter
    private final Map<Long, GarbageLogSegment> garbageLogSegments = new ConcurrentHashMap<>();

    /**
     * Get a list of segments that can be selected for compaction.
     * All the stream log segments are sorted by their start address and then
     * returned, excluding the last N segments where N is the number of protected
     * segments specified in the stream log parameter.
     *
     * @return a list of segments that can be selected for compaction.
     */
    public List<StreamLogSegment> getCompactibleSegments() {
        // TODO: Any race conditions?
        // Create a snapshot of the current streamLogSegments.
        Map<Long, StreamLogSegment> segmentCopy = new HashMap<>(streamLogSegments);

        if (segmentCopy.size() <= logParams.protectedSegments) {
            return Collections.emptyList();
        }

        return segmentCopy
                .values()
                .stream()
                .sorted(Comparator.comparingLong(StreamLogSegment::getStartAddress))
                .limit(segmentCopy.size() - logParams.protectedSegments)
                .collect(Collectors.toList());
    }

    // TODO: Merge method is merge is needed.
}
