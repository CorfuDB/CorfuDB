package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LogData;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by WenbinZhu on 6/20/19.
 */
@Slf4j
@RequiredArgsConstructor
public class SegmentMetaData {

    // Ordinal of the segment.
    @Getter
    private final long ordinal;

    // TODO: do we need atomic here?
    // Total garbage payload size.
    @Getter
    private volatile long garbagePayloadSize;

    // Total payload size, excluding meta data in LogData.
    @Getter
    private volatile long totalPayloadSize;

    // Per-stream compaction marks, any snapshot read on the stream
    // before compaction mark may result in incomplete history.
    private Map<UUID, Long> compactionMarks = new ConcurrentHashMap<>();

    void updatePayloadStats(List<LogData> entries) {
        for (LogData logData : entries) {
            if (logData.getData() == null) {
                log.error("LogData does not have data field, skip recording size, {}", logData);
                continue;
            }
            totalPayloadSize += logData.getData().length;
        }
    }

    /**
     * Get the total size of the garbage payloads in this segment.
     *
     * @return total size of the garbage payloads in MB.
     */
    double getGarbagePayloadSizeMB() {
        return (double) garbagePayloadSize / 1024 / 1024;
    }

    /**
     * Get the garbage ratio of this segment, which is defined by
     * total size of garbage payload  / total payload size.
     *
     * @return garbage ratio of this segment.
     */
    double getGarbageRatio() {
        return (double) garbagePayloadSize / totalPayloadSize;
    }
}
