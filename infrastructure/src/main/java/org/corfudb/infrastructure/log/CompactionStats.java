package org.corfudb.infrastructure.log;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.logprotocol.SMRGarbageEntry;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.runtime.view.Address;

import java.util.Collection;

/**
 * Per-segment compaction stats.
 * <p>
 * Created by WenbinZhu on 6/20/19.
 */
@Slf4j
@RequiredArgsConstructor
@ToString
@Getter
@SuppressWarnings("NonAtomicOperationOnVolatileField")
class CompactionStats {

    // ID of the segment.
    private final SegmentId segmentId;

    // Number of LogData stored in the corresponding stream segment.
    private volatile int streamEntryCount;

    // Total payload size in bytes, excluding meta data in LogData.
    private volatile long totalPayloadSize;

    // Total garbage payload size in bytes.
    private volatile long totalGarbageSize;

    // Size of the garbage in bytes whose marker addresses
    // are bounded by nextCompactionUpperBound.
    private volatile long boundedGarbageSize;

    // Local compactionUpperBound used to track the update of
    // the global static nextCompactionUpperBound below.
    private volatile long compactionUpperBound = Address.MAX;

    // Set by compaction policy to record bounded garbage size (total
    // sizes of garbage entries whose maker addresses are not greater
    // than this value), which will be used in the next compaction cycle
    // to select segments suitable for compaction.
    @Getter
    @Setter
    static volatile long nextCompactionUpperBound = Address.MAX;

    // A guarantee set by compaction policy implementation that ensures
    // specific entries will not be compacted in current compaction cycle
    // if, after being compacted, would move their stream's compaction mark
    // greater than this value.
    @Getter
    @Setter
    static volatile long currCompactionUpperBound = Address.MAX;

    /**
     * Update number of LogData stored in the corresponding stream segment.
     */
    void updateStreamEntryCount(int count) {
        streamEntryCount += count;
    }

    /**
     * Update total payload size for this segment.
     *
     * @param entries entries appended to log which contains size information
     */
    void updateTotalPayloadSize(Collection<LogData> entries) {
        for (LogData logData : entries) {
            if (logData.getType() == DataType.COMPACTED) {
                continue;
            }

            if (logData.getType() != DataType.DATA) {
                log.debug("updateTotalPayloadSize: LogData[{}] is not DATA or COMPACTED " +
                        "type, skip recording stats. Type: {}",
                        logData.getGlobalAddress(), logData.getType());
                continue;
            }

            if (!logData.hasPayloadSize()) {
                log.error("updateTotalPayloadSize: entry {} payload size unknown, skip.", logData);
                return;
            }

            totalPayloadSize += logData.getPayloadSize();
        }
    }

    /**
     * Update garbage size for this segment.
     *
     * @param garbageEntries garbage entries appended to log,
     *                       which contains garbage size information
     */
    void updateGarbageSize(Collection<SMRGarbageEntry> garbageEntries) {
        for (SMRGarbageEntry garbageEntry : garbageEntries) {
            updateBoundedGarbageSize(garbageEntry);
            updateTotalGarbageSize(garbageEntry);
        }
    }

    private void updateTotalGarbageSize(SMRGarbageEntry garbageEntry) {
        totalGarbageSize += garbageEntry.getGarbageSize();
    }

    private void updateBoundedGarbageSize(SMRGarbageEntry garbageEntry) {
        if (compactionUpperBound != nextCompactionUpperBound) {
            compactionUpperBound = nextCompactionUpperBound;
            boundedGarbageSize = totalGarbageSize;
        }
        boundedGarbageSize += garbageEntry.getGarbageSizeUpTo(compactionUpperBound);
    }
}
