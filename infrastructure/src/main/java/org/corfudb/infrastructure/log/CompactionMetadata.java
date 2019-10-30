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
 * Per-segment metadata for compaction.
 * <p>
 * Created by WenbinZhu on 6/20/19.
 */
@Slf4j
@RequiredArgsConstructor
@ToString
class CompactionMetadata {

    // Ordinal of the segment.
    @Getter
    private final long ordinal;

    // Total payload size in bytes, excluding meta data in LogData.
    private volatile long totalPayloadSize;

    // Total garbage payload size in bytes.
    private volatile long totalGarbageSize;

    // Total size of the garbage in bytes whose marker addresses
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

    /**
     * Get the total size of the garbage in this segment in MB.
     *
     * @return total size of the garbage payloads in MB
     */
    double getTotalGarbageSizeMB() {
        return (double) totalGarbageSize / 1024 / 1024;
    }

    /**
     * Get the size of the bounded garbage in this segment in MB.
     *
     * @return total size of the garbage payloads in MB
     */
    double getBoundedGarbageSizeMB() {
        return (double) boundedGarbageSize / 1024 / 1024;
    }

    /**
     * Get the garbage ratio of this segment, which is defined by
     * total size of garbage payload  / total payload size.
     *
     * @return garbage ratio of this segment
     */
    double getGarbageRatio() {
        return (double) totalGarbageSize / totalPayloadSize;
    }

    /**
     * Get the bounded garbage ratio of this segment, which is defined
     * by size of bounded garbage payload  / total payload size.
     *
     * @return bounded garbage ratio of this segment
     */
    double getBoundedGarbageRatio() {
        return (double) boundedGarbageSize / totalPayloadSize;
    }
}
