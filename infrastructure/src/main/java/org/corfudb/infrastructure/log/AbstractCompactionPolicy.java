package org.corfudb.infrastructure.log;

import lombok.RequiredArgsConstructor;
import org.corfudb.infrastructure.ResourceQuota;

import java.io.IOException;
import java.nio.file.FileStore;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by WenbinZhu on 8/11/19.
 */
@RequiredArgsConstructor
public abstract class AbstractCompactionPolicy implements CompactionPolicy {

    protected final StreamLogParams params;

    protected final ResourceQuota logSizeQuota;

    protected final FileStore fileStore;

    /**
     * Returns a list of segments selected for compaction based on
     * the compaction policy implementation.
     *
     * @param compactibleSegments metadata of unprotected segments that
     *                            can be selected for compaction
     * @return a list of ordinals whose associated segments are selected for compaction.
     */
    @Override
    public abstract List<Long> getSegmentsToCompact(List<CompactionMetadata> compactibleSegments);

    /**
     * Check if a force compaction is needed, which ignore the current policy.
     */
    boolean requireForceCompaction(StreamLogParams logParams,
                                   FileStore fileStore,
                                   ResourceQuota logSizeQuota,
                                   List<CompactionMetadata> compactibleSegments) {
        return quotaExceeded(logParams, fileStore, logSizeQuota)
                || garbageSizeExceeds(logParams, compactibleSegments);
    }

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

    private boolean garbageSizeExceeds(StreamLogParams logParams,
                                       List<CompactionMetadata> compactibleSegments) {
        double totalGarbageSize = compactibleSegments.stream()
                .map(CompactionMetadata::getTotalGarbageSizeMB)
                .reduce(0.0, Double::sum);
        return totalGarbageSize > logParams.totalGarbageSizeThresholdMB;
    }

    /**
     * Returns a list of segments for compaction regardless of the policy.
     * <p>
     * The input list is first sorted by segment garbage size, and return
     * at most N segments that contains the most garbage, where N is the
     * user specified maximum number of segments to compaction per cycle.
     */
    List<Long> getSegmentsToForceCompact(List<CompactionMetadata> compactibleSegments) {
        return compactibleSegments
                .stream()
                .filter(meta -> meta.getTotalGarbageSizeMB() > 0)
                .sorted(Comparator.comparing(CompactionMetadata::getTotalGarbageSizeMB).reversed())
                .limit(params.maxSegmentsForCompaction)
                .map(CompactionMetadata::getOrdinal)
                .collect(Collectors.toList());
    }

}
