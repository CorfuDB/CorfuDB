package org.corfudb.infrastructure.log;

import lombok.Builder;
import lombok.Builder.Default;
import org.corfudb.format.Types;
import org.corfudb.infrastructure.log.compression.Codec;

import java.util.concurrent.TimeUnit;


/**
 * This class specifies parameters for stream log implementation
 * and the stream log compactor.
 *
 * Created by WenbinZhu on 5/22/19.
 */
@Builder
public class StreamLogParams {

    // Region: static members
    public static final int VERSION = 2;

    public static final int METADATA_SIZE = Types.Metadata.newBuilder()
            .setLengthChecksum(-1)
            .setPayloadChecksum(-1)
            .setLength(-1)
            .build()
            .getSerializedSize();
    // End region

    // Region: stream log parameters
    public String logPath;

    @Default
    public boolean verifyChecksum = true;

    @Default
    public int recordsPerSegment = 20_000;

    @Default
    public double logSizeQuotaPercentage = 100.0;

    @Default
    public Codec.Type compressionCodec = Codec.Type.None;
    // End region

    // Region: compactor parameters
    @Default
    public String compactionPolicyType = "GARBAGE_SIZE_FIRST";

    @Default
    public long compactorInitialDelay = 20;

    @Default
    public long compactorPeriod = 20;

    @Default
    public TimeUnit compactorTimeUnit = TimeUnit.MINUTES;

    @Default
    public int compactorWorkers = Runtime.getRuntime().availableProcessors() + 1;

    @Default
    public int maxSegmentsForCompaction = 20;

    /**
     * Number of newest segments that will not be selected for compaction.
     * Note: This number should be at least 1.
     */
    @Default
    public int protectedSegments = 1;

    @Default
    public double segmentGarbageRatioThreshold = 0.5;

    @Default
    public double segmentGarbageSizeThresholdMB = 128;

    @Default
    public double totalGarbageSizeThresholdMB = 5 * 1024;
    // End region
}
