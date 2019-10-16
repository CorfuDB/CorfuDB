package org.corfudb.infrastructure.log;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.ToString;
import org.corfudb.format.Types;


/**
 * This class specifies parameters for stream log implementation
 * and the stream log compactor.
 *
 * Created by WenbinZhu on 5/22/19.
 */
@Builder
@ToString
public class StreamLogParams {

    // Region: static members
    public static final int VERSION = 2;

    public static final int METADATA_SIZE = Types.Metadata.newBuilder()
            .setLengthChecksum(-1)
            .setPayloadChecksum(-1)
            .setLength(-1)
            .build()
            .getSerializedSize();

    public static final int RECORDS_PER_SEGMENT = 20_000;

    // End region

    // Region: stream log parameters
    public String logPath;

    public boolean verifyChecksum;

    public double logSizeQuotaPercentage;

    public int maxOpenStreamSegments;

    public int maxOpenGarbageSegments;
    // End region

    // Region: compactor parameters
    public String compactionPolicyType;

    public int compactionInitialDelayMin;

    public int compactionPeriodMin;

    public int compactionWorkers;

    public int maxSegmentsForCompaction;

    public int protectedSegments;

    public double segmentGarbageRatioThreshold;

    public double segmentGarbageSizeThresholdMB;

    public double totalGarbageSizeThresholdMB;
    // End region
}
