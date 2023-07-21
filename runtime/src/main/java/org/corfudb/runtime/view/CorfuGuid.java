package org.corfudb.runtime.view;

import lombok.Getter;

import java.time.Instant;

/**
 * Globally Unique Identity generator that returns ids
 * with weak comparable ordering with minimal distributed state sync.
 * On startup increment a globally unique id via Corfu Object.
 * Use this id with locally incrementing values to return unique ids.
 * <p>
 * Created by Sundar Sridharan on 5/22/19.
 */

public class CorfuGuid {
    public static final int MAX_CORRECTION = 0xF;
    @Getter
    private final long timestamp;
    private final int driftCorrection;
    private final int resolutionCorrection;
    @Getter
    private final long uniqueInstanceId;

    CorfuGuid(long UTCtimestamp, int driftCorrection,
              int resolutionCorrection, long uniqueInstanceId) {
        this.timestamp = UTCtimestamp;
        this.driftCorrection = driftCorrection;
        this.resolutionCorrection = resolutionCorrection;
        this.uniqueInstanceId = uniqueInstanceId;
    }

    private static final long TIMESTAMP_MSB_MASK = 0x0000007FffFF0000L;
    private static final long TIMESTAMP_LSB_MASK = 0x000000000000FFffL;
    private static final long INSTANCE_ID_MASK = 0x000000000000FFffL;
    private static final int CORRECTION_MASK = MAX_CORRECTION;

    private static final long TIMESTAMP_MSB_SHIFT = 24;
    private static final long DRIFT_ADJUST_SHIFT = 36;
    private static final long TIMESTAMP_LSB_SHIFT = 20;
    private static final long INSTANCE_ID_SHIFT = 4;

    /**
     * Construct the CorfuGuid with the UTC + GloballyUniqueCounter + Corrections
     * There are 2 types of corrections necessary to ensure uniqueness:
     * 1. UTC wall clock time can drift backwards - Drift Adjust.
     * 2. UTC resolution may be coarse if many threads try to generate a
     * guid simultaneously - Resolution Adjust.
     * <p>
     * Can tolerate up to 16 NTP time adjustments without global state sync.
     * Can tolerate up to 65 seconds of time drifts without loss of ordering.
     * +---------------+--------------+---------------+-------------+-------------------+
     * | Timestamp MSB | Drift Adjust | Timestamp LSB | Instance ID | Resolution Adjust |
     * +---------------+--------------+---------------+-------------+-------------------+
     * <----24 bits----><----4 bits---><----16 bits---><---16 bits--><-------4 bits----->
     *
     * @return - build the guid out of the timestamp from parts as shown above
     */
    public long getLong() {
        final long timestampMSB = timestamp & TIMESTAMP_MSB_MASK;
        final long driftAdjust = driftCorrection & CORRECTION_MASK;
        final long timestampLSB = timestamp & TIMESTAMP_LSB_MASK;
        final long instanceId = uniqueInstanceId & INSTANCE_ID_MASK;
        final long resolutionAdjust = resolutionCorrection & CORRECTION_MASK;

        return ((timestampMSB << TIMESTAMP_MSB_SHIFT) |
                (driftAdjust << DRIFT_ADJUST_SHIFT) |
                (timestampLSB << TIMESTAMP_LSB_SHIFT) |
                (instanceId << INSTANCE_ID_SHIFT) |
                resolutionAdjust);
    }

    CorfuGuid(long encodedTs) {
        long currentTimestamp = System.currentTimeMillis();
        final long timestampMSB = (encodedTs >> TIMESTAMP_MSB_SHIFT) & TIMESTAMP_MSB_MASK;
        final long timestampLSB = (encodedTs >> TIMESTAMP_LSB_SHIFT) & TIMESTAMP_LSB_MASK;
        timestamp = (currentTimestamp & (~(TIMESTAMP_MSB_MASK | TIMESTAMP_LSB_MASK)))
                | timestampMSB | timestampLSB;
        driftCorrection = (int) ((encodedTs >> DRIFT_ADJUST_SHIFT) & CORRECTION_MASK);
        uniqueInstanceId = (int) ((encodedTs >> INSTANCE_ID_SHIFT) & INSTANCE_ID_MASK);
        resolutionCorrection = (int) (encodedTs & CORRECTION_MASK);
    }

    public static long getTimestampFromGuid(long corfuGuidAsLong, long currentTimestamp) {
        final long timestampMSB = (corfuGuidAsLong >> TIMESTAMP_MSB_SHIFT) & TIMESTAMP_MSB_MASK;
        final long timestampLSB = (corfuGuidAsLong >> TIMESTAMP_LSB_SHIFT) & TIMESTAMP_LSB_MASK;
        return (currentTimestamp & (~(TIMESTAMP_MSB_MASK | TIMESTAMP_LSB_MASK)))
                | timestampMSB | timestampLSB;
    }

    public String toString() {
        return String.format("%s: drift: %d resolution: %d instanceId %d",
                Instant.ofEpochMilli(timestamp).toString(),
                driftCorrection, resolutionCorrection, uniqueInstanceId);
    }
}
