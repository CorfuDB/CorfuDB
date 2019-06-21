package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionType;

import java.util.UUID;

/**
 * Globally Unique Identity generator that returns ids
 * with weak comparable ordering with minimal distributed state sync.
 * On startup increment a globally unique id via Corfu Object.
 * Use this id with locally incrementing values to return unique ids.
 * Higher bits are made of an UTC timestamp while lower bits carry the
 * counter.
 *
 * Created by Sundar Sridharan on 5/22/19.
 */
@Slf4j
public class CorfuGuidGenerator implements OrderedGuidGenerator {
    private final static int TIMESTAMP_SHIFT            = 24;
    private final static int INSTANCE_ID_SHIFT          = 8;
    private final static int MAX_TIMESTAMP_CORRECTION   = 0xFF;
    private final static int MAX_INSTANCE_ID            = 0xFFff;

    private final String GUID_STREAM_NAME = "CORFU_GUID_COUNTER_STREAM";
    private final Integer GUID_STREAM_KEY = 0xdeadbeef;

    private final SMRMap<Integer, Long> distributedCounter;
    private final CorfuRuntime runtime;

    /**
     * Initialize timestamp & correction to MAX to force an update on the instance id on first call.
     * This prevents the instance id from bumping up if there are no calls made to this class.
     */
    private long timestampCorrection = MAX_TIMESTAMP_CORRECTION;
    private long previousTimestamp = Long.MAX_VALUE;
    private long instanceId = 0L;

    public CorfuGuidGenerator(CorfuRuntime rt) {
        runtime = rt;
        distributedCounter = rt.getObjectsView().build()
                               .setType(SMRMap.class)
                               .setStreamName(GUID_STREAM_NAME)
                               .open();
    }

    private long updateInstanceId() {
        long nextInstanceId = 0L;
        while(true) {
            try {
                runtime.getObjectsView().TXBuild()
                        .type(TransactionType.OPTIMISTIC)
                        .build()
                        .begin();

                nextInstanceId = distributedCounter.getOrDefault(GUID_STREAM_KEY, 1L) + 1;
                distributedCounter.put(GUID_STREAM_KEY, nextInstanceId);

                runtime.getObjectsView().TXEnd();
                break;
            } catch (TransactionAbortedException e) {
                log.error("updateInstanceId: Transaction aborted while updating GUID counter", e);
            }
        }

        instanceId = nextInstanceId;
        return nextInstanceId;
    }

    /**
     * +----------------+--------------------+------------+
     * |  UTC Timestamp | Unique Instance ID | Correction |
     * +----------------+--------------------+------------+
     * <----5 bytes----><------2 bytes------><---1 byte--->
     *
     * @return a globally unique id with minimal distributed sync up
     */
    @Override
    public long nextLong() {
        long currentTimestamp = getCorrectedTimestamp();
        return (currentTimestamp << TIMESTAMP_SHIFT) |
                ((instanceId & MAX_INSTANCE_ID) << INSTANCE_ID_SHIFT) |
                (timestampCorrection & MAX_TIMESTAMP_CORRECTION);
    }

    /**
     * +-------------------++--------------+------------+
     * | UTC Timestamp     || Instance Id  | Correction |
     * +-------------------++--------------+------------+
     * <------8 byte-------><---7 byte------><--1 byte-->
     *
     * @return a higher resolution UUID with a timestamp built in.
     */
    @Override
    public UUID nextUUID() {
        long currentTimestamp = getCorrectedTimestamp();
        return new UUID(currentTimestamp,
                (instanceId << INSTANCE_ID_SHIFT) | (timestampCorrection & MAX_TIMESTAMP_CORRECTION));

    }

    /**
     * Ensure that timestamp is monotonically increasing and if not,
     * 1. First bump up the correction
     * 2. If correction has also rolled over, bump up instance id
     *
     * @return currentTimestamp but with side-effects to object state
     */
    private synchronized long getCorrectedTimestamp() {
        long currentTimestamp = System.currentTimeMillis();
        if (currentTimestamp <= previousTimestamp) {
            timestampCorrection++;
            if (timestampCorrection > MAX_TIMESTAMP_CORRECTION) {
                timestampCorrection = 0;
                log.info("updateInstanceId: CorfuGuidGenerator corrected timestamp "+
                                MAX_TIMESTAMP_CORRECTION+
                                " times!"+
                                " timestamp={} previousTimestamp={} correction={} instanceId={}",
                        currentTimestamp, previousTimestamp, timestampCorrection, instanceId);
                updateInstanceId();
            }
        }
        previousTimestamp = currentTimestamp;
        return currentTimestamp;
    }

}
