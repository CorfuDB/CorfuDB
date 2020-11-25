package org.corfudb.runtime.view;

import com.google.protobuf.Message;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.Queue.CorfuGuidMsg;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;

import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Globally Unique Identity generator that returns ids
 * with weak comparable ordering with minimal distributed state sync.
 * On startup increment a globally unique id via Corfu Object.
 * Use this id with locally incrementing values to return unique ids.
 *
 * Created by Sundar Sridharan on 5/22/19.
 */

class CorfuGuid {
    public static final int MAX_CORRECTION = 0xF;
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

    private static final long TIMESTAMP_MSB_MASK = 0x000000FFffFF0000L;
    private static final long TIMESTAMP_LSB_MASK = 0x000000000000FFffL;
    private static final long INSTANCE_ID_MASK   = 0x000000000000FFffL;
    private static final int CORRECTION_MASK     = MAX_CORRECTION;

    private static final long TIMESTAMP_MSB_SHIFT = 24;
    private static final long DRIFT_ADJUST_SHIFT  = 36;
    private static final long TIMESTAMP_LSB_SHIFT = 20;
    private static final long INSTANCE_ID_SHIFT   = 4;

    /**
     * Construct the CorfuGuid with the UTC + GloballyUniqueCounter + Corrections
     * There are 2 types of corrections necessary to ensure uniqueness:
     * 1. UTC wall clock time can drift backwards - Drift Adjust.
     * 2. UTC resolution may be coarse if many threads try to generate a
     *    guid simultaneously - Resolution Adjust.
     *
     * Can tolerate up to 16 NTP time adjustments without global state sync.
     * Can tolerate up to 65 seconds of time drifts without loss of ordering.
     * +---------------+--------------+---------------+-------------+-------------------+
     * | Timestamp MSB | Drift Adjust | Timestamp LSB | Instance ID | Resolution Adjust |
     * +---------------+--------------+---------------+-------------+-------------------+
     * <----24 bits----><----4 bits---><----16 bits---><---16 bits--><-------4 bits----->
     * @return - build the guid out of the timestamp from parts as shown above
     */
    public long getLong() {
        final long timestampMSB     = timestamp & TIMESTAMP_MSB_MASK;
        final long driftAdjust      = driftCorrection & CORRECTION_MASK;
        final long timestampLSB     = timestamp & TIMESTAMP_LSB_MASK;
        final long instanceId       = uniqueInstanceId & INSTANCE_ID_MASK;
        final long resolutionAdjust = resolutionCorrection & CORRECTION_MASK;

        return ((timestampMSB     << TIMESTAMP_MSB_SHIFT) |
                (driftAdjust      << DRIFT_ADJUST_SHIFT)  |
                (timestampLSB     << TIMESTAMP_LSB_SHIFT) |
                (instanceId       << INSTANCE_ID_SHIFT)   |
                resolutionAdjust);
    }

    CorfuGuid(long encodedTs) {
        long currentTimestamp = System.currentTimeMillis();
        final long timestampMSB = (encodedTs >> TIMESTAMP_MSB_SHIFT) & TIMESTAMP_MSB_MASK;
        final long timestampLSB = (encodedTs >> TIMESTAMP_LSB_SHIFT) & TIMESTAMP_LSB_MASK;
        timestamp = (currentTimestamp & (~(TIMESTAMP_MSB_MASK | TIMESTAMP_LSB_MASK)))
                | timestampMSB  | timestampLSB;
        driftCorrection = (int)((encodedTs >> DRIFT_ADJUST_SHIFT) & CORRECTION_MASK);
        uniqueInstanceId = (int)((encodedTs >> INSTANCE_ID_SHIFT) & INSTANCE_ID_MASK);
        resolutionCorrection = (int)(encodedTs & CORRECTION_MASK);
    }

    public String toString() {
        return String.format("%s: drift: %d resolution: %d instanceId %d",
                Instant.ofEpochMilli(timestamp).toString(),
                driftCorrection, resolutionCorrection, uniqueInstanceId);
    }
}

@Slf4j
public class CorfuGuidGenerator implements OrderedGuidGenerator {
    private final String GUID_STREAM_NAME = "CORFU_GUID_COUNTER_STREAM";

    private Table<CorfuGuidMsg, CorfuGuidMsg, Message> distributedCounter;

    private final CorfuStore corfuStore;

    /**
     * Initialize timestamp & correction to MAX to force an update on the instance id on first call.
     * This prevents the instance id from bumping up if there are no calls made to this class.
     */
    private int driftCorrection = CorfuGuid.MAX_CORRECTION;
    private int resolutionCorrection = CorfuGuid.MAX_CORRECTION;
    private long previousTimestamp = Long.MAX_VALUE;
    private long instanceId = 0L;

    private static CorfuGuidGenerator singletonCorfuGuidGenerator;

    private CorfuGuidGenerator(CorfuRuntime rt) {
        corfuStore = new CorfuStore((rt));
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        boolean success;
        try {
            success = executorService.submit(() -> {
                try {
                    distributedCounter = corfuStore.openTable(TableRegistry.CORFU_SYSTEM_NAMESPACE, GUID_STREAM_NAME,
                            CorfuGuidMsg.class,
                            CorfuGuidMsg.class,
                            null,
                            TableOptions.builder().build());
                } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                    log.error("CorfuGuidGenerator: failed to open the instanceId table", e);
                    return false;
                }
                return true;
            }).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("CorfuGuidGenerator: failed to initialize", e);
            success = false;
        }
        if (!success) {
            throw new RuntimeException("Unable to initialize the Guid Generator");
        }
    }

    /**
     * Singleton initialization of the CorfuGuidGenerator.
     *
     * @param rt - CorfuRuntime initialized with connectivity to a live corfu cluster.
     * @return - singleton instance of the CorfuGuidGenerator on success
     */
    public synchronized static CorfuGuidGenerator getInstance(CorfuRuntime rt) {
        if (singletonCorfuGuidGenerator == null) {
            singletonCorfuGuidGenerator = new CorfuGuidGenerator(rt);
        }
        return singletonCorfuGuidGenerator;
    }

    @Override
    public UUID nextUUID() {
        throw new UnsupportedOperationException("Not supported yet");
    }

    /** Sync up with the distributed state using a Corfu Transaction
     * Synchronous Network RPCs among other overheads
     **/
    private void updateInstanceId() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        final long badInstanceId = -1L;
        long nextInstanceID;
        try {
            nextInstanceID = executorService.submit(() -> {
                long reasonableNumberOfRetries = 64;
                while (reasonableNumberOfRetries-- > 0) {
                    try (TxnContext txn = corfuStore.txn(TableRegistry.CORFU_SYSTEM_NAMESPACE)) {
                        final int GUID_INSTANCE_ID_KEY = 0xdeadbeef;
                        CorfuGuidMsg key = CorfuGuidMsg.newBuilder()
                                .setInstanceId(GUID_INSTANCE_ID_KEY).build();

                        CorfuStoreEntry<CorfuGuidMsg, CorfuGuidMsg, Message> prevEntry;
                        prevEntry = txn.getRecord(distributedCounter, key);
                        if (prevEntry.getPayload() == null) {
                            final long startingInstanceId = 1L;
                            txn.putRecord(distributedCounter, key,
                                    CorfuGuidMsg.newBuilder().setInstanceId(startingInstanceId).build(),
                                    null);
                            txn.commit();
                            return startingInstanceId;
                        }
                        txn.putRecord(distributedCounter, key,
                                CorfuGuidMsg.newBuilder().setInstanceId(prevEntry.getPayload().getInstanceId() + 1)
                                        .build(),
                                null);
                        txn.commit();
                        return prevEntry.getPayload().getInstanceId() + 1;
                    } catch (TransactionAbortedException e) {
                        log.error("updateInstanceId: Transaction aborted while updating GUID counter", e);
                    }
                }
                return badInstanceId;
            }).get(); // run it in a separate thread since we do not want the parent transaction to abort on retries.
        } catch (InterruptedException | ExecutionException e) {
            log.error("updateInstanceId: Interrupted while updating GUID counter", e);
            nextInstanceID = badInstanceId;
        }
        if (nextInstanceID == badInstanceId) {
            throw new RuntimeException("CorfuGuidGenerator: unable to get a new globally unique instance id!");
        }
        this.instanceId = nextInstanceID;
    }

    /**
     * @return a globally unique id with minimal distributed sync up
     */
    @Override
    public long nextLong() {
        return getCorrectedTimestamp().getLong();
    }

    /**
     * 1. Get Current timestamp and compare it with previous timestamp
     *   -> if time has drifted backwards bump up driftCorrection
     *     -> if driftCorrection has maxed out, bump up instance Id (loss of ordering)
     *   -> if time is same as previous
     *    -> bump up resolutionCorrection
     *     -> if resolutionCorrection has maxed out, bump up instance Id (possible loss of ordering)
     *
     * @return Parts of the Guid with best effort global ordering in parts
     */
    private synchronized CorfuGuid getCorrectedTimestamp() {
        long currentTimestamp = System.currentTimeMillis();
        if (currentTimestamp == previousTimestamp) {
            resolutionCorrection++;
            // If more than 16 concurrent threads are calling this api, we can hit this.
            // Simple solution is to make the last thread wait a few microseconds.
            // This avoids an transactional call.
            if (resolutionCorrection > CorfuGuid.MAX_CORRECTION) {
                while (currentTimestamp == previousTimestamp) {
                    currentTimestamp = System.currentTimeMillis();
                }
                resolutionCorrection = 0;
            }
        } else {
            resolutionCorrection = 0;
        }

        if (currentTimestamp < previousTimestamp) {
            driftCorrection++;
            if (driftCorrection > CorfuGuid.MAX_CORRECTION) {
                log.warn("updateInstanceId: Time went backward too many times "+
                                " timestamp={} previousTimestamp={} correction={} instanceId={}",
                        currentTimestamp, previousTimestamp, driftCorrection, instanceId);
                driftCorrection = 0;
                updateInstanceId();
            }
        }
        previousTimestamp = currentTimestamp;
        return new CorfuGuid(currentTimestamp, driftCorrection, resolutionCorrection, instanceId);
    }

    public String toString(long encodedTs) {
        return new CorfuGuid(encodedTs).toString();
    }
}
