package org.corfudb.runtime.view;

import com.google.protobuf.Message;
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
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
