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
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class CorfuGuidGenerator implements OrderedGuidGenerator {
    public static final String GUID_STREAM_NAME = "CORFU_GUID_COUNTER_STREAM";

    private Table<CorfuGuidMsg, CorfuGuidMsg, Message> distributedCounter;

    private final CorfuStore corfuStore;

    /**
     * Initialize timestamp & correction to MAX to force an update on the instance id on first call.
     * This prevents the instance id from bumping up if there are no calls made to this class.
     */
    private int driftCorrection = CorfuGuid.MAX_CORRECTION;
    private int resolutionCorrection = CorfuGuid.MAX_CORRECTION;
    private long previousTimestamp = Long.MAX_VALUE;
    private volatile long instanceId = 0L;
    private AtomicLong txnCounter = new AtomicLong();

    private static CorfuGuidGenerator singletonCorfuGuidGenerator;

    private CompletableFuture<Void> instanceIdGetterFuture;

    // 8388608 (~8.3M) new transactions involving Q writes while an old txn is still running before overflow can occur
    public static final long MAX_TXN_ID = 1L << 23;
    // 1048576 (~1M) new jvms restarts while the oldest is still operational before overflow can occur
    public static final long MAX_INSTANCE_ID = 1L << 20;
    public static final long TXN_ID_SHIFT = 40;
    public static final long TXN_ID_MASK      = 0x7fFFff0000000000L;
    public static final long INSTANCE_ID_MASK = 0x000000FffFF00000L;
    public static final long TXN_COUNTER_MASK = 0x00000000000FffFFL;
    public static final long INSTANCE_ID_SHIFT = 20;
    // 1048576 (~1M) write operations per transaction before an error is thrown
    public static final long MAX_TXN_Q_ELEMENTS = 1L << 20;
    // SIGN bit|23 bits of txn id| 20 bits instance id| 20 bits local counter

    public CorfuGuidGenerator(CorfuRuntime rt) {
        corfuStore = new CorfuStore((rt));
        instanceIdGetterFuture = CompletableFuture.runAsync(this::generateNewInstanceId);

        try {
            instanceIdGetterFuture.get(5, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to get instance id", e);
        } catch (TimeoutException e) {
            log.warn("Not going to wait more than 5 seconds for a new instance id");
        }
    }

    private void generateNewInstanceId() {
        long reasonableNumberOfRetries = 64;
        long newInstanceId;
        Table<CorfuGuidMsg, CorfuGuidMsg, Message> distributedCounterTable = null;
        while (reasonableNumberOfRetries-- > 0) {
            try {
                distributedCounterTable = corfuStore.getTable(
                        TableRegistry.CORFU_SYSTEM_NAMESPACE, GUID_STREAM_NAME);
            } catch (IllegalArgumentException | NoSuchElementException e) {
                try {
                    distributedCounterTable = corfuStore.openTable(
                            TableRegistry.CORFU_SYSTEM_NAMESPACE, GUID_STREAM_NAME,
                            CorfuGuidMsg.class,
                            CorfuGuidMsg.class,
                            null,
                            TableOptions.builder().build());
                } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException ee) {
                    log.error("CorfuGuidGenerator: failed to open the instanceId table", e);
                    continue;
                }
            }
            distributedCounter = distributedCounterTable;

            try (TxnContext txn = corfuStore.txn(TableRegistry.CORFU_SYSTEM_NAMESPACE)) {
                final int GUID_INSTANCE_ID_KEY = 0xdeadbeef;
                CorfuGuidMsg key = CorfuGuidMsg.newBuilder()
                        .setInstanceId(GUID_INSTANCE_ID_KEY).build();

                CorfuStoreEntry<CorfuGuidMsg, CorfuGuidMsg, Message> prevEntry;
                prevEntry = txn.getRecord(distributedCounterTable, key);
                if (prevEntry.getPayload() == null) {
                    newInstanceId = 1L; // Starting value should not be zero
                    txn.putRecord(distributedCounterTable, key,
                            CorfuGuidMsg.newBuilder().setInstanceId(newInstanceId).build(),
                            null);
                    txn.commit();
                    this.instanceId = newInstanceId;
                    return;
                }
                newInstanceId = (prevEntry.getPayload().getInstanceId() + 1) % MAX_INSTANCE_ID;
                txn.putRecord(distributedCounterTable, key,
                        CorfuGuidMsg.newBuilder().setInstanceId(newInstanceId)
                                .build(),
                        null);
                txn.commit();
                this.instanceId = newInstanceId;
                return;
            } catch (TransactionAbortedException e) {
                log.error("updateInstanceId: Transaction aborted while updating GUID counter", e);
            }
        }
    }

    public long getInstanceId() {
        if (instanceId != 0) {
            return instanceId;
        }
        try {
            instanceIdGetterFuture.get();
            return instanceId;
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("CorfuGuidGenerator unable to get Id ", e);
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

    public long nextLong(TxnContext txnContext, Table queue) {
        long txnIdForQueue = txnContext.getTxnIdForQueues();
        if (txnContext.getTxnIdForQueues() == 0) {
            txnIdForQueue = generateIdOncePerTxn(getInstanceId());
            log.trace("Generating new txn id for queue {}: {}", queue.getFullyQualifiedTableName(), txnIdForQueue);
        } else {
            txnIdForQueue = generateNextIdInTxn(txnIdForQueue);
            log.trace("Generating incremental id for queue {}: {}", queue.getFullyQualifiedTableName(), txnIdForQueue);
        }
        txnContext.setTxnIdForQueues(txnIdForQueue);
        return txnIdForQueue;
    }

    private long generateIdOncePerTxn(long instId) {
        long newTxnId = txnCounter.incrementAndGet();
        newTxnId = (newTxnId<<TXN_ID_SHIFT) & TXN_ID_MASK;
        newTxnId = newTxnId | ((instId<< INSTANCE_ID_SHIFT )&INSTANCE_ID_MASK);
        return newTxnId;
    }

    private long generateNextIdInTxn(long lastQId) {
        long nextQId = lastQId & TXN_COUNTER_MASK;
        nextQId = nextQId + 1;
        if (nextQId >= MAX_TXN_Q_ELEMENTS) {
            throw new UnsupportedOperationException("More than "+MAX_TXN_Q_ELEMENTS+" not supported in 1 txn");
        }
        nextQId = (lastQId & (~TXN_COUNTER_MASK)) | nextQId;
        return nextQId;
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
