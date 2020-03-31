package org.corfudb.runtime.concurrent;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuQueue;
import org.corfudb.runtime.collections.CorfuQueue.CorfuRecordId;
import org.corfudb.runtime.collections.CorfuRecord;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by hisundar on 5/30/19.
 */
@Slf4j
public class CorfuQueueTxTest extends AbstractTransactionsTest {
    static int myTable = 0;
    @Override
    public void TXBegin() {
        TXBegin(TransactionType.OPTIMISTIC);
    }

    public void TXBegin(TransactionType type) {
        switch (type) {
            case WRITE_AFTER_WRITE:
                WWTXBegin();
                return;
            case OPTIMISTIC:
                OptimisticTXBegin();
                return;
            default:
                throw new IllegalArgumentException("Unsupported TXN type:" + type.toString());
        }
    }

    protected final int numIterations = PARAMETERS.NUM_ITERATIONS_MODERATE;
    protected final Long numConflictKeys = 2L;

    /**
     * This concurrent test validates that the CorfuQueue::enqueue operations
     * are ordered by that of the parent transaction and fail if transaction aborts.
     *
     * @throws Exception
     */
    @Test
    public void queueOrderedByOptimisticTxn() throws Exception {
        queueOrderedByTransaction(TransactionType.OPTIMISTIC);
    }

    @Test
    public void queueOrderedByWWTxn() throws Exception {
        queueOrderedByTransaction(TransactionType.WRITE_AFTER_WRITE);
    }

    public void queueOrderedByTransaction(TransactionType txnType) throws Exception {
        final int numThreads = PARAMETERS.CONCURRENCY_TWO;
        Map<Long, Long> conflictMap = instantiateCorfuObject(CorfuTable.class, "conflictMap");
        CorfuQueue<String>
                corfuQueue = new CorfuQueue<>(getRuntime(), "testQueue");
        class Record {
            @Getter
            public CorfuRecordId id;
            @Getter
            public String data;

            public Record(CorfuRecordId id, String data) {
                this.id = id;
                this.data = data;
            }
        }
        ArrayList<Record> validator = new ArrayList<>(numThreads * numIterations);
        ReentrantLock lock = new ReentrantLock();

        scheduleConcurrently(numThreads, t ->
        {
            for (Long i = 0L; i < numIterations; i++) {
                String queueData = t.toString() + ":" + i.toString();
                try {
                    TXBegin(txnType);
                    Long coinToss = new Random().nextLong() % numConflictKeys;
                    conflictMap.put(coinToss, coinToss);
                    corfuQueue.enqueue(queueData);
                    // Each transaction may or may not sleep to simulate out of order between enQ & commit
                    TimeUnit.MILLISECONDS.sleep(coinToss);
                    lock.lock();
                    final long streamOffset = TXEnd();
                    validator.add(new Record(new CorfuRecordId(0,0,i), queueData));
                    log.debug("ENQ: {} => {} at {}", i, queueData, streamOffset);
                    lock.unlock();
                } catch (TransactionAbortedException txException) {
                    log.debug("{} ---> Abort!!! ", queueData);
                    // Half the transactions are expected to abort
                    lock.unlock();
                }
            }
        });
        executeScheduled(numThreads, PARAMETERS.TIMEOUT_LONG);

        // Re-open the queue to ensure that the ordering is retrieved from a persisted source.
        CorfuQueue<String>
                corfuQueue2 = new CorfuQueue<>(getRuntime(), "testQueue");

        // After all concurrent transactions are complete, validate that number of Queue entries
        // are the same as the number of successful transactions.
        List<CorfuQueue.CorfuQueueRecord<String>> records = corfuQueue2.entryList();
        assertThat(validator.size()).isEqualTo(records.size());

        // Also validate that the order of the queue matches that of the commit order.
        CorfuRecordId testOrder = new CorfuRecordId(0,0,0);
        for (int i = 0; i < validator.size(); i++) {
            log.debug("Entry:" + records.get(i).getRecordId());
            CorfuRecordId order = records.get(i).getRecordId();
            assertThat(testOrder.compareTo(order)).isLessThanOrEqualTo(0);
            log.debug("queue entry"+i+":"+order+"UUID:"+order.asUUID());
            testOrder = order;
            assertThat(validator.get(i).getData()).isEqualTo(records.get(i).getEntry());
        }
        int idx = validator.size() - 1;
        UUID fromRecId = records.get(idx).getRecordId().asUUID();
        assertThat(fromRecId.getLeastSignificantBits()).isEqualTo(records.get(idx).getRecordId().getEntryId());

        CorfuRecordId backToRecId = new CorfuRecordId(fromRecId);
        assertThat(backToRecId.getEntryId()).isEqualTo(records.get(idx).getRecordId().getEntryId());
        assertThat(backToRecId.getEpoch()).isEqualTo(records.get(idx).getRecordId().getEpoch());
        log.debug("UUID from Record {} = {}", idx, fromRecId);
        assertThat(backToRecId.getSequence()).isEqualTo(records.get(idx).getRecordId().getSequence());
        log.debug("RecordId back from UUID = {}", backToRecId);
    }

    //Assist the test that the log address values are not in the same order of enqueue.
    public void queueOutOfOrderedByTransaction(TransactionType txnType, boolean inOrder) throws Exception {
        Semaphore semId = new Semaphore(1);
        Semaphore semTx= new Semaphore(1);
        myTable = 0;
        semId.acquire();
        semTx.acquire();

        int semIdOwner;
        int semTxOwner;

        if (inOrder) {
            semIdOwner = 0;
            semTxOwner = 0;
        } else {
            semIdOwner = 0;
            semTxOwner = 1;
        }

        final int numThreads = PARAMETERS.CONCURRENCY_TWO;
        Map<Integer, Map<Long, Long>> tables = new HashMap<>();

        for (int i = 0; i < numThreads; i++) {
            tables.put(i, instantiateCorfuObject(CorfuTable.class, "testTable" +i));
        }

        CorfuQueue<String>
                corfuQueue = new CorfuQueue<>(getRuntime(), "testQueue");
        class Record {
            @Getter
            public CorfuRecordId id;
            @Getter
            public String data;

            public Record(CorfuRecordId id, String data) {
                this.id = id;
                this.data = data;
            }
        }

        Map<Long, String> validator = new Hashtable<>();
        ReentrantLock lock = new ReentrantLock();
        scheduleConcurrently(numThreads, t ->
        {
            int tableID;
            lock.lock();
            tableID = myTable++;
            lock.unlock();

            log.info("\nmy tableID :" + tableID + " numIterations: " + numIterations);

            Map<Long, Long> testTable = tables.get(tableID);

            for (Long i = 0L; i < numIterations; i++) {
                String queueData = t.toString() + ":" + i.toString();
                try {
                    TXBegin(txnType);
                    Long coinToss = new Random().nextLong() % numConflictKeys;
                    testTable.put(coinToss, coinToss);

                    //enforce the second thread enqueue later
                    if (tableID != semIdOwner) {
                        semId.acquire();
                    }

                    corfuQueue.enqueue(queueData);

                    if (tableID == semIdOwner) {
                        semId.release();
                    }

                    //enforce the first thread get enQueue first, but
                    if (tableID != semTxOwner) {
                        semTx.acquire();
                    }

                    final long streamOffset = TXEnd();
                    if (tableID == semTxOwner) {
                        semTx.release();
                    }

                    //hashtable update is synchronized
                    validator.put(streamOffset, queueData);
                    log.debug("ENQ:" + "=>" + queueData + " at " + streamOffset);
                } catch (TransactionAbortedException txException) {
                    log.warn(queueData + " ---> Abort!!! ");
                    assertThat(0);
                }
            }
        });

        executeScheduled(numThreads, PARAMETERS.TIMEOUT_LONG);

        // After all concurrent transactions are complete, validate that number of Queue entries
        // are the same as the number of successful transactions.
        List<CorfuQueue.CorfuQueueRecord<String>> records = corfuQueue.entryList();
        assertThat(validator.size()).isEqualTo(records.size());

        Map<Long, String> sortedMap = new TreeMap<>(validator);

        int i = 0;
        int cnt = 0;
        for (Map.Entry<Long, String> entry : sortedMap.entrySet()) {
            CorfuRecordId id = records.get(i).getRecordId();
            String val0 = entry.getValue();
            String val1 = records.get(i).getEntry();

            if (entry.getKey() != id.getSequence() || !val0.equals(val1)) {
                log.warn("\nentry: " + entry + " queue item: " + records.get(i));
                cnt++;
            }
            i++;
        }

        assertThat(cnt).isZero();
    }

    @Test
    public void queueInOrderedByWWTxn() throws Exception {
        queueOutOfOrderedByTransaction(TransactionType.WRITE_AFTER_WRITE, true);
    }

    @Test
    public void queueInOrderedByOptimTxn() throws Exception {
        queueOutOfOrderedByTransaction(TransactionType.OPTIMISTIC, true);
    }

    @Test
    public void queueOutOfOrderedByWWTxn() throws Exception {
        queueOutOfOrderedByTransaction(TransactionType.WRITE_AFTER_WRITE, false);
    }

    @Test
    public void queueOutOfOrderedByOptimTxn() throws Exception {
        queueOutOfOrderedByTransaction(TransactionType.OPTIMISTIC, false);
    }
}

