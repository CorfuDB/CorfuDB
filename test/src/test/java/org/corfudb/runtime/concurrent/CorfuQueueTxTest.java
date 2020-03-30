package org.corfudb.runtime.concurrent;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.CorfuQueue;
import org.corfudb.runtime.collections.CorfuQueue.CorfuRecordId;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static java.lang.Thread.sleep;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    protected final int numIterations = PARAMETERS.NUM_ITERATIONS_LOW;
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
                    lock.lock();
                    CorfuRecordId id = corfuQueue.enqueue(queueData);
                    final long streamOffset = TXEnd();
                    validator.add(new Record(id, queueData));
                    log.debug("ENQ:" + id + "=>" + queueData + " at " + streamOffset);
                    lock.unlock();
                } catch (TransactionAbortedException txException) {
                    log.debug(queueData + " ---> Abort!!! ");
                    // Half the transactions are expected to abort
                    lock.unlock();
                }
            }
        });
        executeScheduled(numThreads, PARAMETERS.TIMEOUT_LONG);

        // After all concurrent transactions are complete, validate that number of Queue entries
        // are the same as the number of successful transactions.
        List<CorfuQueue.CorfuQueueRecord<String>> records = corfuQueue.entryList();
        assertThat(validator.size()).isEqualTo(records.size());

        // Also validate that the order of the queue matches that of the commit order.
        for (int i = 0; i < validator.size(); i++) {
            log.debug("Entry:" + records.get(i).getRecordId());
            assertThat(validator.get(i).getId().equals(records.get(i).getRecordId()));
            assertThat(validator.get(i).getData()).isEqualTo(records.get(i).getEntry());
        }

        // Validate that one cannot compare ID from enqueue with ID from entryList()
        assertThatThrownBy(() -> validator.get(0).getId().compareTo(records.get(0).getRecordId())).
                isExactlyInstanceOf(IllegalArgumentException.class);
    }


    //Assist the test that the log address values are not  in the same order of enqueue.
    public void queueOutOfOrderedByTransaction(TransactionType txnType) throws Exception {

        final int numThreads = PARAMETERS.CONCURRENCY_TWO;
        Map<Integer, Map<Long, Long>> tables = new HashMap<>();

        for (int i = 0; i < numThreads; i++) {
            tables.put(i, instantiateCorfuObject(CorfuTable.class, "testTable" +i));
        }

        //Map<Long, Long> conflictMap = instantiateCorfuObject(CorfuTable.class, "conflictMap");


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

        Map<Long, CorfuRecordId> validator = new Hashtable<>();
        ReentrantLock lock = new ReentrantLock();
        scheduleConcurrently(numThreads, t ->
        {
            int tableID;
            lock.lock();
            tableID = myTable++;
            lock.unlock();

            System.out.print("\nmy tableID :" + tableID + " numIterations: " + numIterations);

            Map<Long, Long> testTable = tables.get(tableID);

            for (Long i = 0L; i < 10; i++) {
                String queueData = t.toString() + ":" + i.toString();
                try {
                    TXBegin(txnType);
                    Long coinToss = new Random().nextLong() % numConflictKeys;
                    testTable.put(coinToss, coinToss);

                    //enforce the second thread enqueue later
                    if (tableID != 0) {
                        sleep(100);
                    }
                    CorfuRecordId id = corfuQueue.enqueue(queueData);

                    //enforce the first thread get enQueue first, but
                    if (tableID != 0) {
                        sleep(100);
                    }

                    final long streamOffset = TXEnd();

                    //hashtable update is synchronized
                    validator.put(streamOffset, id);
                    log.debug("ENQ:" + id + "=>" + queueData + " at " + streamOffset);
                } catch (TransactionAbortedException txException) {
                    assertThat(0);
                    System.out.print(queueData + " ---> Abort!!! ");
                }
            }
        });

        executeScheduled(numThreads, PARAMETERS.TIMEOUT_LONG);

        // After all concurrent transactions are complete, validate that number of Queue entries
        // are the same as the number of successful transactions.
        List<CorfuQueue.CorfuQueueRecord<String>> records = corfuQueue.entryList();
        assertThat(validator.size()).isEqualTo(records.size());

        Map<Long, CorfuRecordId> sortedMap = new TreeMap<Long, CorfuRecordId>(validator);

        int i = 0;
        int cnt = 0;
        for (Map.Entry<Long, CorfuRecordId> entry : sortedMap.entrySet()) {
            CorfuRecordId id = records.get(i).getRecordId();
            if (!entry.getValue().equals(id)) {
                cnt++;
                System.out.print("\ncnt " + cnt +" address: " + entry.getKey() + " id: "+ entry.getValue() + " queue index: " + i + " id: " + id);
            }
            i++;
        }

        assertThat(cnt).isNotZero();
    }

    @Test
    public void queueOutOfOrderedByWWTxn() throws Exception {
        queueOutOfOrderedByTransaction(TransactionType.WRITE_AFTER_WRITE);
    }

    @Test
    public void queueOutOfOrderedByOptimTxn() throws Exception {
        queueOutOfOrderedByTransaction(TransactionType.OPTIMISTIC);
    }

}

