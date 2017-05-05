package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.CorfuSharedCounter;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dalia on 12/29/16.
 */
public abstract class TXConflictScenariosTest extends AbstractTransactionContextTest {

    /**
     * test concurrent transactions for opacity. This works as follows:
     *
     * there are 'numTasks' shared counters numbered 0..numTasks-1,
     * and 'numTasks' tasks executing by an interleaving engine by CONCURRENCY_SOME number of  threads.
     *
     * all counters are initialized to INITIAL.
     *
     * Each task is specified as a state machine.
     * The state machine starts a transaction.
     * Then it repeats twice modify, read own counter, read other counter.
     * Specifically,
     *  - task j modifies counter j to OVERWRITE_ONCE,
     *  - task j reads counter j (own), expecting to read OVERWRITE_ONCE,
     *  - task j reads counter (j+1) mod n, and records the value it reads,
     *
     *  - then task j modifies counter j to OVERWRITE_TWICE,
     *  - task j reads counter j (own), expecting to read OVERWRITE_TWICE,
     *  - task j reads counter (j+1) mod n, expecting to read the same value as before,
     *
     *  Then all tasks try to commit their transactions.
     *
     * @throws Exception
     */
    void testOpacity(boolean testInterleaved)
            throws Exception {
        // populate numTasks and sharedCounters array
        setupCounters();

        // SM step 1: start an optimistic transaction
        addTestStep((ignored_task_num) -> {
            TXBegin();
        });

        // SM step 2: modify shared counter per task
        addTestStep((task_num) -> {
            sharedCounters.get(task_num).setValue(OVERWRITE_ONCE);
        });

        // SM step 3: each task reads a shared counter modified by another task and records it
        addTestStep((task_num) -> {
            snapStatus.set(task_num, sharedCounters.get((task_num + 1) % numTasks).getValue());
        });

        // SM step 4: each task verifies opacity, checking that it can read its own modified value
        addTestStep((task_num) -> {
            assertThat(sharedCounters.get(task_num).getValue())
                    .isEqualTo(OVERWRITE_ONCE);
        });

        // SM step 5: next, each task overwrites its own value again
        addTestStep((task_num) -> {
            sharedCounters.get(task_num).setValue(OVERWRITE_TWICE);
        });

        // SM step 6: each task again reads a counter modified by another task.
        // it should read the same snapshot value as the beginning of the transaction
        addTestStep((task_num) -> {
            assertThat(sharedCounters.get((task_num + 1) % numTasks).getValue())
                    .isEqualTo(snapStatus.get(task_num));
        });

        // SM step 7: each task  again verifies opacity, checking that it can read its own modified value
        addTestStep((task_num) -> {
            assertThat(sharedCounters.get(task_num).getValue())
                    .isEqualTo(OVERWRITE_TWICE);
        });

        // SM step 8: all tasks try to commit their transacstion.
        // Task k aborts if, and only if, counter k+1 was modified after it read it and transaction k+1 already committed.
        addTestStep((task_num) -> {
            try {
                TXEnd();
                commitStatus.set(task_num, COMMITVALUE);
            } catch (TransactionAbortedException tae) {
                // do nothing
            }
        });

        if (testInterleaved)
            scheduleInterleaved(PARAMETERS.CONCURRENCY_SOME, numTasks);
        else
            scheduleThreaded(PARAMETERS.CONCURRENCY_SOME, numTasks);


    }

    /**
     * test multiple threads optimistically manipulating objects concurrently. This works as follows:
     *
     * there are 'numTasks' shared counters numbered 0..numTasks-1,
     * and 'numTasks' tasks executing by an interleaving engine by CONCURRENCY_SOME number of  threads.
     *
     * all counters are initialized to INITIAL.
     *
     * Each task is specified as a state machine.
     * The state machine starts a transaction.
     * Within each transaction, each task repeats twice modify, read own, read other.
     *
     * Specifically,
     *  - task j modifies counter j to OVERWRITE_ONCE,
     *  - task j reads counter j (own), expecting to read OVERWRITE_ONCE,
     *  - task j reads counter (j+1) mod n, expecting to read either OVERWRITE_ONCE or INITIAL,
     *
     *  - then task j modifies counter (j+1) mod n to OVERWRITE_TWICE,
     *  - task j reads counter j+1 mod n (own), expecting to read OVERWRITE_TWICE,
     *  - task j reads counter j (the one it changed before), expecting to read OVERWRITE_ONCE ,
     *
     *  Then all tasks try to commit their transasctions.
     */
    void testRWConflicts(boolean testInterleaved)
            throws Exception {
        // populate numTasks and sharedCounters array
        setupCounters();

        assertThat(numTasks).isGreaterThan(1); // don't change concurrency to less than 2, test will break

        // SM step 1: start an optimistic transaction
        addTestStep((ignored_task_num) -> {
            TXBegin();
        });

        // SM step 2: task k modify counter k
        addTestStep((task_num) -> {
            sharedCounters.get(task_num).setValue(OVERWRITE_ONCE);
        });

        // SM step 3: task k reads counter k+1
        addTestStep((task_num) -> {
            assertThat(sharedCounters.get((task_num + 1) % numTasks).getValue())
                    .isBetween(INITIAL, OVERWRITE_ONCE);
        });

        // SM step 4: task k verifies opacity, checking that it can read its own modified value of counter k
        addTestStep((task_num) -> {
            assertThat(sharedCounters.get(task_num).getValue())
                    .isEqualTo(OVERWRITE_ONCE);
        });

        // SM step 5: task k overwrites counter k+1
        addTestStep((task_num) -> {
            sharedCounters.get((task_num + 1) % numTasks).setValue(OVERWRITE_TWICE);
        });

        // SM step 6: task k again check opacity, reading its own modified value, this time of counter k+1
        addTestStep((task_num) -> {
            assertThat(sharedCounters.get((task_num + 1) % numTasks).getValue())
                    .isEqualTo(OVERWRITE_TWICE);
        });

        // SM step 7: each thread again verifies opacity, checking that it can re-read counter k
        addTestStep((task_num) -> {
            assertThat(sharedCounters.get(task_num).getValue())
                    .isEqualTo(OVERWRITE_ONCE);
        });

        // SM step 8: try to commit all transactions;
        // task k aborts only if one or both of (k-1), (k+1) committed before it
        addTestStep((task_num) -> {
            try {
                TXEnd();
                commitStatus.set(task_num, COMMITVALUE);
            } catch (TransactionAbortedException tae) {
                // do nothing
            }
        });

        // invoke the execution engine
        if (testInterleaved)
            scheduleInterleaved(PARAMETERS.CONCURRENCY_SOME, numTasks);
        else
            scheduleThreaded(PARAMETERS.CONCURRENCY_SOME, numTasks);

    }

    void testNoWriteConflictSimple() throws Exception {

        final CorfuSharedCounter sharedCounter1 =
                instantiateCorfuObject(CorfuSharedCounter.class, "test"+1);
        final CorfuSharedCounter sharedCounter2 =
                instantiateCorfuObject(CorfuSharedCounter.class, "test"+2);

        commitStatus = new AtomicIntegerArray(2);

        t(1, this::TXBegin);
        t(2, this::TXBegin);

        t(1, () -> { sharedCounter1.setValue(OVERWRITE_ONCE);});
        t(2, () -> { sharedCounter2.setValue(OVERWRITE_ONCE);});

        t(1, () -> sharedCounter1.getValue());
        t(2, () -> sharedCounter2.getValue());

        t(1, () -> sharedCounter2.getValue());
        t(2, () -> sharedCounter1.getValue());

        t(1, () -> { sharedCounter1.setValue(OVERWRITE_TWICE);});
        t(2, () -> { sharedCounter2.setValue(OVERWRITE_TWICE);});

        t(1, () -> sharedCounter1.getValue());
        t(1, () -> sharedCounter2.getValue());

        t(2, () -> sharedCounter2.getValue());
        t(2, () -> sharedCounter1.getValue());

        t(1, () -> {
            try {
                TXEnd();
                commitStatus.set(0, COMMITVALUE);
            } catch (TransactionAbortedException tae) {
                // do nothing
            }
        });

        t(2, () -> {
            try {
                TXEnd();
                commitStatus.set(1, COMMITVALUE);
            } catch (TransactionAbortedException tae) {
                // do nothing
            }
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    public void finegrainedUpdatesDoNotConflict()
            throws Exception {
        AtomicBoolean commitStatus = new AtomicBoolean(true);

        Map<String, String> testMap = getMap();

        Map<String, String> testMap2 = (Map<String, String>)
                instantiateCorfuObject(
                        new TypeToken<SMRMap<String, String>>() {}, "test stream");

        t(1, () -> TXBegin() );
        t(1, () -> testMap.put("a", "a") );
        t(1, () -> assertThat(testMap.put("a", "b"))
                .isEqualTo("a") );

        t(2, () -> TXBegin() );
        t(2, () -> testMap2.put("b", "f") );
        t(2, () -> assertThat(testMap2.put("b", "g"))
                .isEqualTo("f") );
        t(2, () -> TXEnd() );

        t(1, () -> {
                    try {
                        TXEnd();
                    } catch (TransactionAbortedException te) {
                        commitStatus.set(false);
                    }
                } );

        // verify that both transactions committed successfully
        assertThat(commitStatus.get())
                .isTrue();
    }

    void getAbortTestSM() {
        Random rand = new Random(PARAMETERS.SEED);

        Map<String, String> testMap = getMap();
        testMap.clear();

        // state 0: start a transaction
        addTestStep((ignored_task_num) -> TXBegin() );

        // state 1: do some puts/gets
        addTestStep( (task_num) -> {

            // put to a task-exclusive entry
            testMap.put(Integer.toString(task_num),
                    Integer.toString(task_num));

            // do some gets arbitrarily at random
            for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++)
                testMap.get(Integer.toString(rand.nextInt(PARAMETERS.NUM_ITERATIONS_MODERATE)));
        });

        // state 2 (final): ask to commit the transaction
        addTestStep( (task_num) -> {
            try {
                TXEnd();
                commitStatus.set(task_num, COMMITVALUE);
            } catch (TransactionAbortedException tae) {
            }
        });
    }

    public void concurrentAbortTest(boolean testInterleaved)
            throws Exception
    {
        final int numThreads =  PARAMETERS.CONCURRENCY_SOME;
        final int numRecords = PARAMETERS.NUM_ITERATIONS_VERY_LOW;
        numTasks = numThreads*numRecords;

        long startTime = System.currentTimeMillis();

        commitStatus = new AtomicIntegerArray(numTasks);

        getAbortTestSM();

        // invoke the execution engine
        if (testInterleaved)
            scheduleInterleaved(numThreads, numTasks);
        else
            scheduleThreaded(numThreads, numTasks);

        int aborts = 0;
        for (int i = 0; i < numTasks; i++)
            if (commitStatus.get(i) != COMMITVALUE)
                aborts++;

        // print stats..
        calculateRequestsPerSecond("TPS", numRecords * numThreads, startTime);
        calculateAbortRate(aborts, numRecords * numThreads);
    }
}
