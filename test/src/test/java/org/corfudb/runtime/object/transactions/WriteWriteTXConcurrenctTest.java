package org.corfudb.runtime.object.transactions;

import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.CorfuSharedCounter;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Created by dmalkhi on 12/13/16.
 */
public class WriteWriteTXConcurrenctTest extends AbstractViewTest {

    @Before
    public void setupTest() { getDefaultRuntime(); }

    static final int INITIAL = 32;
    static final int OVERWRITE_ONCE = 33;
    static final int OVERWRITE_TWICE = 34;

    /**
     * test concurrent transactions for opacity. This works as follows:
     *
     * (denote n = concurrencySome for brevity.)
     * there are 'n' shared counters numbered 0..n-1, and 'n' tasks executing by an interleaving engine by 'n' threads.
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
     *  Then all tasks try to commit their transacstion, and they all should succeed, since every counter is modified exclusively by one task.
     *
     * @throws Exception
     */
    @Test
    public void testNoWriteConflict() throws Exception {

        final int numTasks = PARAMETERS.NUM_ITERATIONS_MODERATE;
        CorfuSharedCounter[] sharedCounters = new CorfuSharedCounter[numTasks];

        AtomicIntegerArray snapStatus = new AtomicIntegerArray(numTasks);

        for (int i = 0; i < numTasks; i++)
            sharedCounters[i] = getRuntime().getObjectsView()
                    .build()
                    .setStreamName("test"+i)
                    .setType(CorfuSharedCounter.class)
                    .open();

        // initialize all shared counters
        for (int i = 0; i < numTasks; i++)
            sharedCounters[i].setValue(INITIAL);

        // a state-machine:
        ArrayList<BiConsumer<Integer, Integer>> stateMachine = new ArrayList<BiConsumer<Integer, Integer>>();

        // SM step 1: start an optimistic transaction
        stateMachine.add((Integer ignored_thread_num, Integer ignored_task_num) -> {
            getRuntime().getObjectsView().TXBuild()
                    .setType(TransactionType.OPTIMISTIC)
                    .begin();
        });

        // SM step 2: modify shared counter per task
        stateMachine.add((Integer ignored_thread_num, Integer task_num) -> {
            sharedCounters[task_num].setValue(OVERWRITE_ONCE);
        });

        // SM step 3: each task reads a shared counter modified by another task and records it
        stateMachine.add((Integer ignored_thread_num, Integer task_num) -> {
            snapStatus.set(task_num, sharedCounters[(task_num + 1) % numTasks].getValue());
        });

        // SM step 4: each task verifies opacity, checking that it can read its own modified value
        stateMachine.add((Integer ignored_thread_num, Integer task_num) -> {
            assertThat(sharedCounters[task_num].getValue())
                    .isEqualTo(OVERWRITE_ONCE);
        });

        // SM step 5: next, each task overwrites its own value again
        stateMachine.add((Integer ignored_thread_num, Integer task_num) -> {
            sharedCounters[task_num].setValue(OVERWRITE_TWICE);
        } );

        // SM step 6: each task again reads a counter modified by another task.
        // it should read the same snapshot value as the beginning of the transaction
        stateMachine.add((Integer ignored_thread_num, Integer task_num) -> {
            assertThat(sharedCounters[(task_num + 1) % numTasks].getValue())
                    .isEqualTo(snapStatus.get(task_num));
        });

        // SM step 7: each task  again verifies opacity, checking that it can read its own modified value
        stateMachine.add((Integer ignored_thread_num, Integer task_num) -> {
            assertThat(sharedCounters[task_num].getValue())
                    .isEqualTo(OVERWRITE_TWICE);
        });

        // SM step 8: all tasks try to commit their transacstion.
        // Task k aborts if, and only if, counter k+1 was modified after it read it and transaction k+1 already committed.
        stateMachine.add((Integer ignored_thread_num, Integer task_num) -> {
            try {
                TXEnd();
            } catch (TransactionAbortedException tae) {
                fail("Transasction abort without write-write conflict " + task_num);
            }
        } );

        // invoke the interleaving engine
        scheduleInterleaved(PARAMETERS.CONCURRENCY_SOME, numTasks, stateMachine);
    }

    /**
     * test multiple threads optimistically manipulating objects concurrently. This works as follows:
     *
     * (denote n = concurrencySome for brevity.)
     * there are 'n' shared counters numbered 0..n-1, and 'n' tasks executing by an interleaving engine by 'n' threads.
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
     *  If task k aborts, then either task (k-1), or (k+1), or both, must have committed
     *  (wrapping around for tasks n-1 and 0, respectively).
     */
    @Test
    public void testOptimism() throws Exception {

        assertThat(PARAMETERS.CONCURRENCY_SOME).isGreaterThan(1); // don't change concurrency to less than 2, test will break

        final int numTasks = PARAMETERS.NUM_ITERATIONS_MODERATE;
        CorfuSharedCounter[] sharedCounters = new CorfuSharedCounter[numTasks];

        AtomicIntegerArray commitStatus = new AtomicIntegerArray(numTasks);
        final int COMMITVALUE = 1;

        for (int i = 0; i < numTasks; i++)
            sharedCounters[i] = getRuntime().getObjectsView()
                    .build()
                    .setStreamName("test"+i)
                    .setType(CorfuSharedCounter.class)
                    .open();

        // initialize all shared counters
        for (int i = 0; i < numTasks; i++)
            sharedCounters[i].setValue(INITIAL);

        // a state-machine:
        ArrayList<BiConsumer<Integer, Integer>> stateMachine = new ArrayList<BiConsumer<Integer, Integer>>();

        // SM step 1: start an optimistic transaction
        stateMachine.add((Integer ignored_thread_num, Integer ignored_task_num) -> {
            getRuntime().getObjectsView().TXBuild()
                    .setType(TransactionType.OPTIMISTIC)
                    .begin();
        });

        // SM step 2: task k modify counter k
        stateMachine.add((Integer ignored_thread_num, Integer task_num) -> {
            sharedCounters[task_num].setValue(OVERWRITE_ONCE);
        });

        // SM step 3: task k reads counter k+1
        stateMachine.add((Integer ignored_thread_num, Integer task_num) -> {
            assertThat(sharedCounters[(task_num + 1) % numTasks].getValue())
                    .isBetween(INITIAL, OVERWRITE_ONCE);
        });

        // SM step 4: task k verifies opacity, checking that it can read its own modified value of counter k
        stateMachine.add((Integer ignored_thread_num, Integer task_num) -> {
            assertThat(sharedCounters[task_num].getValue())
                    .isEqualTo(OVERWRITE_ONCE);
        });

        // SM step 5: task k overwrites counter k+1
        stateMachine.add((Integer ignored_thread_num, Integer task_num) -> {
            sharedCounters[(task_num+1)%numTasks].setValue(OVERWRITE_TWICE);
        } );

        // SM step 6: task k again check opacity, reading its own modified value, this time of counter k+1
        stateMachine.add((Integer ignored_thread_num, Integer task_num) -> {
            assertThat(sharedCounters[(task_num + 1) % numTasks].getValue())
                    .isEqualTo(OVERWRITE_TWICE);
        });

        // SM step 7: each thread again verifies opacity, checking that it can re-read counter k
        stateMachine.add((Integer ignored_thread_num, Integer task_num) -> {
            assertThat(sharedCounters[task_num].getValue())
                    .isEqualTo(OVERWRITE_ONCE);
        });

        // SM step 8: try to commit all transactions;
        // task k aborts only if one or both of (k-1), (k+1) committed before it
        stateMachine.add((Integer ignored_thread_num, Integer task_num) -> {
            try {
                TXEnd();
                commitStatus.set(task_num, COMMITVALUE);
            } catch (TransactionAbortedException tae) {
                assertThat(commitStatus.get((task_num + 1) % numTasks) == COMMITVALUE ||
                        commitStatus.get((task_num - 1) % numTasks) == COMMITVALUE)
                        .isTrue();
            }
        } );

        // invoke the interleaving engine
        scheduleInterleaved(PARAMETERS.CONCURRENCY_SOME, numTasks, stateMachine);
    }

    void TXEnd() {
        getRuntime().getObjectsView().TXEnd();
    }

    void TXBegin() {
        getRuntime().getObjectsView().TXBuild()
                .setType(TransactionType.WRITE_AFTER_WRITE)
                .begin();
    }}
