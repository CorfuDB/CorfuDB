package org.corfudb.runtime.object.transactions;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.CorfuSharedCounter;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.StreamView;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dalia on 12/8/16.
 */
public class OptimisticTXConcurrencyTest extends AbstractViewTest {

    @Before
    public void setupTest() { getDefaultRuntime(); }

    static final int INITIAL = 32;
    static final int OVERWRITE_ONCE = 33;
    static final int OVERWRITE_TWICE = 34;

    @Test
    public void OptimisticTXSimpleTest() {
        CorfuSharedCounter sharedCounter1 = getRuntime().getObjectsView()
                .build()
                .setStreamName("test"+1)
                .setType(CorfuSharedCounter.class)
                .open() ;
        sharedCounter1.setValue(INITIAL);

        t(1, this::TXBegin);
        t(1, () -> sharedCounter1.setValue(OVERWRITE_ONCE));
        t(2, () -> sharedCounter1.setValue(OVERWRITE_TWICE));
        t(1, () -> sharedCounter1.getValue());

    }

    int numTasks;
    ArrayList<CorfuSharedCounter> sharedCounters;

    /**
     * build an array of shared counters for the test
     */
    private void setupCounters() {

        numTasks = PARAMETERS.NUM_ITERATIONS_MODERATE;
        sharedCounters = new ArrayList<>();

        for (int i = 0; i < numTasks; i++)
            sharedCounters.add(i, getRuntime().getObjectsView()
                    .build()
                    .setStreamName("test"+i)
                    .setType(CorfuSharedCounter.class)
                    .open() );

        // initialize all shared counters
        for (int i = 0; i < numTasks; i++)
            sharedCounters.get(i).setValue(INITIAL);

    }

    ArrayList<IntConsumer> getOpacityTestSM() {
        // populate numTasks and sharedCounters array
        setupCounters();

        AtomicIntegerArray snapStatus = new AtomicIntegerArray(numTasks);
        AtomicIntegerArray commitStatus = new AtomicIntegerArray(numTasks);
        final int COMMITVALUE = 1;

        // a state-machine:
        ArrayList<IntConsumer> stateMachine = new ArrayList<IntConsumer>();

        // SM step 1: start an optimistic transaction
        stateMachine.add((ignored_task_num) -> {
            TXBegin();
        });

        // SM step 2: modify shared counter per task
        stateMachine.add((task_num) -> {
            sharedCounters.get(task_num).setValue(OVERWRITE_ONCE);
        });

        // SM step 3: each task reads a shared counter modified by another task and records it
        stateMachine.add((task_num) -> {
            snapStatus.set(task_num, sharedCounters.get((task_num + 1) % numTasks).getValue());
        });

        // SM step 4: each task verifies opacity, checking that it can read its own modified value
        stateMachine.add((task_num) -> {
            assertThat(sharedCounters.get(task_num).getValue())
                    .isEqualTo(OVERWRITE_ONCE);
        });

        // SM step 5: next, each task overwrites its own value again
        stateMachine.add((task_num) -> {
            sharedCounters.get(task_num).setValue(OVERWRITE_TWICE);
        } );

        // SM step 6: each task again reads a counter modified by another task.
        // it should read the same snapshot value as the beginning of the transaction
        stateMachine.add((task_num) -> {
            assertThat(sharedCounters.get((task_num + 1) % numTasks).getValue())
                    .isEqualTo(snapStatus.get(task_num));
        });

        // SM step 7: each task  again verifies opacity, checking that it can read its own modified value
        stateMachine.add((task_num) -> {
            assertThat(sharedCounters.get(task_num).getValue())
                    .isEqualTo(OVERWRITE_TWICE);
        });

        // SM step 8: all tasks try to commit their transacstion.
        // Task k aborts if, and only if, counter k+1 was modified after it read it and transaction k+1 already committed.
        stateMachine.add((task_num) -> {
            try {
                TXEnd();
                commitStatus.set(task_num, COMMITVALUE);
            } catch (TransactionAbortedException tae) {
                assertThat(sharedCounters.get((task_num + 1) % numTasks).getValue())
                        .isNotEqualTo(snapStatus.get(task_num));
                assertThat(commitStatus.get((task_num + 1) % numTasks))
                        .isEqualTo(COMMITVALUE);
            }
        } );

        return stateMachine;
    }


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
     *  Then all tasks try to commit their transacstion.
     *  Task aborts if, and only if, counter k+1 was modified after it read it and transaction k+1 already committed.
     *
     * @throws Exception
     */
    @Test
    public void testOpacityInterleaved() throws Exception {
        // invoke the interleaving engine
        scheduleInterleaved(PARAMETERS.CONCURRENCY_SOME, numTasks, getOpacityTestSM());
    }

    @Test
    public void testOpacityThreaded() throws Exception {
        // invoke the threaded engine
        scheduleThreaded(PARAMETERS.CONCURRENCY_SOME, numTasks, getOpacityTestSM());
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
     *  If task k aborts, then either task (k-1), or (k+1), or both, must have committed
     *  (wrapping around for tasks n-1 and 0, respectively).
     */
    @Test
    public void testOptimism() throws Exception {
        // populate numTasks and sharedCounters array
        setupCounters();

        AtomicIntegerArray commitStatus = new AtomicIntegerArray(numTasks);
        final int COMMITVALUE = 1;

        assertThat(numTasks).isGreaterThan(1); // don't change concurrency to less than 2, test will break

        // a state-machine:
        ArrayList<IntConsumer> stateMachine = new ArrayList<IntConsumer>();

        // SM step 1: start an optimistic transaction
        stateMachine.add((ignored_task_num) -> {
            TXBegin();
        });

        // SM step 2: task k modify counter k
        stateMachine.add((task_num) -> {
            sharedCounters.get(task_num).setValue(OVERWRITE_ONCE);
        });

        // SM step 3: task k reads counter k+1
        stateMachine.add((task_num) -> {
            assertThat(sharedCounters.get((task_num + 1) % numTasks).getValue())
                    .isBetween(INITIAL, OVERWRITE_ONCE);
        });

        // SM step 4: task k verifies opacity, checking that it can read its own modified value of counter k
        stateMachine.add((task_num) -> {
            assertThat(sharedCounters.get(task_num).getValue())
                    .isEqualTo(OVERWRITE_ONCE);
        });

        // SM step 5: task k overwrites counter k+1
        stateMachine.add((task_num) -> {
            sharedCounters.get((task_num+1)%numTasks).setValue(OVERWRITE_TWICE);
        } );

        // SM step 6: task k again check opacity, reading its own modified value, this time of counter k+1
        stateMachine.add((task_num) -> {
            assertThat(sharedCounters.get((task_num + 1) % numTasks).getValue())
                    .isEqualTo(OVERWRITE_TWICE);
        });

        // SM step 7: each thread again verifies opacity, checking that it can re-read counter k
        stateMachine.add((task_num) -> {
            assertThat(sharedCounters.get(task_num).getValue())
                    .isEqualTo(OVERWRITE_ONCE);
        });

        // SM step 8: try to commit all transactions;
        // task k aborts only if one or both of (k-1), (k+1) committed before it
        stateMachine.add((task_num) -> {
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
                .setType(TransactionType.OPTIMISTIC)
                .begin();
    }
}
