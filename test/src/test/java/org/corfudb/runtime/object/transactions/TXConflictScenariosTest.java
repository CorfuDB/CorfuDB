package org.corfudb.runtime.object.transactions;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.collections.ICorfuTable;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by dalia on 12/29/16.
 */
public abstract class TXConflictScenariosTest extends AbstractTransactionContextTest {

    @Test
    @SuppressWarnings("unchecked")
    public void finegrainedUpdatesDoNotConflict()
            throws Exception {
        AtomicBoolean commitStatus = new AtomicBoolean(true);

        ICorfuTable<String, String> testMap = getMap();

        ICorfuTable<String, String> testMap2 = (ICorfuTable<String, String>)
                instantiateCorfuObject(
                        new TypeToken<PersistentCorfuTable<String, String>>() {}, "test stream");

        t(1, () -> TXBegin() );
        t(1, () -> testMap.insert("a", "a") );

        t(2, () -> TXBegin() );
        t(2, () -> testMap2.insert("b", "f") );
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

        ICorfuTable<String, String> testMap = getMap();
        testMap.clear();

        // state 0: start a transaction
        addTestStep((ignored_task_num) -> TXBegin() );

        // state 1: do some puts/gets
        addTestStep( (task_num) -> {

            // put to a task-exclusive entry
            testMap.insert(Integer.toString(task_num),
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
