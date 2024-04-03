package org.corfudb.runtime.checkpoint;

import org.corfudb.AbstractCorfuTest;
import org.corfudb.AbstractCorfuTest.CallableConsumer;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.test.CorfuTableSpec;

public class PeriodicCkPointSpec implements CorfuTableSpec {
    @Override
    public void test(CorfuTableSpecContext ctx) throws Exception {
        final int tableSize = AbstractCorfuTest.PARAMETERS.NUM_ITERATIONS_LOW;

        PersistentCorfuTable<String, Long> tableB = openTable(rt, streamNameB);

        // thread 1: populates the maps with mapSize items
        scheduleConcurrently(1, ignored_task_num -> {
            populateMaps(tableSize, tableA, tableB);
        });

        // thread 2: periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        // thread 1: perform a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times
        scheduleConcurrently(1, ignored_task_num -> {
            mapCkpoint(rt, tableA, tableB);
        });

        // thread 3: repeated ITERATION_LOW times starting a fresh runtime, and instantiating the maps.
        // they should rebuild from the latest checkpoint (if available).
        // performs some sanity checks on the map state
        scheduleConcurrently(PARAMETERS.NUM_ITERATIONS_LOW, ignored_task_num -> {
            validateMapRebuild(tableSize, false, false);
        });

        executeScheduled(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.TIMEOUT_LONG);

        // finally, after all three threads finish, again we start a fresh runtime and instantiate the maps.
        // This time the we check that the new map instances contains all values
        validateMapRebuild(tableSize, true, false);
    }

    /**
     * Schedule a task to run concurrently when executeScheduled() is called multiple times.
     *
     * @param repetitions The number of times to repeat execution of the function.
     * @param function    The function to run.
     */
    public void scheduleConcurrently(int repetitions, CallableConsumer function) {
        for (int i = 0; i < repetitions; i++) {
            final int taskNumber = i;

            scheduledThreads.add(() -> {
                // executorService uses Callable functions
                // here, wrap a Corfu test CallableConsumer task (input task #, no output) as a Callable.
                function.accept(taskNumber);
                return null;
            });
        }
    }

    /**
     * initialize the two tables, the second one is all zeros
     *
     * @param tableSize table size
     */
    void populateMaps(int tableSize, PersistentCorfuTable<String, Long> table1, PersistentCorfuTable<String, Long> table2) {
        for (int i = 0; i < tableSize; i++) {
            try {
                table1.insert(String.valueOf(i), (long) i);
                table2.insert(String.valueOf(i), (long) 0);
            } catch (TrimmedException te) {
                // shouldn't happen
                te.printStackTrace();
                throw te;
            }
        }
    }
}
