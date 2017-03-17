package org.corfudb.runtime.concurrent;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.AbstractObjectTest;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

@Slf4j
public class MultipleNonOverlappingTest extends AbstractObjectTest {

    /**
     * High level:
     *
     * This test will create OBJECT_NUM of objects in an SMRMap. Each object's sum will be incremented by VAL until we
     * reach FINAL_SUM. At the end of this test we:
     *
     *   1) Check that there are OBJECT_NUM number of objects in the SMRMap.
     *   2) Ensure that each object's sum is equal to FINAL_SUM
     *
     * Details (the mechanism by which we increment the values in each object):
     *
     *   1) We create THREAD_NUM number of threads
     *   2) Each thread is given a non-overlapping range on which to increment objects' sum. Each thread will increment
     *      it only once by VAL.
     *   3) We spawn all threads and we ensure that each object in the map is incremented only once. We wait for them
     *      to finish.
     *   4) Repeat the above steps FINAL_SUM number of times.
     */
    @Test
    public void testStress() {

        String mapName = "testMap";
        Map<Long, Long> testMap = instantiateCorfuObject(SMRMap.class, mapName);

        final int VAL = 1;

        // You can fine tune the below parameters. OBJECT_NUM has to be a multiple of THREAD_NUM.
        final int OBJECT_NUM = 20;
        final int THREAD_NUM = 5;

        final int FINAL_SUM = OBJECT_NUM;
        final int STEP = FINAL_SUM / THREAD_NUM;

        Assert.assertTrue(OBJECT_NUM % THREAD_NUM == 0);
        Assert.assertEquals(STEP * THREAD_NUM, FINAL_SUM);

        ArrayList<Thread> threadArray = new ArrayList();

        /**
         * 1) Clear the thread array.
         * 2) Create THREAD_NUM number of threads.
         * 3) Start the threads concurrently and wait for them to finish.
         */
        for (int i = 0; i < FINAL_SUM; i++) {
            threadArray.clear();

            for (int j = 0; j < OBJECT_NUM; j += STEP) {
                threadArray.add(new Thread(new NonOverlappingWriter(i + 1, j, j + STEP, VAL)));
            }

            startAndJoinThreads(threadArray);
        }

        Assert.assertEquals(testMap.size(), FINAL_SUM);
        for (Long value : testMap.values()) {
            Assert.assertEquals((long) FINAL_SUM, (long) value);
        }
    }


    public class NonOverlappingWriter implements Runnable {

        String mapName = "testMap";
        Map<Long, Long> testMap = instantiateCorfuObject(SMRMap.class, mapName);

        int start;
        int end;
        int val;
        int expectedSum;

        public NonOverlappingWriter(int expectedSum, int start, int end, int val) {
            this.expectedSum = expectedSum;
            this.start = start;
            this.end = end;
            this.val = val;
        }

        /**
         * Updates objects between index start and index end.
         */
        public void run() {
            for (int i = start; i < end; i++) {
                simpleCreateImpl(i, val);
            }
        }

        /**
         * Does the actual work.
         *
         * @param idx SMRMap key index to update
         * @param val how much to increment the value by.
         */
        private void simpleCreateImpl(long idx, long val) {

            TXBegin();

            if (!testMap.containsKey(idx)) {
                if (expectedSum-1 > 0)
                    log.debug("OBJ FAIL {} doesn't exist expected={}",
                            idx, expectedSum);
                log.debug("OBJ {} PUT {}", idx, val);
                testMap.put(idx, val);
            } else {
                log.debug("OBJ {} GET", idx);
                Long value = testMap.get(idx);
                if (value != (expectedSum-1))
                    log.debug("OBJ FAIL {} value={} expected={}", idx, value,
                            expectedSum-1);
                log.debug("OBJ {} PUT {}+{}", idx, value, val);
                testMap.put(idx, value + val);
            }

            TXEnd();
        }
    }


    /**
     * Concurrently start the provided collection of threads and wait for them to finish.
     *
     * @param array of threads.
     */
    private void startAndJoinThreads(Collection<Thread> array) {
        // Start the threads.
        array.forEach(t -> t.start());

        // Wait for them to finish.
        try {
            for (Thread t : array) {
                t.join();
            }

        } catch (InterruptedException e) {
            Assert.fail("A thread should not be interrupted: " + e);
        }
    }


    /**
     * A helper function that starts a transaction using Write-Write conflict resolution.
     */
    protected void TXBegin() {
        getRuntime().getObjectsView().TXBuild().setType(TransactionType
                .WRITE_AFTER_WRITE).begin();
    }

    /**
     * A helper function that ends a fransaciton.
     */
    protected void TXEnd() {
        getRuntime().getObjectsView().TXEnd();
    }

    protected <T> T instantiateCorfuObject(Class<T> tClass, String name) {

        // TODO: Does not work at the moment.
        // corfudb.runtime.exceptions.NoRollbackException: Can't roll back due to non-undoable exception
        // getRuntime().getParameters().setUndoDisabled(true).setOptimisticUndoDisabled(true);

        return (T) getRuntime()
                .getObjectsView()
                .build()
                .setStreamName(name)     // stream name
                .setType(tClass)        // object class backed by this stream\
                .open();                // instantiate the object!
    }
}

