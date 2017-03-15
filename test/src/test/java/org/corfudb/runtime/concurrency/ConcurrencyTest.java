package org.corfudb.runtime.concurrency;

import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.AbstractObjectTest;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;


public class ConcurrencyTest extends AbstractObjectTest {

    private static final String MAP_NAME = "ConcurrentTest";

    final int VAL = 1;

    // You can fine tune the below parameters. OBJECT_NUM has to be a multiple of THREAD_NUM.
    final int OBJECT_NUM = 50;
    final int THREAD_NUM = 5;

    final int FINAL_SUM = OBJECT_NUM;
    final int STEP = FINAL_SUM / THREAD_NUM;

    ArrayList<Thread> threadArray = new ArrayList();

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
    public void nonOverlappingConcurrentTest() {

        Map<Long, Long> testMap = instantiateCorfuObject(SMRMap.class, MAP_NAME);
        testMap.clear();

        Assert.assertTrue(OBJECT_NUM % THREAD_NUM == 0);
        Assert.assertEquals(STEP * THREAD_NUM, FINAL_SUM);


        /**
         * 1) Clear the thread array.
         * 2) Create THREAD_NUM number of threads.
         * 3) Start the threads concurrently and wait for them to finish.
         */
        for (int i = 0; i < FINAL_SUM; i++) {
            threadArray.clear();

            for (int start = 0; start < OBJECT_NUM; start += STEP) {
                threadArray.add(new Thread(new NonOverlappingWriter(start, start + STEP, VAL)));
            }

            startAndJoinThreads(threadArray);
        }

        Assert.assertEquals(testMap.size(), FINAL_SUM);
        testMap.values().forEach(finalVal -> Assert.assertEquals((long) FINAL_SUM, (long) finalVal));
    }


    /**
     * High level:
     *
     * This test will create OBJECT_NUM of objects in an SMRMap. Each object's sum will be incremented by VAL until we
     * reach FINAL_SUM * THREAD_NUM. At the end of this test we:
     *
     *   1) Check that there are OBJECT_NUM number of objects in the SMRMap.
     *   2) Ensure that each object's sum is equal to FINAL_SUM * THREAD_NUM
     *
     * Details (the mechanism by which we increment the values in each object):
     *
     *   1) We create THREAD_NUM number of threads
     *   2) Each thread is given an overlapping range on which to increment objects' sum. Each thread will increment
     *      it only once by VAL.
     *   3) We spawn all threads and we ensure that each object in the map is incremented only once. We wait for them
     *      to finish.
     *   4) Repeat the above steps FINAL_SUM number of times.
     */
    @Test
    public void overlappingConcurrentTest() {

        Map<Long, Long> testMap = instantiateCorfuObject(SMRMap.class, MAP_NAME);
        testMap.clear();

        Assert.assertTrue(OBJECT_NUM % THREAD_NUM == 0);
        Assert.assertEquals(STEP * THREAD_NUM, FINAL_SUM);


        /**
         * 1) Clear the thread array.
         * 2) Create THREAD_NUM number of threads.
         * 3) Start the threads concurrently and wait for them to finish
         */
        for (int i = 0; i < FINAL_SUM; i++) {
            threadArray.clear();

            for (int j = 0; j < THREAD_NUM; j++ ) {
                threadArray.add(new Thread(new OverlappingWriter(0, OBJECT_NUM, VAL)));
            }

            startAndJoinThreads(threadArray);
        }

        Assert.assertEquals(testMap.size(), OBJECT_NUM);
        testMap.values().forEach(finalVal -> Assert.assertEquals((long) FINAL_SUM * THREAD_NUM, (long) finalVal));
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
        return (T) getRuntime()
                .getObjectsView()
                .build()
                .setStreamName(name)     // stream name
                .setType(tClass)        // object class backed by this stream\
                .open();                // instantiate the object!
    }


    public abstract class Writer implements Runnable {
        final Map<Long, Long> testMap = instantiateCorfuObject(SMRMap.class, MAP_NAME);

        int start;
        int end;
        int val;

        public Writer(int start, int end, int val) {
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


        abstract void simpleCreateImpl(long idx, long val);
    }

    public class NonOverlappingWriter extends Writer {

        public NonOverlappingWriter(int start, int end, int val) {
            super(start, end, val);
        }

        /**
         * Does the actual work.
         *
         * @param idx SMRMap key index to update
         * @param val how much to increment the value by.
         */
        @Override
        void simpleCreateImpl(long idx, long val) {

            TXBegin();

            if (!testMap.containsKey(idx)) {
                testMap.put(idx, val);
            } else {
                Long value = testMap.get(idx);
                testMap.put(idx, value + val);
            }

            TXEnd();
        }
    }

    public class OverlappingWriter extends Writer {

        public OverlappingWriter(int start, int end, int val) {
            super(start, end, val);
        }

        /**
         * Does the actual work.
         *
         * @param idx SMRMap key index to update
         * @param val how much to increment the value by.
         */
        @Override
        void simpleCreateImpl(long idx, long val) {
            try {

                TXBegin();

                if (!testMap.containsKey(idx)) {
                    testMap.put(idx, val);
                } else {
                    Long value = testMap.get(idx);
                    testMap.put(idx, value + val);
                }

                TXEnd();

            } catch (TransactionAbortedException e) {
                simpleCreateImpl(idx, val); // Retry.
            }
        }
    }
}

