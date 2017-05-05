package org.corfudb.runtime.concurrent;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

@Slf4j
public class MultipleNonOverlappingTest extends AbstractTransactionsTest {
    @Override
    public void TXBegin() { WWTXBegin(); }



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
    public void testStress() throws Exception {

        String mapName = "testMapA";
        Map<Long, Long> testMap = instantiateCorfuObject(SMRMap.class, mapName);

        final int VAL = 1;

        // You can fine tune the below parameters. OBJECT_NUM has to be a multiple of THREAD_NUM.
        final int OBJECT_NUM = 20;
        final int THREAD_NUM = 5;

        final int FINAL_SUM = OBJECT_NUM;
        final int STEP = OBJECT_NUM / THREAD_NUM;

        // test all objects advance in lock-step to FINAL_SUM
        for (int i = 0; i < FINAL_SUM; i++) {

            for (int j = 0; j < OBJECT_NUM; j += STEP) {
                NonOverlappingWriter n = new NonOverlappingWriter(i + 1, j, j + STEP, VAL);
                scheduleConcurrently(t -> { n.dowork();});
            }
            executeScheduled(THREAD_NUM, PARAMETERS.TIMEOUT_NORMAL);
        }

        Assert.assertEquals(testMap.size(), FINAL_SUM);
        for (Long value : testMap.values()) {
            Assert.assertEquals((long) FINAL_SUM, (long) value);
        }

    }

    /**
     * Same as above, but two maps, not advancing at the same pace
     * @throws Exception
     */
    @Test
    public void testStress2() throws Exception {

        String mapName1 = "testMapA";
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);
        String mapName2 = "testMapB";
        Map<Long, Long> testMap2 = instantiateCorfuObject(SMRMap.class, mapName2);


        final int VAL = 1;

        // You can fine tune the below parameters. OBJECT_NUM has to be a multiple of THREAD_NUM.
        final int OBJECT_NUM = 20;
        final int THREAD_NUM = 5;

        final int FINAL_SUM1 = OBJECT_NUM;
        final int FINAL_SUM2 = OBJECT_NUM/2+1;
        final int STEP = OBJECT_NUM / THREAD_NUM;

        // test all objects advance in lock-step to FINAL_SUM
        for (int i = 0; i < FINAL_SUM1; i++) {

            for (int j = 0; j < OBJECT_NUM; j += STEP) {
                NonOverlappingWriter n = new NonOverlappingWriter(i + 1, j, j + STEP, VAL);
                scheduleConcurrently(t -> { n.dowork2();});
            }
            executeScheduled(THREAD_NUM, PARAMETERS.TIMEOUT_NORMAL);

        }

        Assert.assertEquals(testMap2.size(), FINAL_SUM1);
        for (long i = 0; i < OBJECT_NUM; i++) {
            log.debug("final testmap1.get({}) = {}", i, testMap1.get(i));
            log.debug("final testmap2.get({}) = {}", i, testMap2.get(i));
            if (i % 2 == 0)
                Assert.assertEquals((long)testMap2.get(i), (long) FINAL_SUM2);
            else
                Assert.assertEquals((long)testMap2.get(i), (long) FINAL_SUM1);
        }

    }


    public class NonOverlappingWriter {

        String mapName1 = "testMapA";
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);
        String mapName2 = "testMapB";
        Map<Long, Long> testMap2 = instantiateCorfuObject(SMRMap.class, mapName2);


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
        public void dowork() {
            for (int i = start; i < end; i++) {
                simpleCreateImpl(i, val);
            }
        }

        /**
         * Updates objects between index start and index end.
         */
        public void dowork2() {
            for (int i = start; i < end; i++) {
                duoCreateImpl(i, val);
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

            if (!testMap1.containsKey(idx)) {
                if (expectedSum - 1 > 0)
                    log.debug("OBJ FAIL {} doesn't exist expected={}",
                            idx, expectedSum);
                log.debug("OBJ {} PUT {}", idx, val);
                testMap1.put(idx, val);
            } else {
                log.debug("OBJ {} GET", idx);
                Long value = testMap1.get(idx);
                if (value != (expectedSum - 1))
                    log.debug("OBJ FAIL {} value={} expected={}", idx, value,
                            expectedSum - 1);
                log.debug("OBJ {} PUT {}+{}", idx, value, val);
                testMap1.put(idx, value + val);
            }

            TXEnd();
        }

        /**
         * Does the actual work.
         *
         * @param idx SMRMap key index to update
         * @param val how much to increment the value by.
         */
        private void duoCreateImpl(long idx, long val) {

            TXBegin();

            if (!testMap1.containsKey(idx)) {
                if (expectedSum - 1 > 0)
                    log.debug("OBJ FAIL {} doesn't exist expected={}",
                            idx, expectedSum);
                log.debug("OBJ {} PUT {}", idx, val);
                testMap1.put(idx, val);
                testMap2.put(idx, val);
            } else {
                log.debug("OBJ {} GET", idx);
                Long value = testMap1.get(idx);
                if (value != (expectedSum - 1))
                    log.debug("OBJ FAIL {} value={} expected={}", idx, value,
                            expectedSum - 1);
                log.debug("OBJ {} PUT {}+{}", idx, value, val);
                testMap1.put(idx, value + val);

                // in map 2, on even rounds, do this on for every other entry
                log.debug("OBJ2 {} GET", idx);
                Long value2 = testMap2.get(idx);
                if (idx % 2 == 0) {
                    if (value2 != (expectedSum/2))
                        log.debug("OBJ2 FAIL {} value={} expected={}", idx, value2,
                                expectedSum/2);
                    if (expectedSum % 2 == 0) {
                        log.debug("OBJ2 {} PUT {}+{}", idx, value2, val);
                        testMap2.put(idx, value2 + val);
                    }
                } else {
                    if (value2 != (expectedSum - 1))
                        log.debug("OBJ2 FAIL {} value={} expected={}", idx, value2,
                                expectedSum - 1);
                    log.debug("OBJ2 {} PUT {}+{}", idx, value2, val);
                    testMap2.put(idx, value2 + val);
                }
            }

            TXEnd();
        }
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

