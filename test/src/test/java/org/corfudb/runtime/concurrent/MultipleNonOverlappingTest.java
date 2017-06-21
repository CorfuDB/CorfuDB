package org.corfudb.runtime.concurrent;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.corfudb.util.CoopScheduler;
import org.corfudb.util.CoopUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.util.CoopScheduler.sched;

@Slf4j
public class MultipleNonOverlappingTest extends AbstractTransactionsTest {
    private String scheduleString;

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
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            testStress(i);
        }
    }

    private void testStress(int iter) throws Exception {
        String mapName = "testMapA" + iter;
        Map<Long, Long> testMap = instantiateCorfuObject(SMRMap.class, mapName);

        final int VAL = 1;

        // You can fine tune the below parameters. OBJECT_NUM has to be a multiple of THREAD_NUM.
        final int OBJECT_NUM = 20;
        final int THREAD_NUM = 5;

        final int FINAL_SUM = OBJECT_NUM;
        final int STEP = OBJECT_NUM / THREAD_NUM;

        final int nThreads = OBJECT_NUM;
        final int schedLength = 300;
        int[] schedule = CoopScheduler.makeSchedule(nThreads, schedLength);
        scheduleString = "Schedule is: " + CoopScheduler.formatSchedule(schedule);

        // test all objects advance in lock-step to FINAL_SUM
        for (int i = 0; i < FINAL_SUM; i++) {
            CoopUtil m = new CoopUtil();
            CoopScheduler.reset(nThreads);
            CoopScheduler.setSchedule(schedule);

            for (int j = 0; j < OBJECT_NUM; j += STEP) {
                NonOverlappingWriter n = new NonOverlappingWriter(iter, i + 1, j, j + STEP, VAL);
                m.scheduleCoopConcurrently((thr, t) -> { n.dowork();});
            }
            m.executeScheduled();
        }

        assertThat(testMap.size())
                .describedAs(scheduleString)
                .isEqualTo(FINAL_SUM);
        for (Long value : testMap.values()) {
            assertThat(value)
                    .describedAs(scheduleString)
                    .isEqualTo(FINAL_SUM);
        }
    }

    /**
     * Same as above, but two maps, not advancing at the same pace
     * @throws Exception
     */
    @Test
    public void testStress2() throws Exception {
        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            testStress2(i);
        }
    }

    public void testStress2(int iter) throws Exception {
        String mapName1 = "testMapA" + iter;
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);
        String mapName2 = "testMapB" + iter;
        Map<Long, Long> testMap2 = instantiateCorfuObject(SMRMap.class, mapName2);


        final int VAL = 1;

        // You can fine tune the below parameters. OBJECT_NUM has to be a multiple of THREAD_NUM.
        final int OBJECT_NUM = 20;
        final int THREAD_NUM = 5;

        final int FINAL_SUM1 = OBJECT_NUM;
        final int FINAL_SUM2 = OBJECT_NUM/2+1;
        final int STEP = OBJECT_NUM / THREAD_NUM;

        final int nThreads = OBJECT_NUM;
        final int schedLength = 300;
        int[] schedule = CoopScheduler.makeSchedule(nThreads, schedLength);
        scheduleString = "Schedule is: " + CoopScheduler.formatSchedule(schedule);

        // test all objects advance in lock-step to FINAL_SUM
        for (int i = 0; i < FINAL_SUM1; i++) {
            CoopUtil m = new CoopUtil();
            CoopScheduler.reset(nThreads);
            CoopScheduler.setSchedule(schedule);

            for (int j = 0; j < OBJECT_NUM; j += STEP) {
                NonOverlappingWriter n = new NonOverlappingWriter(iter, i + 1, j, j + STEP, VAL);
                m.scheduleCoopConcurrently((thr, t) -> { n.dowork2();});
            }
            m.executeScheduled();
        }

        assertThat(testMap2.size())
                .describedAs(scheduleString)
                .isEqualTo(FINAL_SUM1);
        for (long i = 0; i < OBJECT_NUM; i++) {
            log.debug("final testmap1.get({}) = {}", i, testMap1.get(i));
            log.debug("final testmap2.get({}) = {}", i, testMap2.get(i));
            if (i % 2 == 0) {
                assertThat(testMap2.get(i))
                        .describedAs(scheduleString)
                        .isEqualTo(FINAL_SUM2);
            } else {
                assertThat(testMap2.get(i))
                        .describedAs(scheduleString)
                        .isEqualTo(FINAL_SUM1);
            }
        }
    }


    public class NonOverlappingWriter {

        String mapName1;
        Map<Long, Long> testMap1;
        String mapName2;
        Map<Long, Long> testMap2;


        int start;
        int end;
        int val;
        int expectedSum;

        public NonOverlappingWriter(int iter, int expectedSum, int start, int end, int val) {
            mapName1 = "testMapA" + iter;
            testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);
            mapName2 = "testMapB" + iter;
            testMap2 = instantiateCorfuObject(SMRMap.class, mapName2);
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
            sched();

            if (!testMap1.containsKey(idx)) {
                if (expectedSum - 1 > 0)
                    log.debug("OBJ FAIL {} doesn't exist expected={}",
                            idx, expectedSum);
                log.debug("OBJ {} PUT {}", idx, val);
                sched();
                testMap1.put(idx, val);
            } else {
                log.debug("OBJ {} GET", idx);
                sched();
                Long value = testMap1.get(idx);
                if (value != (expectedSum - 1))
                    log.debug("OBJ FAIL {} value={} expected={}", idx, value,
                            expectedSum - 1);
                log.debug("OBJ {} PUT {}+{}", idx, value, val);
                sched();
                testMap1.put(idx, value + val);
            }
            sched();
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
            sched();
            if (!testMap1.containsKey(idx)) {
                if (expectedSum - 1 > 0)
                    log.debug("OBJ FAIL {} doesn't exist expected={}",
                            idx, expectedSum);
                log.debug("OBJ {} PUT {}", idx, val);
                sched();
                testMap1.put(idx, val);
                sched();
                testMap2.put(idx, val);
            } else {
                log.debug("OBJ {} GET", idx);
                sched();
                Long value = testMap1.get(idx);
                if (value != (expectedSum - 1))
                    log.debug("OBJ FAIL {} value={} expected={}", idx, value,
                            expectedSum - 1);
                log.debug("OBJ {} PUT {}+{}", idx, value, val);
                sched();
                testMap1.put(idx, value + val);

                // in map 2, on even rounds, do this on for every other entry
                log.debug("OBJ2 {} GET", idx);
                sched();
                Long value2 = testMap2.get(idx);
                if (idx % 2 == 0) {
                    if (value2 != (expectedSum/2))
                        log.debug("OBJ2 FAIL {} value={} expected={}", idx, value2,
                                expectedSum/2);
                    if (expectedSum % 2 == 0) {
                        log.debug("OBJ2 {} PUT {}+{}", idx, value2, val);
                        sched();
                        testMap2.put(idx, value2 + val);
                    }
                } else {
                    if (value2 != (expectedSum - 1))
                        log.debug("OBJ2 FAIL {} value={} expected={}", idx, value2,
                                expectedSum - 1);
                    log.debug("OBJ2 {} PUT {}+{}", idx, value2, val);
                    sched();
                    testMap2.put(idx, value2 + val);
                }
            }
            sched();
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

