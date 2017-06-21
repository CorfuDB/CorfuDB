package org.corfudb.runtime.checkpoint;

import lombok.extern.slf4j.Slf4j;
import com.google.common.reflect.TypeToken;
import lombok.Getter;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.exceptions.TrimmedUpcallException;
import org.corfudb.runtime.object.AbstractObjectTest;
import org.corfudb.util.CoopScheduler;
import org.corfudb.util.serializer.ISerializer;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.util.CoopScheduler.sched;

/**
 * Version of CheckpointTest with main() function for testing
 * via AspectJ and the CoopScheduler.
 */
@Slf4j
public class CheckpointCoopTest extends AbstractObjectTest {

    @Getter
    CorfuRuntime myRuntime = null;

    public static void main(String[] argv) {
        try {
            CheckpointCoopTest t = new CheckpointCoopTest();
            t.periodicCkpointTrimTest_lots(argv.length > 0 && argv[0].contentEquals("fixed"));
            System.exit(0);
        } catch (Exception e) {
            System.err.printf("ERROR: Caught exception %s at:\n", e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    void setRuntime() {
        myRuntime = new CorfuRuntime(getDefaultConfigurationString()).connect();
        // Deadlock prevention: Java 'synchronized' is used to lock the CorfuRuntime's
        // address space cache's ConcurrentHashMap.  From inside a 'synchronized' block,
        // computeIfAbsent can trigger a Corfu read which can be CoopSched + yield'ed,
        // causing deadlock.  Our workaround is to disable that cache.
        getMyRuntime().setCacheDisabled(true);
    }

    Map<String, Long> instantiateMap(String mapName) {
        log.info("I={}@{}", mapName, Thread.currentThread().getName());
        final byte serializerByte = (byte) 20;
        ISerializer serializer = new CPSerializer(serializerByte);
        Serializers.registerSerializer(serializer);
        return (SMRMap<String, Long>)
                instantiateCorfuObject(
                        getMyRuntime(),
                        new TypeToken<SMRMap<String, Long>>() {},
                        mapName,
                        serializer);
    }

    final String streamNameA = "mystreamA";
    final String streamNameB = "mystreamB";
    final String author = "ckpointTest";
    protected Map<String, Long> m2A;
    protected Map<String, Long> m2B;
    String scheduleString;

    private void myOpTrace(String fmt, Object... args) {
        String s = String.format(fmt, args);
        //// System.err.printf(s + ",");
        log.trace(s);
        CoopScheduler.appendLog(s);
    }

    /**
     * common initialization for tests: establish Corfu runtime and instantiate two maps
     */
    public void instantiateMaps() {
        if (getMyRuntime() == null) { setRuntime(); }

        m2A = instantiateMap(streamNameA);
        m2B = instantiateMap(streamNameB);
    }

    /**
     * checkpoint the maps, and then trim the log
     */
    void mapCkpointAndTrim() throws Exception {
        CorfuRuntime currentRuntime = getMyRuntime();
        getMyRuntime().setCacheDisabled(true);
        final int spin = 20;

        for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_VERY_LOW; i++) {
            try {
                MultiCheckpointWriter mcw1 = new MultiCheckpointWriter();
                mcw1.addMap((SMRMap) m2A);
                mcw1.addMap((SMRMap) m2B);
                for (int j = 0; j < spin; j++) {
                    sched();
                }
                long checkpointAddress = mcw1.appendCheckpoints(currentRuntime, author);
                myOpTrace("cp-%d", i);
                // Trim the log
                for (int j = 0; j < spin; j++) {
                    sched();
                }
                currentRuntime.getAddressSpaceView().prefixTrim(checkpointAddress - 1);
                myOpTrace("trim-%d", i);
                for (int j = 0; j < spin; j++) {
                    sched();
                }
                currentRuntime.getAddressSpaceView().gc();
                myOpTrace("gc-%d", i);
                for (int j = 0; j < spin; j++) {
                    sched();
                }
                currentRuntime.getAddressSpaceView().invalidateServerCaches();
                for (int j = 0; j < spin; j++) {
                    sched();
                }
                currentRuntime.getAddressSpaceView().invalidateClientCache();
            } catch (TrimmedException te) {
                // shouldn't happen
                te.printStackTrace();
                throw te;
            }

        }
    }

    /**
     *  Start a fresh runtime and instantiate the maps.
     * This time the we check that the new map instances contains all values
     * @param mapSize
     * @param expectedFullsize
     */
    void validateMapRebuild(int mapSize, boolean expectedFullsize) {
        setRuntime();
        getMyRuntime().setCacheDisabled(true);
        try {
            Map<String, Long> localm2A = instantiateMap(streamNameA);
            Map<String, Long> localm2B = instantiateMap(streamNameB);
            int max;
            max = localm2A.size();
            myOpTrace("%s@A%d", Thread.currentThread().getName(), max);
            log.info("{}@A{}", Thread.currentThread().getName(), max);
            for (int i = 0; i < max; i++) {
                sched();
                assertThat(localm2A.get(String.valueOf(i)))
                        .describedAs(scheduleString)
                        .isEqualTo((long) i);
            }
            max = localm2B.size();
            myOpTrace("%s@B%d", Thread.currentThread().getName(), max);
            log.info("{}@B{}", Thread.currentThread().getName(), max);
            for (int i = 0; i < max; i++) {
                sched();
                assertThat(localm2B.get(String.valueOf(i)))
                        .describedAs(scheduleString)
                        .isEqualTo(0L);
            }
            if (expectedFullsize) {
                sched();
                assertThat(localm2A.size())
                        .describedAs(scheduleString)
                        .isEqualTo(mapSize);
                sched();
                assertThat(localm2B.size())
                        .describedAs(scheduleString)
                        .isEqualTo(mapSize);
            }
        } catch (TrimmedException te) {
            // shouldn't happen
            te.printStackTrace();
            throw te;
        }
    }

    /**
     * initialize the two maps, the second one is all zeros
     * @param mapSize
     */
    void populateMaps(int mapSize) {
        for (int i = 0; i < mapSize; i++) {
            // If TrimmedUpcallException happens, we need to retry the put() calls,
            // because other parts of this test assume that the put() calls are
            // eventually successful.
            while (true) {
                try {
                    sched();
                    m2A.put(String.valueOf(i), (long) i);
                    myOpTrace("pA-%d", i);
                    sched();
                    m2B.put(String.valueOf(i), (long) 0);
                    myOpTrace("pB-%d", i);
                    break;
                } catch (TrimmedUpcallException te) {
                    // NOTE: This is a possible exception nowadays.
                    System.err.printf("\nSchedule: %s\nNOTICE/TODO: %s\n", scheduleString, te);
                    // Let's keep going.... throw te;
                } catch (TrimmedException te) {
                    // shouldn't happen
                    te.printStackTrace();
                    throw te;
                }
            }
        }
    }

    /**
     * this test is similar to periodicCkpointTest(), but adds simultaneous log prefix-trimming.
     * <p>
     * the test builds two maps, m2A m2B, and brings up three threads:
     * <p>
     * 1. one pupolates the maps with mapSize items
     * 2. one does a periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times,
     * and immediately trims the log up to the checkpoint position.
     * 3. one repeats ITERATIONS_LOW starting a fresh runtime, and instantiating the maps.
     * they should rebuild from the latest checkpoint (if available).
     * this thread performs some sanity checks on the map state
     * <p>
     * Finally, after all three threads finish, again we start a fresh runtime and instante the maps.
     * This time the we check that the new map instances contains all values
     *
     * @throws Exception
     */

    public void periodicCkpointTrimTest_lots(boolean fixedSchedule) throws Exception {
        final int T0 = 0, T1 = 1, T2 = 2, T3 = 3, T4 = 4, T5 = 5, T6 = 6;
        int numThreads = T6+1;
        final int onehundred = 100;
        ArrayList<Object[]> logs = new ArrayList<>();
        final int numTests = 20;

        for (int i = 0; i < numTests; i++) {
            //// System.err.printf("Iter %d, thread count = %d\n", i, Thread.getAllStackTraces().size());
            System.err.printf(".");

            // @After methods:
            cleanupBuffers();
            try { cleanupScheduledThreads(); } catch (Exception e) {};
            shutdownThreadingTest();
            cleanPerTestTempDir();

            // @Before methods:
            setupScheduledThreads();
            clearTestStatus();
            resetThreadingTest();
            InitSM();
            resetTests();
            addSingleServer(SERVERS.PORT_0);
            setRuntime();
            instantiateMaps();

            int[] schedule;
            if (fixedSchedule) {
                schedule = new int[]{T1, T1, T0, T2, T1, T1, T1, T0, T4, T3, T4, T3, T3, T3, T6, T5};
                // The schedule below will yield this error (for debugging, etc.): TrimmedUpcallException: Attempted to get upcall result @25 ......
                // int[] schedule = new int[]{1,4,1,1,3,2,4,3,5,0,2,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,4,1,1,3,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,2,0,6,1,5,5,5,5,5,0,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1};
            } else {
                schedule = CoopScheduler.makeSchedule(numThreads, onehundred);
            }
            scheduleString = "Schedule is: " + CoopScheduler.formatSchedule(schedule);
            // System.err.printf(scheduleString + "\n");

            periodicCkpointTrimTestInner(schedule, numThreads);
            logs.add(CoopScheduler.getLog());

            // printLog(logs.get(i));
        }
        if (fixedSchedule) {
            if (CoopScheduler.logsAreIdentical(logs)) {
                System.err.printf("\nIDENTICAL = true\n");
            } else {
                throw new Exception("logsAreIdentical= false");
            }
        }
    }

    public void periodicCkpointTrimTestInner(int[] schedule, int numThreads) throws Exception {
        final int mapSize = PARAMETERS.NUM_ITERATIONS_LOW;
        Thread ts[] = new Thread[numThreads];
        int idxTs = 0;
        AtomicBoolean workerThreadFailure = new AtomicBoolean(false);

        instantiateMaps();

        CoopScheduler.reset(numThreads);
        CoopScheduler.setSchedule(schedule);
        assertThat(PARAMETERS.CONCURRENCY_SOME + 2).isEqualTo(numThreads);

        // thread 1: populates the maps with mapSize items
        ts[idxTs++] = new Thread(() -> {
                Thread.currentThread().setName("thr-0");
                CoopScheduler.registerThread(0); sched();
                try {
                    populateMaps(mapSize);
                } catch (Exception e) {
                    workerThreadFailure.set(true);
                }
                CoopScheduler.threadDone();
        });

        // thread 2: periodic checkpoint of the maps, repeating ITERATIONS_VERY_LOW times,
        // and immediate prefix-trim of the log up to the checkpoint position
        ts[idxTs++] = new Thread(() -> {
            Thread.currentThread().setName("thr-1");
            CoopScheduler.registerThread(1); sched();
            try {
                mapCkpointAndTrim();
            } catch (TransactionAbortedException e) {
                System.err.printf(",Hey, check this schedule!,");
                // Abort is possible, but no need to fail test if it does.
            } catch (Exception e) {
                workerThreadFailure.set(true);
            }
            CoopScheduler.threadDone();
        });

        // thread 3: repeated ITERATION_LOW times starting a fresh runtime, and instantiating the maps.
        // they should rebuild from the latest checkpoint (if available).
        // performs some sanity checks on the map state
        for (int i = 0; i < PARAMETERS.CONCURRENCY_SOME; i++) {
            final int ii = i;
            ts[idxTs++] = new Thread(() -> {
                Thread.currentThread().setName("thr-" + (ii + 2));
                CoopScheduler.registerThread(ii+2); sched();
                for (int j = 0; j < 2*2*2; j++) {
                    try {
                        validateMapRebuild(mapSize, false);
                    } catch (Exception e) {
                        workerThreadFailure.set(true);
                    }
                }
                CoopScheduler.threadDone();
            });
        }
        for (int i = 0; i < ts.length; i++) { ts[i].start(); }
        CoopScheduler.runScheduler(ts.length);
        for (int i = 0; i < ts.length; i++) { ts[i].join(); }

        assertThat(workerThreadFailure.get())
                .describedAs(scheduleString)
                .isFalse();

        // finally, after all three threads finish, again we start a fresh runtime and instante the maps.
        // This time the we check that the new map instances contains all values
        log.info("FINAL CHECK");
        validateMapRebuild(mapSize, true);
    }

    private void printLog(Object[] log) {
        for (int i = 0; i < log.length; i++) {
            System.err.printf("%s,", log[i]);
        }
        System.err.printf("\n");
    }
}

