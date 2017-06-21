package org.corfudb.runtime.object;

import java.util.ArrayList;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.util.CoopScheduler;
import org.corfudb.util.CoopUtil;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.util.CoopScheduler.sched;

/**
 * Version of CorfuSMRObjectConcurrencyTest with main() function for testing
 * via AspectJ and the CoopScheduler.
 */
public class CorfuSmrObjectConcurrencyCoopTest extends AbstractObjectTest {
    int concurrency = PARAMETERS.CONCURRENCY_SOME * 2;
    int writeconcurrency = PARAMETERS.CONCURRENCY_SOME;
    int writerwork = PARAMETERS.NUM_ITERATIONS_LOW;
    String scheduleString;
    final int scheduleLength = 500;

    public static void main(String[] argv) {
        try {
            new CorfuSmrObjectConcurrencyCoopTest()
                .testCorfuSharedCounterConcurrentReads_lots();
            System.exit(0);
        } catch (Exception e) {
            System.err.printf("ERROR: Caught exception %s at:\n", e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void testCorfuSharedCounterConcurrentReads_lots() throws Exception {
        ArrayList<Object[]> logs = new ArrayList<>();
        final int NUM_TESTS = 20;

        for (int i = 0; i < NUM_TESTS; i++) {
            // System.err.printf("Iter %d, thread count = %d\n", i, Thread.getAllStackTraces().size());
            System.err.printf(".");

            junitAfterMethodCalls();
            junitBeforeMethodCalls();
            testCorfuSharedCounterConcurrentReads();
            logs.add(CoopScheduler.getLog());

            // printLog(logs.get(i));
        }
    }

    @Test
    public void testCorfuSharedCounterConcurrentReads() throws Exception {
        getDefaultRuntime();
        setupCoopScheduler();

        final int COUNTER_INITIAL = 55;

        CorfuSharedCounter sharedCounter = (CorfuSharedCounter)
                instantiateCorfuObject(CorfuSharedCounter.class, "test");

        sharedCounter.setValue(COUNTER_INITIAL);

        sharedCounter.setValue(-1);
        assertThat(sharedCounter.getValue())
                .isEqualTo(-1);

        CoopUtil util = new CoopUtil();

        util.scheduleCoopConcurrently(writeconcurrency, (thr, t) -> {
            for (int i = 0; i < 2*2*2; i++) {
                sched();
            }
            for (int i = 0; i < writerwork; i++) {
                        sched();
                        int val = (int) t * writerwork + i;
                        CoopScheduler.appendLog(val);
                        sharedCounter.setValue(val);
                    }
                }
        );
        util.scheduleCoopConcurrently(concurrency-writeconcurrency, (thr, t) -> {
                    int lastread = -1;
                    for (int i = 0; i < PARAMETERS.NUM_ITERATIONS_LOW; i++) {
                        sched();
                        int res = sharedCounter.getValue();
                        CoopScheduler.appendLog("g" + thr);
                        boolean assertflag =
                                (
                                        ( ((lastread < writerwork && res < writerwork) || (lastread >= writerwork && res >= writerwork) ) && lastread <= res ) ||
                                                ( (lastread < writerwork && res >= writerwork) || (lastread >= writerwork && res < writerwork) )
                                );
                        assertThat(assertflag)
                                .describedAs(scheduleString)
                                .isTrue();
                    }
                }
        );
        util.executeScheduled();
    }

    private void setupCoopScheduler() {
        int[] schedule = CoopScheduler.makeSchedule(concurrency, scheduleLength);
        // int[] schedule = new int[]{0,1,2,3,4,5,6,7,8,9};
        scheduleString = "Schedule is: " + CoopScheduler.formatSchedule(schedule);
        CoopScheduler.reset(concurrency);
        CoopScheduler.setSchedule(schedule);
    }

    void setRuntime() {
        setRuntime(new CorfuRuntime(getDefaultConfigurationString()).connect());
        getRuntime().setCacheDisabled(true);
    }

    private void junitAfterMethodCalls() {
        // @After methods:
        cleanupBuffers();
        try {
            cleanupScheduledThreads();
        } catch (Exception e) {
        }
        try {
            shutdownThreadingTest();
        } catch (Exception e) {
        }
        cleanPerTestTempDir();
    }

    private void junitBeforeMethodCalls() {
        // @Before methods:
        setupScheduledThreads();
        clearTestStatus();
        resetThreadingTest();
        InitSM();
        resetTests();
        addSingleServer(SERVERS.PORT_0);
        setRuntime();
    }

    private void printLog(Object[] log) {
        for (int i = 0; i < log.length; i++) {
            System.err.printf("%s,", log[i]);
        }
        System.err.printf("\n");
    }
}
