package org.corfudb.runtime.concurrent;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.AbstractTransactionsTest;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by dalia on 3/18/17.
 */
@Slf4j
public class MapsAsMQsTest extends AbstractTransactionsTest {
    @Override
    public void TXBegin() { OptimisticTXBegin(); }


    protected int numIterations = PARAMETERS.NUM_ITERATIONS_LOW;

    /**
     * This test verifies commit atomicity against concurrent -read- activity,
     * which constantly causes rollbacks and optimistic-rollbacks.
     *
     * @throws Exception
     */
    @Test
    public void useMapsAsMQs() throws Exception {
        String mapName1 = "testMapA";
        Map<Long, Long> testMap1 = instantiateCorfuObject(SMRMap.class, mapName1);

        final int nThreads = 4;
        CountDownLatch barrier = new CountDownLatch(nThreads-1);
        Semaphore s1 = new Semaphore(0);
        Semaphore s2 = new Semaphore(0);


        // 1st thread: producer of new "trigger" values
        scheduleConcurrently(t -> {

            // wait for other threads to start
            barrier.await();
            log.debug("all started");

            for (int i = 0; i < numIterations; i++) {
                // place a value in the map
                log.debug("- sending 1st trigger " + i);
                testMap1.put(1L, (long) i);

                // await for the consumer condition to circulate back
                s2.acquire();

                log.debug("- s2.await() finished at " + i);
            }
        });

        // 2nd thread: monitor map and wait for "trigger" values to show up, produce 1st signal
        scheduleConcurrently(t -> {

            // signal start
            barrier.countDown();

            int busyDelay = 1; // millisecs

            for (int i = 0; i < numIterations; i++) {
                while (testMap1.get(1L) == null || testMap1.get(1L) != (long) i) {
                    log.debug( "- wait for 1st trigger " + i);
                    Thread.sleep(busyDelay);
                }
                log.debug( "- received 1st trigger " + i);

                // 1st producer signal through lock
                // 1st producer signal
                log.debug( "- sending 1st signal " + i);
                s1.release();
            }
        });

        // 3rd thread: monitor 1st producer condition and produce a second "trigger"
        scheduleConcurrently(t -> {

            // signal start
            barrier.countDown();

            for (int i = 0; i < numIterations; i++) {
                TXBegin();
                log.debug( "- received 1st condition POST-lock PRE-await" + i);

                // wait for 1st producer signal
                s1.acquire();
                log.debug( "- received 1st condition " + i);

                // produce another tigger value
                log.debug( "- sending 2nd trigger " + i);
                testMap1.put(2L, (long) i);
                TXEnd();
            }
        });

        // 4th thread: monitor map and wait for 2nd "trigger" values to show up, produce second signal
        scheduleConcurrently(t -> {

            // signal start
            barrier.countDown();

            int busyDelay = 1; // millisecs

            for (int i = 0; i < numIterations; i++) {
                while (testMap1.get(2L) == null || testMap1.get(2L) != (long) i)
                    Thread.sleep(busyDelay);
                log.debug( "- received 2nd trigger " + i);

                // 2nd producer signal through lock
                // 2nd producer signal
                log.debug( "- sending 2nd signal " + i);
                s2.release();
            }
        });

        executeScheduled(nThreads, PARAMETERS.TIMEOUT_LONG);
    }
}
