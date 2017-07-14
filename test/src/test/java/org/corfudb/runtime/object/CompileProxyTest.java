package org.corfudb.runtime.object;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 11/11/16.
 */
public class CompileProxyTest extends AbstractViewTest {
    
    @Test
    public void testObjectMapSimple() throws Exception {

        Map<String, String> map = getDefaultRuntime()
                                    .getObjectsView().build()
                                    .setStreamName("my stream")
                                    .setTypeToken(new TypeToken<SMRMap<String,String>>() {})
                                    .open();

        getDefaultRuntime().getObjectsView().TXBegin();
        map.put("hello", "world");
        map.put("hell", "world");
        getDefaultRuntime().getObjectsView().TXEnd();

        assertThat(map)
                .containsEntry("hello", "world");
        assertThat(map)
                .containsEntry("hell", "world");
    }

    @Test
    public void testObjectCounterSimple() throws Exception {
        CorfuSharedCounter sharedCounter = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setTypeToken(new TypeToken<CorfuSharedCounter>() {
                })
                .open();

        final int VALUE = 33;
        sharedCounter.setValue(VALUE);
        assertThat(sharedCounter.getValue())
                .isEqualTo(VALUE);
    }

    /**
     * test concurrent writes to a shared counter.
     * set up 'concurrency' threads that concurrently try to set the counter to their thread-index.
     * the test tracks raw stream updates, one by one, guaranteeing that all the updates appended to the stream differ from each other.
     *
     * @throws Exception
     */
    @Test
    public void testObjectCounterWriteConcurrency() throws Exception {
        CorfuSharedCounter sharedCounter = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setTypeToken(new TypeToken<CorfuSharedCounter>() {
                })
                .open();
        final int concurrency = PARAMETERS.CONCURRENCY_LOTS;
        final int INITIAL = -1;

        sharedCounter.setValue(INITIAL);
        assertThat(sharedCounter.getValue())
                .isEqualTo(INITIAL);

        // schedule 'concurrency' number of threads,
        // each one sets the shared counter value to its thread index
        scheduleConcurrently(concurrency, t ->
                sharedCounter.setValue(t)
        );
        executeScheduled(concurrency, PARAMETERS.TIMEOUT_NORMAL);

        // track the raw stream updates caused by the execution so far
        ICorfuSMR<CorfuSharedCounter> compiledSharedCounter = (ICorfuSMR<CorfuSharedCounter>) sharedCounter;
        ICorfuSMRProxyInternal<CorfuSharedCounter> proxy_CORFUSMR = (ICorfuSMRProxyInternal<CorfuSharedCounter>) compiledSharedCounter.getCorfuSMRProxy();
        //IStreamView objStream = proxy_CORFUSMR.getUnderlyingObject().getStreamViewUnsafe();

        int beforeSync, afterSync;

        // before sync'ing the in-memory object, the in-memory copy does not get updated
        assertThat(beforeSync = proxy_CORFUSMR.getUnderlyingObject().object.getValue())
                .isEqualTo(INITIAL);

        // sync with the stream entry by entry
        for (int timestamp = 1; timestamp <= concurrency; timestamp++) {
            proxy_CORFUSMR.getUnderlyingObject()
                    .syncObjectUnsafe(timestamp);
            assertThat((afterSync = proxy_CORFUSMR.getUnderlyingObject().object.getValue()))
                    .isBetween(0, concurrency);
            assertThat(beforeSync)
                    .isNotEqualTo(afterSync);
            beforeSync = afterSync;
        }

        // now we get the LATEST value through the Corfu object API
        assertThat((afterSync = sharedCounter.getValue()))
                .isBetween(0, concurrency);
        assertThat(beforeSync)
                .isEqualTo(afterSync);
    }

    /**
     * test serializability guarantee of mutatorAccessor methods.
     * The test invokes CorfuSharedCounter::CAS by 'concurrency' number of concurrent threads.
     * @throws Exception
     */
    @Test
    public void testObjectCounterCASConcurrency() throws Exception {
    CorfuSharedCounter sharedCounter = getDefaultRuntime()
            .getObjectsView().build()
            .setStreamName("my stream")
            .setTypeToken(new TypeToken<CorfuSharedCounter>() {
            })
            .open();
        int concurrency = PARAMETERS.CONCURRENCY_LOTS;
        final int INITIAL = -1;

        sharedCounter.setValue(INITIAL);

        // concurrency invoke CAS by multiple threads
        AtomicInteger casSucceeded = new AtomicInteger(0);
        scheduleConcurrently(concurrency, t -> {
                    if (sharedCounter.CAS(INITIAL, t) == INITIAL)
                        casSucceeded.incrementAndGet();
        });
        executeScheduled(concurrency, PARAMETERS.TIMEOUT_SHORT);

        // check that exactly one CAS succeeded
        assertThat(sharedCounter.getValue())
                .isBetween(0, concurrency);
        assertThat(casSucceeded.get())
                .isEqualTo(1);
    }

    /**
     * the test interleaves reads and writes to a shared counter.
     *
     * The test uses the interleaving engine to interleave numTasks*threads state machines.
     * A simple state-machine is built. It randomly chooses to either read or append the counter.
     * On a read, the last written value is expected.
     *
     * @throws Exception
     */
    @Test
    public void testObjectCounterReadConcurrency() throws Exception {
        CorfuSharedCounter sharedCounter = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setTypeToken(new TypeToken<CorfuSharedCounter>() {
                })
                .open();

        int numTasks = PARAMETERS.NUM_ITERATIONS_LOW;
        Random r = new Random(PARAMETERS.SEED);

        sharedCounter.setValue(-1);
        AtomicInteger lastUpdate = new AtomicInteger(-1);

        assertThat(sharedCounter.getValue())
                .isEqualTo(lastUpdate.get());

        // only one step: randomly choose between read/append of the shared counter
        addTestStep ((task_num) -> {
            if (r.nextBoolean()) {
                sharedCounter.setValue(task_num);
                lastUpdate.set(task_num); // remember the last written value
            } else {
                assertThat(sharedCounter.getValue()).isEqualTo(lastUpdate.get()); // expect to read the value in lastUpdate
            }
        } );

        // invoke the interleaving engine
        scheduleInterleaved(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.CONCURRENCY_SOME*numTasks);
    }

    /**
     * the test is similar to the interleaved read/append above to a shared counter.
     * in addition to checking read/append values, it tracks the raw stream state between updates and syncs.
     *
     * The test uses the interleaving engine to interleave numTasks*threads state machines.
     * A simple state-machine is built. It randomly chooses to either read or append the counter.
     * On a read, the last written value is expected.
     *
     * @throws Exception
     */

    @Test
    public void testObjectCounterConcurrencyStream() throws Exception {
        CorfuSharedCounter sharedCounter = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setTypeToken(new TypeToken<CorfuSharedCounter>() {
                })
                .open();

        ICorfuSMR<CorfuSharedCounter> compiledSharedCounter = (ICorfuSMR<CorfuSharedCounter>)  sharedCounter;
        ICorfuSMRProxyInternal<CorfuSharedCounter> proxy_CORFUSMR = (ICorfuSMRProxyInternal<CorfuSharedCounter>) compiledSharedCounter.getCorfuSMRProxy();
      //  IStreamView objStream = proxy_CORFUSMR.getUnderlyingObject().getStreamViewUnsafe();

        int numTasks = PARAMETERS.NUM_ITERATIONS_LOW;
        Random r = new Random(PARAMETERS.SEED);

        final int INITIAL = -1;

        sharedCounter.setValue(INITIAL);
        AtomicInteger lastUpdate = new AtomicInteger(INITIAL);
        AtomicLong lastUpdateStreamPosition = new AtomicLong(0);

        assertThat(sharedCounter.getValue())
                .isEqualTo(lastUpdate.get());

        AtomicInteger lastRead = new AtomicInteger(INITIAL);

        // build a state-machine:

        // only one step: randomly choose between read/append of the shared counter
        addTestStep((task_num) -> {

            if (r.nextBoolean()) {
                sharedCounter.setValue(task_num);
                lastUpdate.set(task_num); // remember the last written value
                lastUpdateStreamPosition.incrementAndGet(); // advance the expected stream position

            } else {
                // before sync'ing the in-memory object, the in-memory copy does not get updated
                // check that the in-memory copy is only as up-to-date as the latest 'get()'
                assertThat(proxy_CORFUSMR.getUnderlyingObject().object.getValue())
                        .isEqualTo(lastRead.get());

                // now read, expect to get the latest written
                assertThat(sharedCounter.getValue()).isEqualTo(lastUpdate.get());

                // remember the last read
                lastRead.set(lastUpdate.get());
            }


        } );


        // invoke the interleaving engine
        scheduleInterleaved(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.CONCURRENCY_SOME*numTasks);
    }


    /**
     * test concurrent 'put()' into a map.
     * the test sets up 'concurrency' number of threads that each inserts a map entry using its own thread index as key.
     * we then check that the map contains keys with all thread indexes.
     *
     * @throws Exception
     */
    @Test
    public void testCorfuMapConcurrency() throws Exception {

        Map<String, String> map = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                })
                .open();
        int concurrency = PARAMETERS.CONCURRENCY_LOTS;

        // Blocking until sequencer becomes functional.
        getDefaultRuntime().getSequencerView().nextToken(Collections.EMPTY_SET, 0);

        // schedule 'concurrency' number of threads,
        // each one put()'s a key with its thread index

        scheduleConcurrently(concurrency, t -> {
            map.put(Integer.toString(t), "world");
        });
        executeScheduled(concurrency, PARAMETERS.TIMEOUT_SHORT);

        // check that map contains keys with every thread index
        for (int i = 0; i < concurrency; i++)
            assertThat(map.containsKey(Integer.toString(i)))
                    .isTrue();

        // check that containsKey can return false..
        // any value outside the range 0..(concurrency-1) will work.
        assertThat(map.containsKey(Integer.toString(concurrency)))
                .isFalse();

    }

    /**
     * test a corfu compound object, where a class field contains an inner class.
     * this is a simple test: set the object fields and check reading back the values.
     *
     * @throws Exception
     */
    @Test
    public void testObjectCompoundSimple() throws Exception {
        CorfuCompoundObj sharedCorfuCompound = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setTypeToken(new TypeToken<CorfuCompoundObj>() {
                })
                .open();

        final int TESTVALUE = 33;
        sharedCorfuCompound.set(sharedCorfuCompound.new Inner("A", "B"), TESTVALUE);

        assertThat(sharedCorfuCompound.getID())
                .isEqualTo(TESTVALUE);
        assertThat(sharedCorfuCompound.getUser().firstName)
                .isEqualTo("A");
        assertThat(sharedCorfuCompound.getUser().lastName)
                .isEqualTo("B");
    }

    /**
     * test concurrent updates to a compound Corfu object with an inner class.
     * the test sets up 'concurrency' number of threads that each sets the compound object fields to thread-specific values.
     * then it does some sanity checks on the final value of the object.
     *
     * @throws Exception
     */
    @Test
    public void testObjectCompoundWriteConcurrency() throws Exception {
        CorfuCompoundObj sharedCorfuCompound = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setTypeToken(new TypeToken<CorfuCompoundObj>() {
                })
                .open();

        int concurrency = PARAMETERS.CONCURRENCY_LOTS;

        // Block until sequencer operational.
        getDefaultRuntime().getSequencerView().nextToken(Collections.EMPTY_SET, 0);

        // set up 'concurrency' number of threads that concurrency update sharedCorfuCompound, each to a different value
        scheduleConcurrently(concurrency, t -> {
            sharedCorfuCompound.set(sharedCorfuCompound.new Inner("A"+t, "B"+t), t);
        });
        executeScheduled(concurrency, PARAMETERS.TIMEOUT_SHORT);

        // sanity checks on the final value of sharedCorfuCompound
        assertThat(sharedCorfuCompound.getID())
                .isBetween(0, concurrency);
        assertThat(sharedCorfuCompound.getUser().firstName)
                .startsWith("A");
        assertThat(sharedCorfuCompound.getUser().lastName)
                .startsWith("B");
    }

    /**
     * test concurrent updates and reads to/from a compound Corfu object with an inner class.
     * this is similar to the tests above, but add tracking of raw stream status.
     *
     * the test sets up 'concurrency' number of concurrent threads.
     * each thread execute a 2-step state machine:
     *
     * One step updates the compound object to a task-specific value.
     * This step also verifies that the stream grows in length by 1.
     *
     * The second step checks that the in-memory object state has not been updated.
     *
     * The test utilizes the interleaving engine to interleaving executions of these state machines
     * over PARAMETERS.NUM_ITERATIONS_MODERATE number of tasks with PARAMETERS.CONCURRENCY_SOME concurrent threads.
     *
     * @throws Exception
     */
    @Test
    public void testObjectCompoundWriteConcurrencyStream() throws Exception {
        CorfuCompoundObj sharedCorfuCompound = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setTypeToken(new TypeToken<CorfuCompoundObj>() {
                })
                .open();

        // for tracking raw stream status
        ICorfuSMR<CorfuCompoundObj> compiledCorfuCompound = (ICorfuSMR<CorfuCompoundObj>) sharedCorfuCompound;
        ICorfuSMRProxyInternal<CorfuCompoundObj> proxy_CORFUSMR = (ICorfuSMRProxyInternal<CorfuCompoundObj>) compiledCorfuCompound.getCorfuSMRProxy();
       // IStreamView objStream = proxy_CORFUSMR.getUnderlyingObject().getStreamViewUnsafe();

        // initialization
        sharedCorfuCompound.set(sharedCorfuCompound.new Inner("E" + 0, "F" + 0), 0);

        // invoke some accessor to sync the in-memory object
        // return value is ignored
        sharedCorfuCompound.getID();

        // build a state-machine:

        // step 1: update the shared compound to task-specific value
        addTestStep((task_num) -> {
            sharedCorfuCompound.set(sharedCorfuCompound.new Inner("C" + task_num, "D" + task_num), task_num);
        });

        // step 2: check the unsync'ed in-memory object state
        addTestStep((ignored_task_num) -> {
            // before sync'ing the in-memory object, the in-memory copy does not get updated
            assertThat(proxy_CORFUSMR.getUnderlyingObject().object.getUser().getFirstName())
                    .startsWith("E");
            assertThat(proxy_CORFUSMR.getUnderlyingObject().object.getUser().getLastName())
                    .startsWith("F");
        });

        // invoke the interleaving engine
        scheduleInterleaved(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.NUM_ITERATIONS_MODERATE);

        assertThat(sharedCorfuCompound.getUser().getFirstName())
                .startsWith("C");
        assertThat(sharedCorfuCompound.getUser().getLastName())
                .startsWith("D");

    }

}
