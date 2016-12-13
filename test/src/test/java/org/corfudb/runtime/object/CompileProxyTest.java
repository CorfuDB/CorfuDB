package org.corfudb.runtime.object;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.Mutator;
import org.corfudb.annotations.MutatorAccessor;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.StreamView;
import org.corfudb.runtime.view.StreamsView;
import org.junit.Test;
import org.omg.CORBA.INITIALIZE;
import org.omg.CORBA.TIMEOUT;

import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 11/11/16.
 */
public class CompileProxyTest extends AbstractViewTest {
    
    @Test
    public void testMap() throws Exception {

        Map<String, String> map = getDefaultRuntime()
                                    .getObjectsView().build()
                                    .setStreamName("my stream")
                                    .setUseCompiledClass(true)
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
    public void testSharedCounterSimple() throws Exception {
        CorfuSharedCounter sharedCounter = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setUseCompiledClass(true)
                .setTypeToken(new TypeToken<CorfuSharedCounter>() {
                })
                .open();

        final int VALUE = 33;
        sharedCounter.setValue(VALUE);
        assertThat(sharedCounter.getValue())
                .isEqualTo(VALUE);
    }

    @Test
    public void testCorfuSharedCounterConcurrentWrites() throws Exception {
        CorfuSharedCounter sharedCounter = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setUseCompiledClass(true)
                .setTypeToken(new TypeToken<CorfuSharedCounter>() {
                })
                .open();
        int concurrency = PARAMETERS.CONCURRENCY_SOME;

        sharedCounter.setValue(-1);
        assertThat(sharedCounter.getValue())
                .isEqualTo(-1);

        scheduleConcurrently(concurrency, t ->
            sharedCounter.setValue(t)
        );
        executeScheduled(concurrency, PARAMETERS.TIMEOUT_NORMAL);

        ICorfuSMR<CorfuSharedCounter> compiledSharedCounter = (ICorfuSMR<CorfuSharedCounter>)  sharedCounter;
        ICorfuSMRProxyInternal<CorfuSharedCounter> proxy_CORFUSMR = (ICorfuSMRProxyInternal<CorfuSharedCounter>) compiledSharedCounter.getCorfuSMRProxy();
        StreamView objStream = proxy_CORFUSMR.getUnderlyingObject().getStreamViewUnsafe();

        assertThat(objStream.check())
                .isEqualTo(concurrency);

        int beforeSync, afterSync;

        // before sync'ing the in-memory object, the in-memory copy does not get updated
        assertThat(beforeSync = proxy_CORFUSMR.getUnderlyingObject().object.getValue())
                .isEqualTo(-1);

        // after sync'ing the in-memory map object, the in-memory map has all the keys
        for (int timestamp = 1; timestamp <= concurrency; timestamp++) {
            proxy_CORFUSMR.syncObjectUnsafe(proxy_CORFUSMR.getUnderlyingObject(), timestamp);
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

        int curValue = sharedCounter.getValue();
        AtomicInteger casSucceeded = new AtomicInteger(0);
        scheduleConcurrently(concurrency, t -> {
                    if (sharedCounter.CAS(curValue, t+1) == curValue)
                        casSucceeded.incrementAndGet();
        });
        executeScheduled(concurrency, PARAMETERS.TIMEOUT_SHORT);
        assertThat(sharedCounter.getValue())
                .isBetween(0, concurrency);
        assertThat(casSucceeded.get())
                .isEqualTo(1);
    }

    /**
     * the test interleaves reads and writes to a shared counter.
     *
     * The test uses the interleaving engine to interleave numTasks*threads state machines.
     * A simple state-machine is built. It randomly chooses to either read or write the counter.
     * On a read, the last written value is expected.
     *
     * @throws Exception
     */
    @Test
    public void testCorfuSharedCounterConcurrentReads() throws Exception {
        CorfuSharedCounter sharedCounter = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setUseCompiledClass(true)
                .setTypeToken(new TypeToken<CorfuSharedCounter>() {
                })
                .open();

        int numTasks = PARAMETERS.NUM_ITERATIONS_LOW;
        Random r = new Random(PARAMETERS.isRandomSeed() ? System.currentTimeMillis() : 0);

        sharedCounter.setValue(-1);
        AtomicInteger lastUpdate = new AtomicInteger(-1);

        assertThat(sharedCounter.getValue())
                .isEqualTo(lastUpdate.get());

        // build a state-machine:
        ArrayList<BiConsumer<Integer, Integer>> stateMachine = new ArrayList<BiConsumer<Integer, Integer>>(){

            {
                // only one step: randomly choose between read/write of the shared counter
                add ((Integer ignored_thread_num, Integer task_num) -> {
                    if (r.nextBoolean()) {
                        sharedCounter.setValue(task_num);
                        lastUpdate.set(task_num); // remember the last written value
                    } else {
                        assertThat(sharedCounter.getValue()).isEqualTo(lastUpdate.get()); // expect to read the value in lastUpdate
                    }
                } );
            }
        };

        // invoke the interleaving engine
        scheduleInterleaved(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.CONCURRENCY_SOME*numTasks, stateMachine);
    }

    /**
     * the test is similar to the interleaved read/write above to a shared counter.
     * in addition to checking read/write values, it tracks the raw stream state between updates and syncs.
     *
     * The test uses the interleaving engine to interleave numTasks*threads state machines.
     * A simple state-machine is built. It randomly chooses to either read or write the counter.
     * On a read, the last written value is expected.
     *
     * @throws Exception
     */

    @Test
    public void testCorfuSharedCounterConcurrentMixedReadsWrites() throws Exception {
        CorfuSharedCounter sharedCounter = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setUseCompiledClass(true)
                .setTypeToken(new TypeToken<CorfuSharedCounter>() {
                })
                .open();

        ICorfuSMR<CorfuSharedCounter> compiledSharedCounter = (ICorfuSMR<CorfuSharedCounter>)  sharedCounter;
        ICorfuSMRProxyInternal<CorfuSharedCounter> proxy_CORFUSMR = (ICorfuSMRProxyInternal<CorfuSharedCounter>) compiledSharedCounter.getCorfuSMRProxy();
        StreamView objStream = proxy_CORFUSMR.getUnderlyingObject().getStreamViewUnsafe();

        int numTasks = PARAMETERS.NUM_ITERATIONS_LOW;
        Random r = new Random(PARAMETERS.isRandomSeed() ? System.currentTimeMillis() : 0);

        final int INITIAL = -1;

        sharedCounter.setValue(INITIAL);
        AtomicInteger lastUpdate = new AtomicInteger(INITIAL);
        AtomicLong lastUpdateStreamPosition = new AtomicLong(0);

        assertThat(sharedCounter.getValue())
                .isEqualTo(lastUpdate.get());

        AtomicInteger lastRead = new AtomicInteger(INITIAL);

        // build a state-machine:
        ArrayList<BiConsumer<Integer, Integer>> stateMachine = new ArrayList<BiConsumer<Integer, Integer>>(){

            {
                // only one step: randomly choose between read/write of the shared counter
                add ((Integer ignored_thread_num, Integer task_num) -> {

                    // check that stream has the expected number of entries: number of updates - 1
                    assertThat(objStream.check())
                            .isEqualTo(lastUpdateStreamPosition.get());

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
            }
        };

        // invoke the interleaving engine
        scheduleInterleaved(PARAMETERS.CONCURRENCY_SOME, PARAMETERS.CONCURRENCY_SOME*numTasks, stateMachine);
    }


    @Test
    public void testCorfuMapConcurrency() throws Exception {

        Map<String, String> map = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setUseCompiledClass(true)
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {
                })
                .open();
        int concurrency = PARAMETERS.CONCURRENCY_SOME;

        scheduleConcurrently(concurrency, t -> {
            map.put(t.toString(), "world");
        });
        executeScheduled(concurrency, PARAMETERS.TIMEOUT_SHORT);


        for (int i = 0; i < concurrency; i++)
            assertThat(map.containsKey(Integer.toString(i)))
                    .isTrue();

        assertThat(map.containsKey(Integer.toString(concurrency)))
                .isFalse();

    }

    @Test
    public void testCorfuCompoundObjectSimple() throws Exception {
        CorfuCompoundObj sharedCorfuCompound = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setUseCompiledClass(true)
                .setTypeToken(new TypeToken<CorfuCompoundObj>() {
                })
                .open();

        final int VALUE = 33;
        CorfuCompoundObj.Inner inner = sharedCorfuCompound.new Inner();
        inner.setFirstName("A");
        inner.setLastName("B");
        sharedCorfuCompound.set(inner, VALUE);

        assertThat(sharedCorfuCompound.getID())
                .isEqualTo(VALUE);
        assertThat(sharedCorfuCompound.getUser().firstName)
                .isEqualTo("A");
        assertThat(sharedCorfuCompound.getUser().lastName)
                .isEqualTo("B");
    }

    @Test
    public void testCorfuCompoundObjectConcurrentWrites() throws Exception {
        CorfuCompoundObj sharedCorfuCompound = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setUseCompiledClass(true)
                .setTypeToken(new TypeToken<CorfuCompoundObj>() {
                })
                .open();

        int concurrency = PARAMETERS.CONCURRENCY_SOME;
        CorfuCompoundObj.Inner inner = sharedCorfuCompound.new Inner();

        scheduleConcurrently(concurrency, t -> {
            inner.setFirstName("A" + t);
            inner.setLastName("B" + t);
            sharedCorfuCompound.set(inner, t);
        });
        executeScheduled(concurrency, PARAMETERS.TIMEOUT_SHORT);

        assertThat(sharedCorfuCompound.getID())
                .isBetween(0, concurrency);

        assertThat(sharedCorfuCompound.getUser().firstName)
                .startsWith("A");
        assertThat(sharedCorfuCompound.getUser().lastName)
                .startsWith("B");
    }


    @Test
    public void testCorfuCompoundObjectConcurrentMixed() throws Exception {
        CorfuCompoundObj sharedCorfuCompound = getDefaultRuntime()
                .getObjectsView().build()
                .setStreamName("my stream")
                .setUseCompiledClass(true)
                .setTypeToken(new TypeToken<CorfuCompoundObj>() {
                })
                .open();

        ICorfuSMR<CorfuCompoundObj> compiledCorfuCompound = (ICorfuSMR<CorfuCompoundObj>) sharedCorfuCompound;
        ICorfuSMRProxyInternal<CorfuCompoundObj> proxy_CORFUSMR = (ICorfuSMRProxyInternal<CorfuCompoundObj>) compiledCorfuCompound.getCorfuSMRProxy();
        StreamView objStream = proxy_CORFUSMR.getUnderlyingObject().getStreamViewUnsafe();

        CorfuCompoundObj.Inner inner = sharedCorfuCompound.new Inner();

        inner.setFirstName("E" + 0);
        inner.setLastName("F" + 0);
        sharedCorfuCompound.set(inner, 0);
        sharedCorfuCompound.getID();

        for (int repetition = 0; repetition < PARAMETERS.NUM_ITERATIONS_LOW; repetition += 2) {
            final int r = repetition;
            CorfuCompoundObj.Inner inn = sharedCorfuCompound.new Inner();
            inn.setFirstName("C" + (r+1));
            inn.setLastName("D" + (r+1));
            t(1, () -> sharedCorfuCompound.set(inn, r+1));
            t(2, () -> {
                assertThat(objStream.check())
                    .isEqualTo(r+1);

                // before sync'ing the in-memory object, the in-memory copy does not get updated
                assertThat(proxy_CORFUSMR.getUnderlyingObject().object.getUser().getFirstName())
                        .startsWith("E" + r);
                assertThat(proxy_CORFUSMR.getUnderlyingObject().object.getUser().getLastName())
                        .startsWith("F" + r);
            });

            inn.setFirstName("E" + (r+2));
            inn.setLastName("F" + (r+2));
            t(1, () -> sharedCorfuCompound.set(inn, r+2));
            t(2, () -> {
                assertThat(objStream.check())
                        .isEqualTo(r+2);

                // before sync'ing the in-memory object, the in-memory copy does not get updated
                assertThat(proxy_CORFUSMR.getUnderlyingObject().object.getUser().getFirstName())
                        .startsWith("E" + r);
                assertThat(proxy_CORFUSMR.getUnderlyingObject().object.getUser().getLastName())
                        .startsWith("F" + r);

                assertThat(sharedCorfuCompound.getUser().getFirstName())
                        .startsWith("E" + (r+2));
                assertThat(sharedCorfuCompound.getUser().getLastName())
                        .startsWith("F" + (r+2));
            });
        }

    }

}
