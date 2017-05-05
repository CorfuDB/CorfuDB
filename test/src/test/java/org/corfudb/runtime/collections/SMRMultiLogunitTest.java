package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.ObjectsView;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Created by kjames88 on 3/23/17.
 */
public class SMRMultiLogunitTest extends AbstractViewTest {
    public static final int ONE_THOUSAND = 1000;
    public static final int ONE_HUNDRED = 100;
    public CorfuRuntime runtime;


    @Before
    public void setRuntime() {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);
        Layout layout = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(layout);
        runtime = getRuntime().connect();
    }

    /**
     * Single Thread.
     * Test a read after write on SMRMap.
     */
    @Test
    public void simpleWriteRead() {
        Map<String, String> testMap = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();
        assertEquals(testMap.put("1", "a"), null);
        assertEquals(testMap.get("1"), "a");
    }

    /**
     * Single Thread.
     * Verifies a read after 2 writes on the same key.
     * The writes are done across 2 different instantiations of the same SMRMap.
     */
    @Test
    public void writeDualRead() {
        Map<String, String> testMap = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();
        assertEquals(null, testMap.put("1", "a"));
        assertEquals("a", testMap.get("1"));
        Map<String, String> anotherMap = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();
        assertEquals("a", anotherMap.put("1", "b"));
        assertEquals("b", testMap.get("1"));
    }

    /**
     * Single Thread.
     * Verifies read after multiple writes (ONE_THOUSAND).
     */
    @Test
    public void manyWritesThenRead() {
        Map<String, String> testMap = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();
        for (int i=0; i < ONE_THOUSAND; i++) {
            String key = "key" + String.valueOf(i);
            String val = "value" + String.valueOf(i);
            assertEquals(null, testMap.put(key, val));
        }
        // change to another map just to be sure Corfu is doing something
        Map<String, String> anotherMap = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();
        for (int i=0; i < ONE_THOUSAND; i++) {
            String key = "key" + String.valueOf(i);
            String val = "value" + String.valueOf(i);
            assertEquals(val, anotherMap.get(key));
        }
    }

    /**
     * Multi Threaded.
     * Verify reads after multiple transactional writes done concurrently (using 2 threads)
     *
     * @throws TransactionAbortedException
     */

    @Test(expected = TransactionAbortedException.class)
    public void transactionalManyWritesThenRead() throws TransactionAbortedException {
        int numKeys = ONE_THOUSAND;
        ObjectsView view = getRuntime().getObjectsView();
        final CountDownLatch barrier = new CountDownLatch(2);

        Map<String, String> testMap = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        // concurrently run two conflicting transactions:  one or the other should succeed without overlap
        scheduleConcurrently((ignored_step) -> {
            view.TXBegin();
            for (int i=0; i < numKeys; i++) {
                String key = "key" + String.valueOf(i);
                String val = "value0_" + String.valueOf(i);
                testMap.put(key, val);
                if (i == 0) {
                    barrier.countDown();
                    barrier.await();
                }
                if (i % ONE_HUNDRED == 0) {
                    Thread.yield();
                }
            }
            view.TXEnd();
        });

        scheduleConcurrently((ignored_step) -> {
            view.TXBegin();
            for (int i = 0; i < numKeys; i++) {
                String key = "key" + String.valueOf(i);
                String val = "value1_" + String.valueOf(i);
                testMap.put(key, val);
                if (i == 0) {
                    barrier.countDown();
                    barrier.await();
                }
                if (i % ONE_HUNDRED == 0) {
                    Thread.yield();
                }
            }
            view.TXEnd();
        });

        try {
            executeScheduled(2, PARAMETERS.TIMEOUT_NORMAL);
        } catch (TransactionAbortedException e) {
            throw e;
        } catch (Exception e) {
            e.printStackTrace();
        }
        // check that all the values are either value0_ or value1_ not a mix
        String base = "invalid";
        for (int i=0; i < numKeys; i++) {
            String key = "key" + String.valueOf(i);
            String val = testMap.get(key);
            if (val != null) {
                if (i == 0) {
                    int underscore = val.indexOf("_");
                    assertNotEquals(-1, underscore);
                    base = val.substring(0, underscore);
                    System.out.println("base is " + base);
                }
            }
            assertEquals(true, val.contains(base));
        }
    }

    /**
     * Multi Threaded.
     * Verify reads after multiple non-transactional writes done concurrently (using 2 threads)
     *
     */
    @Test
    public void multiThreadedManyWritesThenRead() {
        int numKeys = ONE_THOUSAND;
        ObjectsView view = getRuntime().getObjectsView();
        Map<String, String> testMap = getRuntime()
                .getObjectsView()
                .build()
                .setStreamName("test")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        AtomicInteger threadsComplete = new AtomicInteger(0);
        addTestStep((step) -> {
            for (int i=0; i < numKeys; i++) {
                String key = "key" + String.valueOf(i);
                String val = "value" + String.valueOf(step) + "_" + String.valueOf(i);
                testMap.put(key, val);
                if (i % ONE_HUNDRED == 0) {
                    Thread.yield();
                }
            }
            threadsComplete.incrementAndGet();
        });
        try {
            scheduleThreaded(2, 2);
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertEquals(2, threadsComplete.get());
        for (int i=0; i < numKeys; i++) {
            String key = "key" + String.valueOf(i);
            String val = testMap.get(key);
            assertNotNull(val);
        }
    }
}