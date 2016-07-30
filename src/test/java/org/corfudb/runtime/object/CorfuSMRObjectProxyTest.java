package org.corfudb.runtime.object;

import lombok.Getter;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.infrastructure.LogUnitServer;
import org.corfudb.infrastructure.SequencerServer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.ObjectOpenOptions;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 1/21/16.
 */
public class CorfuSMRObjectProxyTest extends AbstractViewTest {
    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteToSingle()
            throws Exception {
        getDefaultRuntime();

        Map<String,String> testMap = getRuntime().getObjectsView().open(
                CorfuRuntime.getStreamID("test"), TreeMap.class);
        testMap.clear();
        assertThat(testMap.put("a","a"))
                .isNull();
        assertThat(testMap.put("a","b"))
                .isEqualTo("a");
        assertThat(testMap.get("a"))
                .isEqualTo("b");

        Map<String,String> testMap2 = getRuntime().getObjectsView().open(
                CorfuRuntime.getStreamID("test"), TreeMap.class);
        assertThat(testMap2.get("a"))
                .isEqualTo("b");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void multipleWritesConsistencyTest()
            throws Exception {
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        wireRouters();

        getRuntime().connect();

        Map<String,String> testMap = getRuntime().getObjectsView().open(
                CorfuRuntime.getStreamID("test"), TreeMap.class);
        testMap.clear();

        for (int i = 0; i < 10_000; i++) {
            assertThat(testMap.put(Integer.toString(i), Integer.toString(i)))
                    .isNull();
        }

        Map<String,String> testMap2 = getRuntime().getObjectsView().open(
                CorfuRuntime.getStreamID("test"), TreeMap.class);
        for (int i = 0; i < 10_000; i++) {
            assertThat(testMap2.get(Integer.toString(i)))
                    .isEqualTo(Integer.toString(i));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void multipleWritesConsistencyTestConcurrent()
            throws Exception {
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        wireRouters();

        getRuntime().connect();


        Map<String,String> testMap = getRuntime().getObjectsView().open(
                CorfuRuntime.getStreamID("test"), TreeMap.class);
        testMap.clear();
        int num_threads = 5;
        int num_records = 1_000;

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                assertThat(testMap.put(Integer.toString(i), Integer.toString(i)))
                        .isEqualTo(null);
            }
        });
        executeScheduled(num_threads, 50, TimeUnit.SECONDS);

        Map<String,String> testMap2 = getRuntime().getObjectsView().open(
                CorfuRuntime.getStreamID("test"), TreeMap.class);

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                assertThat(testMap2.get(Integer.toString(i)))
                        .isEqualTo(Integer.toString(i));
            }
        });
        executeScheduled(num_threads, 50, TimeUnit.SECONDS);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canWrapObjectWithPrimitiveTypes()
            throws Exception {
        // default layout is chain replication.
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        wireRouters();

        //begin tests
        CorfuRuntime r = getRuntime().connect();
        TestClassWithPrimitives test = r.getObjectsView().open("test", TestClassWithPrimitives.class);
        test.setPrimitive("hello world".getBytes());
        assertThat(test.getPrimitive())
                .isEqualTo("hello world".getBytes());
    }


    @Test
    @SuppressWarnings("unchecked")
    public void canUsePrimitiveSerializer()
            throws Exception {
        // default layout is chain replication.
        addServerForTest(getDefaultEndpoint(), new LayoutServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new LogUnitServer(defaultOptionsMap()));
        addServerForTest(getDefaultEndpoint(), new SequencerServer(defaultOptionsMap()));
        wireRouters();

        //begin tests
        CorfuRuntime r = getRuntime().connect();
        TestClassWithPrimitives test = r.getObjectsView().build()
                                                            .setType(TestClassWithPrimitives.class)
                                                            .setStreamName("test")
                                                            .setSerializer(Serializers.SerializerType.PRIMITIVE)
                                                            .open();
        test.setPrimitive("hello world".getBytes());
        assertThat(test.getPrimitive())
                .isEqualTo("hello world".getBytes());
    }

    @Test
    public void postHandlersFire() throws Exception {
        CorfuRuntime r = getDefaultRuntime();

        Map<String,String> test = r.getObjectsView().build()
                .setType(SMRMap.class)
                .setStreamName("test")
                .open();

        ICorfuSMRObject cObj = (ICorfuSMRObject) test;
        final AtomicInteger ai = new AtomicInteger(0);
        cObj.registerPostHandler((String method, Object[] args, Object state) -> {
            ai.incrementAndGet();
        });
        test.put("a", "b");
        test.get("a");
        assertThat(ai.get())
                .isEqualTo(1);
    }

}
