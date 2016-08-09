package org.corfudb.runtime.collections;

import com.google.common.collect.ImmutableMap;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.ICorfuSMRObject;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.ObjectOpenOptions;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by mwei on 1/7/16.
 */
public class SMRMapTest extends AbstractViewTest {
    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @Test
    @SuppressWarnings("unchecked")
    public void canReadWriteToSingle()
            throws Exception {
        getDefaultRuntime().connect();

        Map<String, String> testMap = getRuntime().getObjectsView().open(UUID.randomUUID(), SMRMap.class);
        testMap.clear();
        assertThat(testMap.put("a", "a"))
                .isNull();
        assertThat(testMap.put("a", "b"))
                .isEqualTo("a");
        assertThat(testMap.get("a"))
                .isEqualTo("b");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canGetID()
            throws Exception {
        CorfuRuntime r = getDefaultRuntime();
        UUID id = UUID.randomUUID();
        ICorfuSMRObject testMap = getRuntime().getObjectsView().open(id, SMRMap.class);
        assertThat(id)
                .isEqualTo(testMap.getStreamID());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void loadsFollowedByGets()
            throws Exception {
        getDefaultRuntime().connect();

        Map<String, String> testMap = getRuntime().getObjectsView().open(UUID.randomUUID(), SMRMap.class);
        testMap.clear();
        for (int i = 0; i < 1_000; i++) {
            assertThat(testMap.put(Integer.toString(i), Integer.toString(i)))
                    .isNull();
        }
        for (int i = 0; i < 1_000; i++) {
            assertThat(testMap.get(Integer.toString(i)))
                    .isEqualTo(Integer.toString(i));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canContainOtherCorfuObjects()
            throws Exception {
        getDefaultRuntime().connect();

        Map<String, String> testMap = getRuntime().getObjectsView().open(CorfuRuntime.getStreamID("a"), SMRMap.class);
        testMap.clear();
        testMap.put("z", "e");
        Map<String, Map<String, String>> testMap2 = getRuntime().getObjectsView().open(CorfuRuntime.getStreamID("b"), SMRMap.class);
        testMap2.put("a", testMap);

        assertThat(testMap2.get("a").get("z"))
                .isEqualTo("e");

        testMap2.get("a").put("y", "f");

        assertThat(testMap.get("y"))
                .isEqualTo("f");

        Map<String, String> testMap3 = getRuntime().getObjectsView().open(CorfuRuntime.getStreamID("a"), SMRMap.class);

        assertThat(testMap3.get("y"))
                .isEqualTo("f");

    }

    @Test
    @SuppressWarnings("unchecked")
    public void canContainNullObjects()
            throws Exception {
        getDefaultRuntime().connect();

        Map<String, String> testMap = getRuntime().getObjectsView().open(CorfuRuntime.getStreamID("a"), SMRMap.class);
        testMap.clear();
        testMap.put("z", null);
        assertThat(testMap.get("z"))
                .isEqualTo(null);
        Map<String, String> testMap2 = getRuntime().getObjectsView().open(CorfuRuntime.getStreamID("a"), SMRMap.class);
        assertThat(testMap2.get("z"))
                .isEqualTo(null);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void loadsFollowedByGetsConcurrent()
            throws Exception {
        getDefaultRuntime().connect();

        Map<String, String> testMap = getRuntime().getObjectsView().open(UUID.randomUUID(), SMRMap.class);

        final int num_threads = 5;
        final int num_records = 2000;
        testMap.clear();

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                assertThat(testMap.put(Integer.toString(i), Integer.toString(i)))
                        .isEqualTo(null);
            }
        });

        long startTime = System.currentTimeMillis();
        executeScheduled(num_threads, 30, TimeUnit.SECONDS);
        calculateRequestsPerSecond("WPS", num_records * num_threads, startTime);

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                assertThat(testMap.get(Integer.toString(i)))
                        .isEqualTo(Integer.toString(i));
            }
        });

        startTime = System.currentTimeMillis();
        executeScheduled(num_threads, 30, TimeUnit.SECONDS);
        calculateRequestsPerSecond("RPS", num_records * num_threads, startTime);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void collectionsStreamInterface()
            throws Exception {
        getDefaultRuntime().connect();

        Map<String, String> testMap = getRuntime().getObjectsView().open(UUID.randomUUID(), SMRMap.class);

        testMap.put("a", "b");
        getRuntime().getObjectsView().TXBegin();
        if (testMap.values().stream().anyMatch(x -> x.equals("c"))) {
            throw new Exception("test");
        }
        testMap.compute("b", (k, v) -> "c");
        getRuntime().getObjectsView().TXEnd();
        assertThat(testMap)
                .containsEntry("b", "c");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canUpdateSingleObjectTransacationally()
            throws Exception {
        getDefaultRuntime().connect();

        Map<String, String> testMap = getRuntime().getObjectsView().open(UUID.randomUUID(), SMRMap.class);
        getRuntime().getObjectsView().TXBegin();
        assertThat(testMap.put("a", "a"))
                .isNull();
        assertThat(testMap.put("a", "b"))
                .isEqualTo("a");
        assertThat(testMap.get("a"))
                .isEqualTo("b");
        getRuntime().getObjectsView().TXEnd();
        assertThat(testMap.get("a"))
                .isEqualTo("b");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void multipleTXesAreApplied()
            throws Exception {
        getDefaultRuntime().connect();

        Map<String, String> testMap = getRuntime().getObjectsView().open(UUID.randomUUID(), SMRMap.class);
        IntStream.range(0, 10).asLongStream()
                .forEach(l -> {
                    try {
                        assertThat(testMap)
                                .hasSize((int) l);
                        getRuntime().getObjectsView().TXBegin();
                        assertThat(testMap.put(Long.toString(l), Long.toString(l)))
                                .isNull();
                        assertThat(testMap)
                                .hasSize((int) l + 1);
                        getRuntime().getObjectsView().TXEnd();
                        assertThat(testMap)
                                .hasSize((int) l + 1);
                    } catch (TransactionAbortedException tae) {
                        throw new RuntimeException(tae);
                    }
                });

        assertThat(testMap)
                .hasSize(10);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void multipleTXesAreAppliedWOAccessors()
            throws Exception {
        getDefaultRuntime().connect();

        Map<String, String> testMap = getRuntime().getObjectsView().open(UUID.randomUUID(), SMRMap.class);
        IntStream.range(0, 10).asLongStream()
                .forEach(l -> {
                    try {
                        getRuntime().getObjectsView().TXBegin();
                        assertThat(testMap.put(Long.toString(l), Long.toString(l)))
                                .isNull();
                        getRuntime().getObjectsView().TXEnd();
                    } catch (TransactionAbortedException tae) {
                        throw new RuntimeException(tae);
                    }
                });

        assertThat(testMap)
                .hasSize(10);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void mutatorFollowedByATransaction()
            throws Exception {
        getDefaultRuntime().connect();

        Map<String, String> testMap = getRuntime().getObjectsView().open(UUID.randomUUID(), SMRMap.class);
        testMap.clear();
        getRuntime().getObjectsView().TXBegin();
        assertThat(testMap.put("a", "a"))
                .isNull();
        assertThat(testMap.put("a", "b"))
                .isEqualTo("a");
        assertThat(testMap.get("a"))
                .isEqualTo("b");
        getRuntime().getObjectsView().TXEnd();
        assertThat(testMap.get("a"))
                .isEqualTo("b");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void objectViewCorrectlyReportsInsideTX()
            throws Exception {
        getDefaultRuntime().connect();
        assertThat(getRuntime().getObjectsView().TXActive())
                .isFalse();
        getRuntime().getObjectsView().TXBegin();
        assertThat(getRuntime().getObjectsView().TXActive())
                .isTrue();
        getRuntime().getObjectsView().TXEnd();
        assertThat(getRuntime().getObjectsView().TXActive())
                .isFalse();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void canUpdateSingleObjectTransacationallyWhenCached()
            throws Exception {
        getDefaultRuntime().connect()
                .setCacheDisabled(false);

        Map<String, String> testMap = getRuntime().getObjectsView().open(UUID.randomUUID(), SMRMap.class);
        getRuntime().getObjectsView().TXBegin();
        assertThat(testMap.put("a", "a"))
                .isNull();
        assertThat(testMap.put("a", "b"))
                .isEqualTo("a");
        assertThat(testMap.get("a"))
                .isEqualTo("b");
        getRuntime().getObjectsView().TXEnd();
        assertThat(testMap.get("a"))
                .isEqualTo("b");
    }


    @Test
    @SuppressWarnings("unchecked")
    public void abortedTransactionsCannotBeReadOnSingleObject()
            throws Exception {
        getDefaultRuntime().connect();

        Map<String, String> testMap = getRuntime().getObjectsView().open(UUID.randomUUID(), SMRMap.class);
        getRuntime().getObjectsView().TXBegin();
        testMap.clear();
        assertThat(testMap.put("a", "a"))
                .isNull();
        assertThat(testMap.put("a", "b"))
                .isEqualTo("a");
        assertThat(testMap.get("a"))
                .isEqualTo("b");
        getRuntime().getObjectsView().TXAbort();
        assertThat(testMap.size())
                .isEqualTo(0);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void modificationDuringTransactionCausesAbort()
            throws Exception {
        getDefaultRuntime().connect();

        Map<String, String> testMap = getRuntime().getObjectsView()
                .open(CorfuRuntime.getStreamID("A"), SMRMap.class);
        assertThat(testMap.put("a", "z"));
        getRuntime().getObjectsView().TXBegin();
        assertThat(testMap.put("a", "a"))
                .isEqualTo("z");
        assertThat(testMap.put("a", "b"))
                .isEqualTo("a");
        assertThat(testMap.get("a"))
                .isEqualTo("b");
        CompletableFuture cf = CompletableFuture.runAsync(() -> {
            Map<String, String> testMap2 = getRuntime().getObjectsView()
                    .open(UUID.nameUUIDFromBytes("A".getBytes()), SMRMap.class, null,
                            EnumSet.of(ObjectOpenOptions.NO_CACHE), Serializers.SerializerType.JSON);
            testMap2.put("a", "f");
        });
        cf.join();
        assertThatThrownBy(() -> getRuntime().getObjectsView().TXEnd())
                .isInstanceOf(TransactionAbortedException.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void smrMapCanContainCustomObjects()
            throws Exception {
        getDefaultRuntime().connect();

        Map<String, TestObject> testMap = getRuntime().getObjectsView().open(UUID.randomUUID(),
                SMRMap.class);
        testMap.put("A", new TestObject("A", 2, ImmutableMap.of("A", "B")));
        assertThat(testMap.get("A").getTestString())
                .isEqualTo("A");
        assertThat(testMap.get("A").getTestInt())
                .isEqualTo(2);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void smrMapCanContainCustomObjectsInsideTXes()
            throws Exception {
        getDefaultRuntime().connect();

        Map<String, TestObject> testMap = getRuntime().getObjectsView().open(UUID.randomUUID(),
                SMRMap.class);

        IntStream.range(0, 10)
                .forEach(l -> {
                    try {
                        getRuntime().getObjectsView().TXBegin();
                        testMap.put(Integer.toString(l), new TestObject(Integer.toString(l), l, ImmutableMap.of(
                                Integer.toString(l), l)));
                        if (l > 0) {
                            assertThat(testMap.get(Integer.toString(l - 1)).getTestInt())
                                    .isEqualTo(l - 1);
                        }
                        getRuntime().getObjectsView().TXEnd();
                    } catch (TransactionAbortedException tae) {
                        throw new RuntimeException(tae);
                    }
                });

        assertThat(testMap.get("3").getTestString())
                .isEqualTo("3");
        assertThat(testMap.get("3").getTestInt())
                .isEqualTo(3);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void unusedMutatorAccessor()
            throws Exception {
        CorfuRuntime r = getDefaultRuntime();

        Map<String, String> testMap = getRuntime().getObjectsView()
                .open(CorfuRuntime.getStreamID("A"), SMRMap.class);

        testMap.put("a", "z");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void concurrentAbortTest()
            throws Exception {
        getDefaultRuntime();

        Map<String, String> testMap = getRuntime().getObjectsView().open(UUID.randomUUID(), SMRMap.class);

        final int num_threads = 5;
        final int num_records = 100;
        AtomicInteger aborts = new AtomicInteger();
        testMap.clear();

        scheduleConcurrently(num_threads, threadNumber -> {
            int base = threadNumber * num_records;
            for (int i = base; i < base + num_records; i++) {
                try {
                    getRuntime().getObjectsView().TXBegin();
                    assertThat(testMap.put(Integer.toString(i), Integer.toString(i)))
                            .isEqualTo(null);
                    getRuntime().getObjectsView().TXEnd();
                } catch (TransactionAbortedException tae) {
                    aborts.incrementAndGet();
                }
            }
        });

        long startTime = System.currentTimeMillis();
        executeScheduled(num_threads, 30, TimeUnit.SECONDS);
        calculateRequestsPerSecond("TPS", num_records * num_threads, startTime);

        calculateAbortRate(aborts.get(), num_records * num_threads);
    }

    @Data
    @ToString
    static class TestObject {
        final String testString;
        final int testInt;
        final Map<String, Object> deepMap;
    }
}
