package org.corfudb.runtime.collections;

import lombok.Getter;
import org.corfudb.runtime.object.*;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Created by mwei on 4/7/16.
 */
public class PutIfAbsentMapTest extends AbstractViewTest {

    @Getter
    final String defaultConfigurationString = getDefaultEndpoint();

    @CorfuObject(objectType = ObjectType.SMR,
                stateSource = StateSource.SELF
                )

    public static class PutIfAbsentMap<K,V> implements ICorfuSMRObject {

        HashMap<K,V> map = new HashMap<>();

        @MutatorAccessor
        public V put(K key, V value) {
            return map.put(key, value);
        }

        @Accessor
        public V get(K key) {
            return map.get(key);
        }

        @MutatorAccessor
        public boolean putIfAbsent(K key, V value) {
            if (map.get(key) == null) {
                map.put(key, value);
                return true;
            }
            return false;
        }

    }

    @Test
    public void putIfAbsentTest() {
        getDefaultRuntime();

        PutIfAbsentMap<String, String> stringMap = getRuntime().getObjectsView().build()
                                            .setStreamName("stringMap")
                                            .setType(PutIfAbsentMap.class)
                                            .open();

        stringMap.put("a", "b");

        assertThat(stringMap.get("a"))
                .isEqualTo("b");

        assertThat(stringMap.putIfAbsent("a", "c"))
                .isFalse();

        assertThat(stringMap.get("a"))
                .isEqualTo("b");
    }

    @Test
    public void putIfAbsentTestConcurrent()
    throws Exception {
        getDefaultRuntime();

        PutIfAbsentMap<String, String> stringMap = getRuntime().getObjectsView().build()
                .setStreamName("stringMap")
                .setType(PutIfAbsentMap.class)
                .open();

        ConcurrentLinkedQueue<Boolean> resultList = new ConcurrentLinkedQueue<>();
        scheduleConcurrently(100, x -> {
           resultList.add(stringMap.putIfAbsent("a", Integer.toString(x)));
        });
        executeScheduled(4, 30, TimeUnit.SECONDS);

        long trueCount = resultList.stream()
                .filter(x -> x)
                .count();

        assertThat(trueCount)
                .isEqualTo(1);
    }
}
