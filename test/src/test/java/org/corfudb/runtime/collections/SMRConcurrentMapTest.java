package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Test;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Created by mwei on 5/19/17.
 */
public class SMRConcurrentMapTest extends AbstractViewTest {

    @Test
    public void concurrentModificationThrowsException() {
        Map<String, String> map = getDefaultRuntime().getObjectsView()
                .build()
                .setTypeToken(new TypeToken<SMRConcurrentMap<String, String>>() {})
                .setStreamName("test")
                .open();

        // Add some elements to the map
        t1(() -> map.put("a", "a"));
        t1(() -> map.put("b", "b"));
        t1(() -> map.put("c", "c"));
        t1(() -> map.put("d", "d"));
        t1(() -> map.put("e", "e"));
        t1(() -> map.put("f", "f"));

        // Obtain a keyset and it's iterator
        final AtomicReference<Set<String>> keySet =
                new AtomicReference<>();
        final AtomicReference<Iterator<String>> keyIterator =
                new AtomicReference<>();
        t1(() -> keySet.set(map.keySet()));
        t1(() -> keyIterator.set(keySet.get().iterator()));

        // Begin partial iteration.
        t1(() -> keyIterator.get().next())
                .assertResult()
                .isEqualTo("a");
        t1(() -> keyIterator.get().next())
                .assertResult()
                .isEqualTo("b");

        // On another thread, delete some elements
        t2(() -> map.remove("a"));
        t2(() -> map.remove("d"));
        t2(() -> map.remove("f"));

        // Now continue iteration
        t1(() -> keyIterator.get().next())
                .assertResult()
                .isEqualTo("c");
    }
}
