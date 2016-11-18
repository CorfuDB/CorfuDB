package org.corfudb.tutorials;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;

import java.util.Map;

import static java.lang.Runtime.getRuntime;

/**
 * Created by dmalkhi on 11/3/16.
 *
 * Recommended practices 1 tutorial: Avoid direct accessing into Corfu Object state.
 *
 * A Corfu object state must only be manipulated through its methods.
 * Otherwise, Corfu will not be aware that the in-memory state has modified, and will not back the update on the Corfu log.
 */
public class InconsistentInMemoryView {
    private static CorfuRuntime getRuntimeAndConnect(String configurationString) {
        CorfuRuntime corfuRuntime = new CorfuRuntime(configurationString).connect();
        return corfuRuntime;
    }

    /**
     * Any object type can be used inside a map, just like a normal Java map. However, modifications to objects stored in the map should only be made via map methods.
     * First, we declare a mutable class.
     */
    class MutableClass {
        public int value;
        public MutableClass(int value) {
            this.value = value;
        }
    }

    public static void main(String[] args) {
        new InconsistentInMemoryView().start();
    }

    private void start() {
        CorfuRuntime runtime = getRuntimeAndConnect("localhost:8888");

        /* Next, we will declare a map that stores MutableClass objects: */
        Map<String, MutableClass> testMap = runtime.getObjectsView()
                .build()
                .setType(SMRMap.class)
                .setStreamName("stream1")
                .open();
        String key = "key1";
        testMap.put(key, new MutableClass(1));

        /* We will insert a MutableClass object with key "key1" to the map. We then let the application modify the memory object directly, not through map operations.
         * The Corfu runtime will not be aware of the change, so the in-memory cached copy of the object will not be backed by the Corfu log.
         */
        int value1 = testMap.get(key).value;
        MutableClass mutable = testMap.get(key);
        mutable.value = 2;
        int value2 = testMap.get(key).value;

        System.out.println("value1=" + value1);
        System.out.println("value2=" + value2);  /*     this will print "value2=1" */
    }

}
