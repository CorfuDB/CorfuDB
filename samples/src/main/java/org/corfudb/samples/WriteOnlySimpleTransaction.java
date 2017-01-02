package org.corfudb.samples;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;

import java.util.Map;

/**
 * A write-only transaction is a normal transaction that has only object-mutator method invokations,
 * and does not perform any object-accessor invocations.
 *
 * In the default (Optimistic) transaction isolation level, since it has no read-set,
 * a write-only transaction never needs to abort due to another transaction's commit.
 *
 * The main reason to group mutator updates into a write-transaction is commit *atomicity*:
 * Either all of mutator updates are visible to an application or none.
 *
 * This program illustrates the write atomicity concept with a simple transaction example.
 *
 * Created by dalia on 12/30/16.
 */
public class WriteOnlySimpleTransaction {
    static String defaultCorfuService = "localhost:9999";

    /**
     * @param configurationString specifies the IP:port of the CorfuService
     * @return a CorfuRuntime object, with which Corfu applications perform all Corfu operations
     */
    private static CorfuRuntime getRuntimeAndConnect(String configurationString) {
        CorfuRuntime corfuRuntime = new CorfuRuntime(configurationString).connect();
        return corfuRuntime;
    }

    public static void main(String[] args) {
        /**
         * First, the application needs to instantiate a CorfuRuntime
         */
        CorfuRuntime runtime = getRuntimeAndConnect(defaultCorfuService);

        /**
         * Instantiate a Corfu Stream named "A" dedicated to an SMRmap object.
         */
        Map<String, Integer> map = runtime.getObjectsView()
                .build()
                .setStreamName("A")     // stream name
                .setType(SMRMap.class)  // object class backed by this stream
                .open();                // instantiate the object!

        // thread 1: update "a" and "b" atomically
        new Thread(() -> {
            runtime.getObjectsView().TXBegin();
            map.put("a", 1);
            map.put("b", 1);
            runtime.getObjectsView().TXEnd();
        }
        ).start();

        // thread 2: read "a", then "b"
        // this thread will print either both 1's, or both nil's
        new Thread(() -> {
            Integer valA = 0, valB = 0;
            valA = map.get("a");
            System.out.println("a: " + valA);
            valB = map.get("b");
            System.out.println("b: " + valB);
        }
        ).start();
    }
}
