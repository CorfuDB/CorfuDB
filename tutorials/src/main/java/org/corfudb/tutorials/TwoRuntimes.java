package org.corfudb.tutorials;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.FGMap;
import org.corfudb.runtime.collections.SMRMap;

import java.util.Map;
import java.util.Set;

/**
 * Created by dmalkhi on 11/15/16.
 */
public class TwoRuntimes {
    private static CorfuRuntime getRuntimeAndConnect(String configurationString) {
        CorfuRuntime corfuRuntime = new CorfuRuntime(configurationString).connect();
        return corfuRuntime;
    }

    public static void main(String[] args) {
        new TwoRuntimes().start();
    }

    private void start() {
        CorfuRuntime runtime1 = getRuntimeAndConnect("localhost:8888");
        CorfuRuntime runtime2 = getRuntimeAndConnect("localhost:8888");


        Map<String, Integer> testMap1 = runtime1.getObjectsView()
                .build()
                .setType(FGMap.class)
                .setStreamName("stream1")
                .open();
        Map<String, Integer> testMap2 = runtime2.getObjectsView()
                .build()
                .setType(FGMap.class)
                .setStreamName("stream1")
                .open();

        Set<String> value2 = testMap2.keySet();


        String key = "key1";
        testMap1.put(key, new Integer(1));
        int value1 = testMap1.get(key);

        System.out.println("value1=" + value1);
        System.out.println("value2=" + value2);
    }

}
