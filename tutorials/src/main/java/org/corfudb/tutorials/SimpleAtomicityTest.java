package org.corfudb.tutorials;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;

import java.util.Map;

/**
 * Created by dmalkhi on 11/17/16.
 */
public class SimpleAtomicityTest {
    private static CorfuRuntime getRuntimeAndConnect(String configurationString) {
        CorfuRuntime corfuRuntime = new CorfuRuntime(configurationString).connect();
        return corfuRuntime;
    }

    private CorfuRuntime runtime;

    public void start() {
        runtime = getRuntimeAndConnect("localhost:9090");

        SimpleAtomicityBench();
    }

    private void SimpleAtomicityBench() {

        Map<String, Integer> myunsafemap =
                runtime.getObjectsView()
                        .build()
                        .setStreamName("M")
                        .setType(SMRMap.class)
                        .open();

        // thread 1: update "a" and "b" atomically
        new Thread(() -> {
            runtime.getObjectsView().TXBegin();
            synchronized (myunsafemap) { myunsafemap.put("a", 1); }
            synchronized (myunsafemap) { myunsafemap.put("b", 1); }
            runtime.getObjectsView().TXEnd();
        }
        ).start();

        // thread 2: read "a", then "b"
        new Thread(() -> {
            Integer valA=0, valB=0;
            synchronized (myunsafemap) {valA = myunsafemap.get("a");}
            System.out.println("a: " + valB);
            synchronized (myunsafemap) {valA = myunsafemap.get("a");}
            System.out.println("b: " + valB);
        }
        ).start();
    }

    public static void main(String[] args) {

        new MapTransactionsBench().start();
    }

}
