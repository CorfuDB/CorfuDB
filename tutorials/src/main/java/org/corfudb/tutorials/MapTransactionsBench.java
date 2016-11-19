package org.corfudb.tutorials;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by dmalkhi on 11/3/16.
 *
 * This tutorial demonstrates different ways to batch map operations into transactions, and their effect on atomicity and on performance.
 */
public class MapTransactionsBench {

    private static CorfuRuntime getRuntimeAndConnect(String configurationString) {
        CorfuRuntime corfuRuntime = new CorfuRuntime(configurationString).connect();
        return corfuRuntime;
    }

    private CorfuRuntime runtime;

    public void start() {
        runtime = getRuntimeAndConnect("localhost:8888");

        SequentialMapBench();
        BatchMapBench();
        MultiStreamBatchMapBench();
    }

    /**
     * populate map in batched transactions.
     * Sometimes developers may group mutator operations into transactions for performance reasons.
     */
    private void BatchMapBench() {
        long startt, endt;

        Map<String, Integer> map = runtime.getObjectsView()
                .build()
                .setStreamName("B")
                .setType(SMRMap.class)
                .open();

        startt = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            runtime.getObjectsView().TXBegin();
            System.out.println("put " + i * 1000);
            for (int j = 0; j < 1000; j++)
                // if (i < 10 || (i % 1000)==0) System.out.println("put " + i);
                map.put("k2" + (i * 1000 + j), i);
            runtime.getObjectsView().TXEnd();
        }
        endt = System.currentTimeMillis();
        System.out.println("time to populate map in batches (secs): " + (endt - startt) / 1000);
    }

    /**
     * populate map sequentially
     */
    private void SequentialMapBench() {
        long startt, endt;

        Map<String, Integer> map2 = runtime.getObjectsView()
                .build()
                .setStreamName("C")
                .setType(SMRMap.class)
                .open();

        startt = System.currentTimeMillis();
        for (int i = 0; i < 100000; i++) {
            if (i < 10 || (i % 1000) == 0) System.out.println("put " + i);
            map2.put("k1" + i, i);
        }
        endt = System.currentTimeMillis();
        System.out.println("time to populate map sequentially (secs): " + (endt - startt) / 1000);

    }

    /** multi stream transactions
     * populate map: in batched transactions
     */
    private void MultiStreamBatchMapBench() {
        long startt, endt;

        List<Map<String, Integer>> maparray = new ArrayList<Map<String, Integer>>(100);
        for (int m = 0; m < 100; m++) {
            maparray.add(m,
                    runtime.getObjectsView()
                            .build()
                            .setStreamName("C" + m)
                            .setType(SMRMap.class)
                            .open()
            );
        }

        startt = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            runtime.getObjectsView().TXBegin();
            if (i < 10 || (i%10)==0) System.out.println("put " + i);
            for (int j = 0; j < 100; j++)
                // if (i < 10 || (i % 1000)==0) System.out.println("put " + i);
                maparray.get(j).put("k2"+(i*100+j), i);
            runtime.getObjectsView().TXEnd();
        }
        endt = System.currentTimeMillis();
        System.out.println("time to populate map-array in batches (secs): " + (endt-startt)/1000);
    }

    public static void main(String[] args) {

        new MapTransactionsBench().start();
    }
}
