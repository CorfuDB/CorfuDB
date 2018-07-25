package org.corfudb.perfDriver;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class Driver {
    public static void mapTest(String[] args) {
        String connString = args[0];
        final int size1 = Integer.valueOf(args[1]);
        final int size2 = Integer.valueOf(args[2]);
        final int size3 = Integer.valueOf(args[3]);
        final int size4 = Integer.valueOf(args[4]);
        final int numQuery = Integer.valueOf(args[5]);
        final UUID streamID = UUID.randomUUID();

        CorfuRuntime rt = new CorfuRuntime(connString).connect();
        Random rand = new Random();

        Map<Integer, Integer> map1 = rt.getObjectsView().build().setStreamID(streamID).setType(SMRMap.class).open();
        for (int i = 0; i < size1; i++) {
            map1.put(i, i + 1);
        }
        System.out.println("done with puts");

        long startTime = System.currentTimeMillis();

        rt.getObjectsView().TXBegin();
        for (int i = 0; i < numQuery; i++) {
            int index = rand.nextInt(size1 - 1);
            map1.get(index);
        }
        rt.getObjectsView().TXEnd();

        long endTime = System.currentTimeMillis();
        long time = endTime - startTime;
        System.out.println("Throughput: " + ((time * 1.0) / (size1 + numQuery)));

//        Map<Integer, Integer> map2 = new HashMap<>();
//        for (int i = 0; i < size2; i++) {
//            map2.put(i, i + 1);
//        }
//
//        Map<Integer, Integer> map3 = new HashMap<>();
//        for (int i = 0; i < size3; i++) {
//            map3.put(i, i + 1);
//        }
//
//        Map<Integer, Integer> map4 = new HashMap<>();
//        for (int i = 0; i < size4; i++) {
//            map4.put(i, i + 1);
//        }
    }
}
