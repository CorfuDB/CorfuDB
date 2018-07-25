package org.corfudb.perfDriver;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;

import java.util.Map;
import java.util.Random;
import java.util.UUID;

public class Driver {
    public static void main(String[] args) {
        String connString = args[0];
        final int size = Integer.valueOf(args[1]);
        final int numQuery = Integer.valueOf(args[2]);
        final UUID streamID = UUID.randomUUID();

        CorfuRuntime rt = new CorfuRuntime(connString).connect();
        Random rand = new Random();

        long putStartTime = System.currentTimeMillis();

        Map<Integer, Integer> map = rt.getObjectsView().build().setStreamID(streamID).setType(SMRMap.class).open();
        for (int i = 0; i < size; i++) {
            map.put(i, i + 1);
        }

        long putEndTime = System.currentTimeMillis();
        long putTime = putEndTime - putStartTime;
        System.out.println("Writes Throughput: " + ((size * 1.0) / (putTime * 1.0)));

        long getStartTime = System.currentTimeMillis();

        rt.getObjectsView().TXBegin();
        for (int i = 0; i < numQuery; i++) {
            int index = rand.nextInt(size - 1);
            map.get(index);
        }
        rt.getObjectsView().TXEnd();

        long getEndTime = System.currentTimeMillis();
        long getTime = getEndTime - getStartTime;
        System.out.println("Reads Throughput: " + ((numQuery) / (getTime * 1.0)));

        System.out.println("Throughput: " + ((size + numQuery) / ((putTime + getTime) * 1.0)));
    }
}
