package org.corfudb.perfClient;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.view.*;

import java.util.*;

public class Driver {
    public static void runProducerConsumer(String[] args, String prodClass, String consClass) throws Exception {
        String connString = args[0];
        final int numRt = Integer.valueOf(args[1]);
        final int numProd = Integer.valueOf(args[2]);
        final int numCons = Integer.valueOf(args[3]);
        final int numReq = Integer.valueOf(args[4]);
        final int payloadSize = Integer.valueOf(args[5]);
        final UUID streamID = UUID.randomUUID();

        CorfuRuntime[] rts = new CorfuRuntime[numRt];

        for (int x = 0; x < numRt; x++) {
            rts[x] = new CorfuRuntime(connString).connect();
        }

        Thread[] prodThreads = new Thread[numProd];
        Thread[] consThreads = new Thread[numCons];

        byte[] payload = new byte[payloadSize];

        // Producer
        int[] numWrites = new int[numProd];
        boolean[] completed = {false};
        for (int x = 0; x < numProd; x++) {
            final CorfuRuntime rt = rts[x%numRt];
            final int ind = x;
            Runnable r = () -> {
                long startTime = System.currentTimeMillis();

                System.out.println("Inside producer " + (ind));

                Producer producer = null;
                if (prodClass.equals("AddressSpaceProducer")) {
                    producer = new AddressSpaceProducer(rt);
                } else if (prodClass.equals("StreamsProducer")) {
                    producer = new StreamsProducer(rt, streamID);
                }

                for (int i = 0; i < numReq; i++) {
                    producer.send(payload);
                    numWrites[ind] += 1;
                    System.out.println("Incremented num writes to " + (numWrites[ind]));
                }

                long endTime = System.currentTimeMillis();
                long time = endTime - startTime;

                System.out.println("(Producer) Latency [ms/op]: " + ((time*1.0)/numReq)
                        + " " + ((time*1.0)/numWrites[ind]));
                System.out.println("(Producer) Throughput [ops/ms]: " + (numReq/(time*1.0))
                        + " " + (numWrites[ind])/(time*1.0));
                System.out.println("Num Writes: " + numWrites[ind]);
            };

            prodThreads[x] = new Thread(r); // performs lambda fn from above
        }

        // Consumer
        int[] numReads = new int[numCons];
        for (int x = 0; x < numCons; x++) {
            final CorfuRuntime rt = rts[x%numRt];
            final int ind = x;
            Runnable r = () -> {
                long startTime = System.currentTimeMillis();

                System.out.println("Inside consumer " + (ind));

                List<ILogData> data = new ArrayList<>();

                Consumer consumer = null;
                if (consClass.equals("AddressSpaceConsumer")) {
                    consumer = new AddressSpaceConsumer(rt);
                } else if (consClass.equals("StreamConsumer")) {
                    consumer = new StreamConsumer(streamID, rt);
                }

                List<ILogData> newData;
                while (!completed[0]) {
                    newData = consumer.poll(1000);
                    data.addAll(newData);
                    numReads[ind] += newData.size();
                    System.out.println("Incremented num reads to " + (numWrites[ind]));
                }
                newData = consumer.poll(1000);
                data.addAll(newData);
                numReads[ind] += newData.size();

                long endTime = System.currentTimeMillis();
                long time = endTime - startTime;

                System.out.println("(Consumer) Latency [ms/op]: " + ((time*1.0)/numReads[ind]));
                System.out.println("(Consumer) Throughput [ops/ms]: " + (numReads[ind]/(time*1.0)));
                System.out.println("Num Reads: " + numReads[ind]);
            };

            consThreads[x] = new Thread(r); // performs lambda fn from above
        }

        long startTime = System.currentTimeMillis();

        for (int x = 0; x < numProd; x++) {
            prodThreads[x].start();
        }
        for (int x = 0; x < numCons; x++) {
            consThreads[x].start();
        }
        for (int x = 0; x < numProd; x++) {
            prodThreads[x].join();
        }

        completed[0] = true;

        for (int x = 0; x < numCons; x++) {
            consThreads[x].join();
        }

        long endTime = System.currentTimeMillis();
        long time = endTime - startTime;

        int totalWrites = 0;
        for (int i = 0; i < numProd; i++) {
            totalWrites += numWrites[i];
        }
        int totalReads = 0;
        for (int i = 0; i < numCons; i++) {
            totalReads += numReads[i];
        }

        System.out.println("\nOverall");
        System.out.println("Total Writes: " + (totalWrites));
        System.out.println("Total Reads: " + (totalReads));

        System.out.println("Total Time [ms]: " + (time));
        System.out.println("Num Ops: " + (totalWrites + totalReads));
        System.out.println("Throughput: " + (((totalWrites + totalReads)*1.0) / (time)));
    }

    public static void main(String[] args) throws Exception {
        //runProducerConsumer(args, "AddressSpaceProducer", "AddressSpaceConsumer");
        //runProducerConsumer(args, "StreamsProducer", "StreamConsumer");
        mapTest(args);
    }

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

        long startTime = System.currentTimeMillis();

        Map<Integer, Integer> map1 = rt.getObjectsView().build().setStreamID(streamID).setType(SMRMap.class).open();
        for (int i = 0; i < size1; i++) {
            map1.put(i, i + 1);
        }
        System.out.println("done with puts");
        for (int i = 0; i < numQuery; i++) {
            int index = rand.nextInt(size1 - 1);
            map1.get(index);
        }

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