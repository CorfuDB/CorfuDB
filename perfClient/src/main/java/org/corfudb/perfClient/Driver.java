package org.corfudb.perfClient;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.BootstrapUtil;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.*;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
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
                    producer.send(payload, 1);
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

    public static Layout deserializeLayout(List<String> hosts) {
        String layout = "{\n" +
                "  \"layoutServers\": [\n" +
                "    \"10.197.4.69:9001\",\n" +
                "    \"10.197.4.68:9002\"\n" +
                "  ],\n" +
                "  \"sequencers\": [\n" +
                "    \"10.33.82.253:9000\"\n" +
                "  ],\n" +
                "  \"segments\": [\n" +
                "    {\n" +
                "      \"replicationMode\": \"CHAIN_REPLICATION\",\n" +
                "      \"start\": 0,\n" +
                "      \"end\": -1,\n" +
                "      \"stripes\": [\n" +
                "        {\n" +
                "          \"logServers\": [\n" +
                "            \"10.197.4.69:9001\"\n" +
                "          ]\n" +
                "        },\n" +
                "        {\n" +
                "          \"logServers\": [\n" +
                "            \"10.197.4.68:9002\"\n" +
                "          ]\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ],\n" +
                "  \"unresponsiveServers\": [],\n" +
                "  \"epoch\": 0,\n" +
                "  \"clusterId\": \"2c3f91d4-663f-46e5-9f2d-488f035f29a5\"\n" +
                "}";

        Gson parser = new GsonBuilder()
                .registerTypeAdapter(Layout.class, new LayoutDeserializer())
                .create();

        Layout layoutCopy = parser.fromJson(layout, Layout.class);

        return layoutCopy;
    }

    public static void bootstrapClusterWithStrings(List<String> hosts) {
        Layout layout = deserializeLayout(hosts);
        Duration TIMEOUT_SHORT = Duration.of(5, ChronoUnit.SECONDS);
        Duration.of(1, ChronoUnit.SECONDS);
        final int retries = 3;
        System.out.println("Layout: " + (layout));

        BootstrapUtil.bootstrap(layout, retries, TIMEOUT_SHORT);
    }


    public static void runClusterWrites(String[] args, String prodClass) throws Exception {
        // args: localhost 300 100 100
        // new args: numHosts, [each host], numRts, numProd, numReq, numAsync, payloadSize, waitTime (sec), callBootstrapper

        final int numHosts = Integer.valueOf(args[0]);
        List<String> hosts = new ArrayList<>();
        for (int i = 0; i < numHosts; i++) {
            hosts.add(args[i + 1]);
        }
        final int numRts = Integer.valueOf(args[numHosts + 1]);
        final int numProd = Integer.valueOf(args[numHosts + 2]);
        final int numReq = Integer.valueOf(args[numHosts + 3]);
        final int numAsync = Integer.valueOf(args[numHosts + 4]);
        final int payloadSize = Integer.valueOf(args[numHosts + 5]);
        final int waitTime = Integer.valueOf(args[numHosts + 6]);
        final boolean bootstrap = Boolean.valueOf(args[numHosts + 7]);
        final UUID streamID = UUID.randomUUID();

        Thread[] prodThreads = new Thread[numProd];

        byte[] payload = new byte[payloadSize];

        long preBootstrap = System.currentTimeMillis();

        // call bootstrapper - currently hardcoded values of hosts
        if (bootstrap) {
            bootstrapClusterWithStrings(hosts);
            System.out.println("Bootstrapped!");
        }

        // create runtimes
        final CorfuRuntime[] rts = new CorfuRuntime[numRts];
        for (int i = 0; i < numRts; i++) {
            rts[i] = new CorfuRuntime(hosts.get(i % numHosts)).connect();
            rts[i].setCacheDisabled(true);
            System.out.println("Connected! " + (hosts.get(i % numHosts)));
        }

        // wait for all drivers to be connected
        long diff = (preBootstrap + waitTime * 1000) - System.currentTimeMillis();
        if (diff > 0) {
            Thread.sleep(diff);
        }
        System.out.println("Done sleeping, awakening now...");

        // Producer
        int[] numWrites = new int[numProd];
        for (int x = 0; x < numProd; x++) {
            final CorfuRuntime rt = rts[x % numRts];
            final int ind = x;

            Runnable r = () -> {
                Producer producer = null;
                if (prodClass.equals("AddressSpaceProducer")) {
                    producer = new AddressSpaceProducer(rt);
                } else if (prodClass.equals("StreamsProducer")) {
                    producer = new StreamsProducer(rt, streamID);
                }

                for (int i = 0; i < numReq / numAsync; i++) {
                    producer.send(payload, numAsync);
                    numWrites[ind] += numAsync;
                }
            };

            prodThreads[x] = new Thread(r); // performs lambda fn from above
        }

        long startTime = System.currentTimeMillis();

        for (int x = 0; x < numProd; x++) {
            prodThreads[x].start();
        }

        for (int x = 0; x < numProd; x++) {
            prodThreads[x].join();
        }

        long endTime = System.currentTimeMillis();
        long time = endTime - startTime;

        int totalWrites = 0;
        for (int i = 0; i < numProd; i++) {
            totalWrites += numWrites[i];
        }

        System.out.println("\nOverall");
        System.out.println("Total Writes: " + (totalWrites));
        System.out.println("Total Time [ms]: " + (time));
        System.out.println("Throughput: " + ((totalWrites*1.0) / (time)));
    }

    public static void main(String[] args) throws Exception {
        //runProducerConsumer(args, "AddressSpaceProducer", "AddressSpaceConsumer");
        //runProducerConsumer(args, "StreamsProducer", "StreamConsumer");
        runClusterWrites(args, "AddressSpaceProducer");
    }
}