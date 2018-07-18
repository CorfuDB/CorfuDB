package org.corfudb.perfClient;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;

public class Driver {

    public static void main(String[] args) throws Exception {
        // use cons prod APIs now instead of all this other stuff

        String connString = args[0];
        final int numRt = Integer.valueOf(args[1]);
        final int numProd = Integer.valueOf(args[2]);
        final int numCons = Integer.valueOf(args[3]);
        final int numReq = Integer.valueOf(args[4]);
        final int payloadSize = Integer.valueOf(args[5]);

        CorfuRuntime[] rts = new CorfuRuntime[numRt];

        for (int x = 0; x < numRt; x++) {
            rts[x] = new CorfuRuntime(connString).connect();
        }

        Thread[] prodThreads = new Thread[numProd];
        Thread[] consThreads = new Thread[numCons];

        byte[] payload = new byte[payloadSize];

        // Producer
        System.out.println("Producer");
        int[] numWrites = new int[numProd];
        long[] lastAddress = new long[numProd];
        boolean[] completed = {false};
        for (int x = 0; x < numProd; x++) {
            final CorfuRuntime rt = rts[x%numRt];
            final int ind = x;
            Runnable r = () -> {
                System.out.println("[p] x: " + (ind)); // should be diff for all threads

                long time = 0;

                for (int i = 0; i < numReq; i++) {
                    long ts1 = System.currentTimeMillis();

                    TokenResponse tr = rt.getSequencerView().next();
                    Token t = tr.getToken();
                    rt.getAddressSpaceView().write(t, payload);
                    lastAddress[ind] = t.getTokenValue(); // this is the new last address
                    numWrites[ind] += 1;

                    long ts2 = System.currentTimeMillis();
                    time += (ts2 - ts1);
                }

                System.out.println("(Producer) Latency [ms/op]: " + ((time*1.0)/numReq)
                        + " " + ((time*1.0)/numWrites[ind]));
                System.out.println("(Producer) Throughput [ops/ms]: " + (numReq/(time*1.0))
                        + " " + (numWrites[ind])/(time*1.0));
                System.out.println("Num Writes: " + numWrites[ind]);
            };

            prodThreads[x] = new Thread(r); // performs lambda fn from above
        }

        // Consumer
        System.out.println("Consumer");
        int[] numReads = new int[numCons];
        for (int x = 0; x < numCons; x++) {
            final CorfuRuntime rt = rts[x%numRt];
            final int ind = x;
            Runnable r = () -> {
                System.out.println("[c] x: " + (ind)); // should be diff for all threads

                long time = 0;
                long localOffset = -1;

                boolean lastTime = false;

                while (!completed[0] || lastTime) {
                    long ts1 = System.currentTimeMillis();

                    long maxAddress = 0;
                    for (int i = 0; i < numProd; i++) {
                        if (lastAddress[i] > maxAddress) {
                            maxAddress = lastAddress[i];
                        }
                    }

                    long globalOffset = maxAddress;
                    long delta = globalOffset - localOffset;

                    if (delta == 0) {
                        try {
                            Thread.sleep(1000);
                        } catch (Exception e) {
                            System.out.println("Error: could not sleep!");
                        }
                    } else {
                        for (long i = localOffset + 1; i <= globalOffset; i++) {
                            rt.getAddressSpaceView().read(i);
                            numReads[ind] += 1;
                        }
                        localOffset = globalOffset;
                    }

                    long ts2 = System.currentTimeMillis();
                    time += (ts2 - ts1);

                    if (lastTime) {
                        lastTime = false;
                    } else if (completed[0]) {
                        lastTime = true;
                    }
                }

                System.out.println("(Consumer) Latency [ms/op]: " + ((time*1.0)/numReads[ind]));
                System.out.println("(Consumer) Throughput [ops/ms]: " + (numReads[ind]/(time*1.0)));
                System.out.println("Num Reads: " + numReads[ind]);
            };

            consThreads[x] = new Thread(r); // performs lambda fn from above
        }

        long ts1 = System.currentTimeMillis();

        for (int x = 0; x < numProd; x++) {
            prodThreads[x].start();
        }
        for (int x = 0; x < numCons; x++) {
            consThreads[x].start();
        }
        for (int x = 0; x < numProd; x++) {
            prodThreads[x].join();
        }

        // set last address here (do the .query)
        completed[0] = true;

        for (int x = 0; x < numCons; x++) {
            consThreads[x].join();
        }

        long ts2 = System.currentTimeMillis();

        int totalWrites = 0;
        for (int i = 0; i < numProd; i++) {
            totalWrites += numWrites[i];
        }
        int totalReads = 0;
        for (int i = 0; i < numCons; i++) {
            totalReads += numReads[i];
        }

        System.out.println("\nOverall");
        System.out.println(totalWrites);
        System.out.println(totalReads);

        System.out.println("Total Time [ms]: " + (ts2 - ts1));
        System.out.println("Num Ops: " + (totalWrites + totalReads));
        System.out.println("Throughput: " + (((totalWrites + totalReads)*1.0) / (ts2 - ts1)));
    }
}