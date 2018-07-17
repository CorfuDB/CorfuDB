package org.corfudb.perfClient;

import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;

public class Driver {

    public static void main(String[] args) throws Exception {
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
        int[] numWrites = {0};
        long[] lastAddress = {0};
        for (int x = 0; x < numProd; x++) {
            final CorfuRuntime rt = rts[x%numRt];
            Runnable r = () -> {
                long time = 0;

                for (int i = 0; i < numReq; i++) {
                    long ts1 = System.currentTimeMillis();

                    TokenResponse tr = rt.getSequencerView().next();
                    Token t = tr.getToken();
                    rt.getAddressSpaceView().write(t, payload);
                    numWrites[0] += 1;

                    long ts2 = System.currentTimeMillis();
                    time += (ts2 - ts1);
                }
                lastAddress[0] += rt.getSequencerView().next().getTokenValue();

                System.out.println("(Producer) Latency [ms/op]: " + ((time*1.0)/numReq));
                System.out.println("(Producer) Throughput [ops/ms]: " + (numReq/(time*1.0)));
            };

            prodThreads[x] = new Thread(r); // performs lambda fn from above
        }

        // Consumer
        System.out.println("Consumer");
        int[] numReads = {0};
        for (int x = 0; x < numCons; x++) {
            final CorfuRuntime rt = rts[x%numRt];
            Runnable r = () -> {
                long localOffset = -1;

                while (localOffset < lastAddress[0]) {
                    TokenResponse tr = rt.getSequencerView().query();
                    long globalOffset = tr.getTokenValue();

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
                            numReads[0] += 1;
                        }
                        localOffset = globalOffset;
                    }
                }
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
        for (int x = 0; x < numCons; x++) {
            consThreads[x].join();
        }

        long ts2 = System.currentTimeMillis();

        System.out.println("\nOverall");
        System.out.println(numWrites[0]);
        System.out.println(numReads[0]);

        System.out.println("Total Time [ms]: " + (ts2 - ts1));
        System.out.println("Num Ops: " + (numWrites[0] + numReads[0]));
        System.out.println("Throughput: " + (((numWrites[0] + numReads[0]) * 1.0) / (ts2 - ts1)));
    }
}