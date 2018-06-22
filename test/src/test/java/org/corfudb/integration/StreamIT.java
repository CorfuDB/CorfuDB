package org.corfudb.integration;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.WriteSizeException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A set integration tests that exercise the stream API.
 */

public class StreamIT{

    @Test
    public void largeStreamWrite() throws Exception {

        String connString = "localhost:9000";
        int numThreads = 128;
        final int numOps = 100_000;
        final int numRt = 3;

        CorfuRuntime[] rts = new CorfuRuntime[numRt];

        for (int x = 0; x < numRt; x++) {
            rts[x] = new CorfuRuntime(connString).connect();
        }

        Thread[] threads = new Thread[numThreads];

        for (int x = 0; x < numThreads; x++) {
            final CorfuRuntime rt = rts[x%numRt];
            Runnable r = () -> {
                long avg = 0;
                for (int y = 0; y < numOps; y++) {
                    long ts1 = System.nanoTime();
                    rt.getSequencerView().query();
                    long ts2 = System.nanoTime();
                    avg += (ts2 - ts1);
                }

                System.out.println("latency ms/op " + ((avg*1.0)/numOps/1000000));
            };

            threads[x] = new Thread(r);
        }


        long ts1 = System.currentTimeMillis();

        for (int x = 0; x < numThreads; x++) {
            threads[x].start();
        }

        for (int x = 0; x < numThreads; x++) {
            threads[x].join();
        }


        long ts2 = System.currentTimeMillis();

        System.out.println("Total time: " + (ts2 - ts1));
        System.out.println("Num Ops: " + (numThreads * numOps));
        System.out.println("Throughput: " + ((numThreads * numOps) / (ts2 - ts1)));
    }
}
