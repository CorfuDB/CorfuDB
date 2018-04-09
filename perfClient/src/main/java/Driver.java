
import org.corfudb.runtime.CorfuRuntime;

import java.util.Collections;

public class Driver {

    public static void main(String[] args) throws Exception {

        String connString = args[0];
        int numThreads = Integer.valueOf(args[1]);
        final int numOps = Integer.valueOf(args[2]);
        final int numRt = Integer.valueOf(args[3]);

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
                    rt.getSequencerView().nextToken(Collections.emptySet(), 0);
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
