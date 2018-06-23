
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.SequencerClient;
import org.corfudb.runtime.view.RuntimeLayout;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class Driver {

    public static void main(String[] args) throws Exception {

        String connString = args[0];
        int numThreads = Integer.valueOf(args[1]);
        final int numOps = Integer.valueOf(args[2]);
        final int numRt = Integer.valueOf(args[3]);

        final int numAsync = 500 * 2;

        CorfuRuntime[] rts = new CorfuRuntime[numRt];

        for (int x = 0; x < numRt; x++) {
            rts[x] = new CorfuRuntime(connString).connect();
        }

        Thread[] threads = new Thread[numThreads];

        for (int x = 0; x < numThreads; x++) {
            final CorfuRuntime rt = rts[x%numRt];

            final SequencerClient client = new RuntimeLayout(rt.getLayoutView().getLayout(),
                    rt).getPrimarySequencerClient();

            Runnable r = () -> {
                long avg = 0;
                int numIter = numOps / numAsync;
                CompletableFuture[] futures = new CompletableFuture[numAsync];
                for (int y = 0; y < numIter; y++) {
                    long ts1 = System.nanoTime();

                    for (int t = 0; t < numAsync; t++) {
                        futures[t] = client.nextToken(Collections.EMPTY_LIST, 0);
                    }

                    for (int t = 0; t < numAsync; t++) {
                        try {
                            futures[t].get();
                        } catch (Exception e) {
                            System.out.println(e);
                            throw new RuntimeException(e);
                        }
                    }

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
