import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;

public class Driver {

    public static void main(String[] args) throws Exception {
        String connString = args[0];
        int numThreads = Integer.valueOf(args[1]);
        final int numRt = Integer.valueOf(args[2]);
        final int numProd = Integer.valueOf(args[3]);
        final int numCons = Integer.valueOf(args[4]);
        final int numReq = Integer.valueOf(args[5]);
        final int payloadSize = Integer.valueOf(args[6]);

        CorfuRuntime[] rts = new CorfuRuntime[numRt];

        for (int x = 0; x < numRt; x++) {
            rts[x] = new CorfuRuntime(connString).connect();
        }

        Thread[] prodThreads = new Thread[numProd];
        Thread[] consThreads = new Thread[numCons];

        // Producer
        for (int x = 0; x < numProd; x++) {
            final CorfuRuntime rt = rts[x%numRt];
            Runnable r = () -> {
                long avg = 0;

                for (int i = 0; i < numReq; i++) {
                    long ts1 = System.nanoTime();

                    TokenResponse tr = rt.getSequencerView().next();
                    Token t = tr.getToken();
                    rt.getAddressSpaceView().write(t, new byte[payloadSize]);

                    long ts2 = System.nanoTime();
                    avg += (ts2 - ts1);
                }

                System.out.println("Producer: Latency [ms/op]: " + ((avg*1.0)/numReq/1000000.0));
            };

            prodThreads[x] = new Thread(r); // performs lambda fn from above
        }

        // Consumer
        for (int x = 0; x < numCons; x++) {
            final CorfuRuntime rt = rts[x%numRt];
            Runnable r = () -> {
                long localOffset = -1;

                while (true) {
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
        // kill all cons threads
        for (int x = 0; x < numCons; x++) {
            consThreads[x].stop();
            consThreads[x].join();
        }

        long ts2 = System.currentTimeMillis();

        System.out.println("Total Time [ms]: " + (ts2 - ts1));
        System.out.println("Num Ops: " + (numThreads * numReq));
        System.out.println("Throughput: " + ((numThreads * numReq * 1.0) / (ts2 - ts1)));
    }
}