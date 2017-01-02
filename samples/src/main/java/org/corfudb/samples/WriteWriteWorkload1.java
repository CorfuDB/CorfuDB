package org.corfudb.samples;

import com.sun.org.apache.regexp.internal.RE;
import org.corfudb.runtime.collections.ISMRMap;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionType;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by dalia on 12/30/16.
 */
public class WriteWriteWorkload1 extends BasicCorfuProgram {
    /**
     * main() and standard setup methods are deferred to BasicCorfuProgram
     * @return
     */
    static BasicCorfuProgram selfFactory() { return new WriteWriteWorkload1(); }
    public static void main(String[] args) { selfFactory().start(args); }

    private final int NUM_BATCHES = 100;
    private final int BATCH_SZ = 1_000;
    private final int numTasks = NUM_BATCHES * BATCH_SZ;

    final int TOTAL_PERCENT = 100;

    /**
     * This overrides TXBegin in order to set the transaction type to WRITE_AFTER_WRITE
     */
    @Override
    protected void TXBegin() {
        getCorfuRuntime().getObjectsView().TXBuild()
                .setType(TransactionType.WRITE_AFTER_WRITE)
                .begin();
    }

    /**
     * This is where activity is started
     */
    @Override
    void action() {

        final int NUM_THREADS = 3;
        final long THREAD_TIMEOUT = 300_000; // 5 minutes
        final int READ_PERCENT = 80;

        ArrayList<Thread> tList = new ArrayList<>();

        // generate the maps and populate with elements
        generateMaps();

        // start NUM_THREADS threads and trigger activity in each
        for (int t = 0; t < NUM_THREADS; t++) {
            final int threadNum = t;
            tList.add(new Thread(() -> mixedReadWriteLoad(threadNum, READ_PERCENT)));
            tList.get(t).start();
        }

        tList.forEach(th -> {
            try {
                th.join(THREAD_TIMEOUT);
            } catch (InterruptedException ie) {
                System.out.println("Thread timeout!");
            }
        });
    }

    /**
     * This workload operates over three distinct maps
     */
    ISMRMap<String, Integer> map1, map2, map3;

    /**
     * Set up a repeatable PRNG
     */
    final int SEED = 434343;
    Random rand = new Random(SEED);

    /**
     * This method initiates all the data structures for this program
     */
    void generateMaps() {
        /**
         * Instantiate three streams with three SMRmap objects
         */
        map1 = instantiateCorfuObject(SMRMap.class, "A");
        map2 = instantiateCorfuObject(SMRMap.class, "B");
        map3 = instantiateCorfuObject(SMRMap.class, "C");

        // populate maps
        System.out.print("generating maps..");
        for (int i = 0; i < NUM_BATCHES; i++) {
            System.out.print(".");
            TXBegin();
            for (int j = 0; j < BATCH_SZ; j++) {
                map1.put("m1" + (i * BATCH_SZ + j), i);
                map2.put("m2" + (i * BATCH_SZ + j), i);
                map3.put("m3" + (i * BATCH_SZ + j), i);
            }
            TXEnd();
        }
        System.out.println("END");
    }

    /**
     * generate a workload with mixed R/W TX's
     *   - NUM_BATCHES TX's
     *   - each TX performs BATCH_SZ operations, mixing R/W according to the specified ratio.
     *
     * @param readPrecent ratio of reads (to 100)
     */
    void mixedReadWriteLoad(int threadNum, int readPrecent) {
        System.out.print("running mixedRWload..");
        long startt = System.currentTimeMillis();
        AtomicInteger aborts = new AtomicInteger(0);
       // java.util.concurrent.ThreadLocalRandom rand = java.util.concurrent.ThreadLocalRandom.current();

        for (int i = 0; i < NUM_BATCHES; i++) {
            System.out.print(".");
            TXBegin();
            int accumulator = 0;
            for (int j = 0; j < BATCH_SZ; j++) {

                // perform random get()'s with probability 'readPercent/100'
                if (rand.nextInt(TOTAL_PERCENT) < readPrecent) {
                    int r1 = rand.nextInt(numTasks);
                    int r2 = rand.nextInt(numTasks);
                    int r3 = rand.nextInt(numTasks);
                    try {
                        accumulator += map1.get("m1" + r1);
                        accumulator += map2.get("m2" + r2);
                        accumulator += map3.get("m3" + r3);
                    } catch (Exception e) {
                        System.out.println(threadNum +
                                ": exception on iteration " + i + "," + j
                                + ", r=" + r1 + "," + r2 + "," + r3);
                        e.printStackTrace();
                        return;
                    }
                }

                // otherwise, perform a random put()
                else {
                    if (rand.nextInt(2) == 1)
                        map2.put("m2" + rand.nextInt(numTasks), accumulator);
                    else
                        map3.put("m3" + rand.nextInt(numTasks), accumulator);
                }
            }

            try {
                TXEnd();
            } catch (TransactionAbortedException te) {
                aborts.getAndIncrement();
            }
        }
        System.out.println("END");

        long endt = System.currentTimeMillis();
        System.out.println(threadNum
                + ": mixedReadWriteLoad elapsed time (msecs): " + (endt - startt));
        System.out.println(threadNum
                + ":   #aborts/#TXs: " + aborts.get() + "/" + numTasks);
    }
}
