package org.corfudb.samples;

import org.corfudb.runtime.collections.ISMRMap;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionType;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class demonstrates transactions with write-write isolation level.
 *
 * By default, Corfu transaction wrapped between TXBegin()/TXEnd() guarantee serializabilty.
 * The other supported isolation levels are:
 *   - write-write: Check that modification by this transaction do not overwrite concurrent modifications
 *   - snapshot: Always commits, only guarantees execution against a consistent snapshot
 *
 * All transactions execute against a consistent snapshot, but have different conflict resolution criteria.
 * By default, the snapshot time of a transaction is the time of its first object accessor.
 * Corfu supports explicitly designating snapshot time (a log offset, which is returned by object mutators).
 *
 * Below, the utilitiy method {@link WriteWriteTXs ::TXBegin()} illustrates how to replace TXBegin()
 * with a transasction-builder indicating write-write isolation.
 *
 * The utility method {@link WriteWriteTXs ::TXBegin(long snapTime)} illustrates how to additionally set
 * a transasction snapshot-time to a specific position.
 *
 * Created by dalia on 12/30/16.
 */
public class WriteWriteTXs extends BaseCorfuAppUtils {
    /**
     * main() and standard setup methods are deferred to BaseCorfuAppUtils
     * @return
     */
    static BaseCorfuAppUtils selfFactory() { return new WriteWriteTXs(); }
    public static void main(String[] args) { selfFactory().start(args); }

    /**
     * test parameters
     */
    private final int NUM_BATCHES = 1_000;
    private final int BATCH_SZ = 1_000;
    private final int numTasks = NUM_BATCHES * BATCH_SZ;
    private final int TOTAL_PERCENT = 100;

    /**
     * This overrides TXBegin in order to set the transaction type to WRITE_AFTER_WRITE
     */
    @Override
    protected void TXBegin() {
        while (true) {
            try {
                getCorfuRuntime().getObjectsView().TXBuild()
                        .setType(TransactionType.WRITE_AFTER_WRITE)
                        .begin();
                break;
            } catch (NetworkException ne) {
                getCorfuRuntime().invalidateLayout();
            }
        }
    }

    /**
     * This variant of TXBegin sets a snapshot-point,
     * as well as sets the transaction type to WRITE_AFTER_WRITE
     */
    protected void TXBegin(long snapTime) {
        getCorfuRuntime().getObjectsView().TXBuild()
                .setSnapshot(snapTime)
                .setType(TransactionType.WRITE_AFTER_WRITE)
                .begin();
    }


    /**
     * This is where activity is started
     */
    @Override
    void action() {

        final int NUM_THREADS = 20;
        final long THREAD_TIMEOUT = 300_000; // 5 minutes
        final int READ_PERCENT = 80;

        ArrayList<Thread> tList = new ArrayList<>();

        // generate the maps and populate with elements
        generateMaps();

        // first workload:

        // start NUM_THREADS threads and trigger activity in each
        for (int t = 0; t < NUM_THREADS; t++) {
            final int threadNum = t;
            //disabled.
          //  tList.add(new Thread(() -> mixedReadWriteLoad2(threadNum,
          //          READ_PERCENT)));
//            tList.get(t).start();
        }

        tList.forEach(th -> {
            try {
                th.join(THREAD_TIMEOUT);
            } catch (InterruptedException ie) {
                System.out.println("Thread timeout!");
            }
        });

        // second workload:
       tList.clear();

        // start NUM_THREADS threads and trigger activity in each
        for (int t = 0; t < NUM_THREADS; t++) {
            final int threadNum = t;
            tList.add(new Thread(() -> mixedReadWriteLoad1(threadNum, READ_PERCENT)));
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
    void mixedReadWriteLoad1(int threadNum, int readPrecent) {
        System.out.print("running mixedRWload1..");
        long startt = System.currentTimeMillis();
        AtomicInteger aborts = new AtomicInteger(0);
        AtomicInteger conflictAborts = new AtomicInteger(0);
        AtomicInteger newSeqAborts = new AtomicInteger(0);

        for (int i = 0; i < NUM_BATCHES; i++) {
//            System.out.print(".");
            long s1 = System.currentTimeMillis();
            TXBegin();
//            System.out.println(threadNum + ": TX #" + i + " starting in epoch : " + getCorfuRuntime().getLayoutView().getLayout().getEpoch());
            int accumulator = 0;
            for (int j = 0; j < BATCH_SZ; j++) {

                while (true) {
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
//                            System.out.println(threadNum +
//                                    ": exception on iteration " + i + "," + j
//                                    + ", r=" + r1 + "," + r2 + "," + r3 + "->" + e.getMessage());
                            getCorfuRuntime().invalidateLayout();
                            continue;
                        }
                    }

                    // otherwise, perform a random put()
                    else {
                        try {
                            if (rand.nextInt(2) == 1)
                                map2.put("m2" + rand.nextInt(numTasks), accumulator);
                            else
                                map3.put("m3" + rand.nextInt(numTasks), accumulator);
                        } catch (Exception e) {
//                            System.out.println(threadNum +
//                                    ": exception on iteration " + i + "," + j + "->" + e.getMessage());
                            getCorfuRuntime().invalidateLayout();
                            continue;
                        }
                    }
                    break;
                }
            }

            while (true) {
                try {
//                    System.out.println(threadNum + ": TX #" + i + " ending in epoch : " + getCorfuRuntime().getLayoutView().getLayout().getEpoch());
                    TXEnd();
                    break;
                } catch (NetworkException ne) {
                    getCorfuRuntime().invalidateLayout();
                } catch (TransactionAbortedException te) {
//                    System.out.println(threadNum + ": TX Abort #" + i + " = " + te);
                    aborts.getAndIncrement();
                    if (te.getAbortCause().equals(AbortCause.CONFLICT)) conflictAborts.getAndIncrement();
                    else if (te.getAbortCause().equals(AbortCause.NEW_SEQUENCER)) newSeqAborts.getAndIncrement();
                    break;
                }
            }
            long e1 = System.currentTimeMillis();
            System.out.println(threadNum + ": [TX: #" + i + "\t]\t" + (e1-s1) );
        }
        System.out.println("END");

        long endt = System.currentTimeMillis();
        System.out.println(threadNum
                + ": mixedReadWriteLoad elapsed time (msecs): " + (endt - startt));
        System.out.println(threadNum
                + ":   #aborts/#TXs: " + aborts.get() + "/" + numTasks);
        System.out.println(threadNum
                + ":   #conflictAborts/#TXs: " + conflictAborts.get() + "/" + numTasks);
        System.out.println(threadNum
                + ":   #newSeqAborts/#TXs: " + newSeqAborts.get() + "/" + numTasks);
    }

    /**
     * This is a similar workload to mixedReadWriteLoad1, with an important optimization.
     *
     * All the transactions indicate an explicit snapshot timestamp to read from.
     * This prevents concurrent threads from flip-flopping between snapshots.
     * In order to prevent a transaction from viewing stale snapshots, transactions touch the entries they get().
     * This causes abort if a transaction get()'s an entry which is modified by another.
     *
     * @param readPrecent ratio of reads (to 100)
     */
    // This sample is currently not working, because snapshots for non read-only
    // transactions don't work.
    /*
    void mixedReadWriteLoad2(int threadNum, int readPrecent) {
        System.out.print("running mixedRWload2..");
        long startt = System.currentTimeMillis();
        AtomicInteger aborts = new AtomicInteger(0);

        // obtain the current snapshot time, before any transaction activity starts
        long snapTime = getCorfuRuntime()
                .getStreamsView()
                .get(getCorfuRuntime().getStreamID("A"))
                .;

        for (int i = 0; i < NUM_BATCHES; i++) synchronized (map1) {
            System.out.print(".");
            TXBegin(snapTime);          // use the snapshot we kept as the starting snapshot
            int accumulator = 0;
            for (int j = 0; j < BATCH_SZ; j++) {

                // perform random get()'s with probability 'readPercent/100'
                if (rand.nextInt(TOTAL_PERCENT) < readPrecent) {
                    int r1 = rand.nextInt(numTasks);
                    int r2 = rand.nextInt(numTasks);
                    int r3 = rand.nextInt(numTasks);
                    try {
//                        accumulator += map1.computeIfPresent("m1" + r1, (K,V) -> V);
//                        accumulator += map2.computeIfPresent("m2" + r2, (K,V) -> V);
//                        accumulator += map3.computeIfPresent("m3" + r3, (K,V) -> V);
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
    */

}
