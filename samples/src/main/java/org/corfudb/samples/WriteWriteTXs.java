package org.corfudb.samples;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.collections.ISMRMap;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import org.corfudb.runtime.object.transactions.TransactionType;

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
 * Below, the utility method {@link WriteWriteTXs ::TXBegin()} illustrates how to replace TXBegin()
 * with a transaction-builder indicating write-write isolation.
 *
 * The utility method {@link WriteWriteTXs ::TXBegin(long snapTime)} illustrates how to additionally set
 * a transaction snapshot-time to a specific position.
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
    private final int NUM_BATCHES = 10;
    private final int BATCH_SZ = 1_000;
    private final int numTasks = NUM_BATCHES * BATCH_SZ;
    private final int TOTAL_PERCENT = 100;

    /**
     * This overrides TXBegin in order to set the transaction type to WRITE_AFTER_WRITE
     */
    @Override
    protected void TXBegin() {
        getCorfuRuntime().getObjectsView().TXBuild()
                .type(TransactionType.WRITE_AFTER_WRITE)
                .build()
                .begin();
    }

    /**
     * This is where activity is started
     */
    @Override
    @SuppressWarnings("checkstyle:printLine") // Sample code
    void action() {

        final int NUM_THREADS = 2;
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
            tList.get(t).start();
        }

        tList.forEach(th -> {
            try {
                th.join(THREAD_TIMEOUT);
            } catch (InterruptedException ie) {
                System.out.println("Thread timeout!");
                throw new UnrecoverableCorfuInterruptedError(ie);
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
                throw new UnrecoverableCorfuInterruptedError(ie);
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
    final Random rand = new Random(SEED);

    /**
     * This method initiates all the data structures for this program
     */
    @SuppressWarnings({"checkstyle:printLine", "checkstyle:print"}) // Sample code
    void generateMaps() {
        /**
         * Instantiate three streams with three SMRmap objects
         */
        map1 = instantiateCorfuObject(new TypeToken<SMRMap<String, Integer>>() {}, "A");
        map2 = instantiateCorfuObject(new TypeToken<SMRMap<String, Integer>>() {}, "B");
        map3 = instantiateCorfuObject(new TypeToken<SMRMap<String, Integer>>() {}, "C");

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
     * @param readPercent ratio of reads (to 100)
     */
    @SuppressWarnings({"checkstyle:printLine", "checkstyle:print"}) // Sample code
    void mixedReadWriteLoad1(int threadNum, int readPercent) {
        System.out.print("running mixedRWload1..");
        long startt = System.currentTimeMillis();
        AtomicInteger aborts = new AtomicInteger(0);

        for (int i = 0; i < NUM_BATCHES; i++) {
            System.out.print(".");
            TXBegin();
            int accumulator = 0;
            for (int j = 0; j < BATCH_SZ; j++) {

                // perform random get()'s with probability 'readPercent/100'
                if (rand.nextInt(TOTAL_PERCENT) < readPercent) {
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

//    /**
//     * This is a similar workload to mixedReadWriteLoad1, with an important optimization.
//     *
//     * All the transactions indicate an explicit snapshot timestamp to read from.
//     * This prevents concurrent threads from flip-flopping between snapshots.
//     * In order to prevent a transaction from viewing stale snapshots, transactions touch the entries they get().
//     * This causes abort if a transaction get()'s an entry which is modified by another.
//     *
//     * @param readPercent ratio of reads (to 100)
//     */
    // This sample is currently not working, because snapshots for non read-only
    // transactions don't work.
    /*
    void mixedReadWriteLoad2(int threadNum, int readPercent) {
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
                if (rand.nextInt(TOTAL_PERCENT) < readPercent) {
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
