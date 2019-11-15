package org.corfudb.samples;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.collections.SMRMap;

/**
 * Sometimes developers may group mutator operations into transactions for performance reasons.
 *
 * This program runs two such workloads.
 *
 * Workload 1:
 * This workload populates a large map with entries, in two ways,
 *   - default, where one mutation is dumped to the log
 *   - in batches, each batch is a "transaction", dumped to the log as an indivisible entry.
 *
 * Running on my macbook, the first loop takes (roughly) 57 seconds, the second one 5 seconds.
 *
 * The reason is  that internally, the Corfu corfuRuntime sends updates to the Corfu log to persist.
 * When an application performs a bunch of individual updates, they are persisted one at a time,
 * each in a separate log entry.
 *
 * In a transaction, the corfuRuntime waits until the transaction end and persists one (large) log entry
 * for the entire transaction. Writing one large entry takes much less time than many small ones.
 *
 * Workload 2:
 *
 * This workload also populates map entries in transaction batches. However, it works over a collection of maps.
 * Transactions perform updates to distinct streams.
 *
 * Running again on a single mac-book, it takes (roughly) 59 seconds.
 * The reason is that, despite the transaction batching, each stream needs to persist the transaction independently.
 * So the load is the same as performing individual updates, one by one.
 *
 * ======
 *
 *  Note: For either workload, this sort of batching must be done with care,
 *  to avoid causing false-sharing and inflicting aborts.
 *
 * Created by dalia on 12/30/16.
 */
public class BatchedWriteTransaction extends BaseCorfuAppUtils {
    /**
     * main() and standard setup methods are deferred to BaseCorfuAppUtils
     * @return
     */
    static BaseCorfuAppUtils selfFactory() { return new BatchedWriteTransaction(); }
    public static void main(String[] args) { selfFactory().start(args); }

    private final int numBatches = 100;
    private final int batchSize = 1_000;
    private final int numTasks = numBatches * batchSize;

    /**
     * this method initiates activity
     */
    @Override
    void action() {
        workload1();
        workload2();
    }

    @SuppressWarnings("checkstyle:printLine") // Sample code
    void workload1() {
        /**
         * Instantiate a Corfu Stream named "A" dedicated to an SMRmap object
         */
        Map<String, Integer> map = getCorfuRuntime().getObjectsView()
                .build()
                .setStreamName("A")     // stream name
                .setTypeToken(new TypeToken<SMRMap<String, Integer>>() {})
                .open();                // instantiate the object!

        // populate map: sequentially
        long startt = System.currentTimeMillis();
        for (int i = 0; i < numTasks; i++) {
            map.put("k1" + i, i);
        }
        long endt = System.currentTimeMillis();
        System.out.println("time to populate map sequentially (msecs): " + (endt - startt) );

        // populate map: in batched transactions
        startt = System.currentTimeMillis();
        for (int i = 0; i < numBatches; i++) {
            getCorfuRuntime().getObjectsView().TXBegin();
            for (int j = 0; j < batchSize; j++)
                map.put("k2" + (i * batchSize + j), i);
            getCorfuRuntime().getObjectsView().TXEnd();
        }
        endt = System.currentTimeMillis();
        System.out.println("time to populate map in batches (msecs): " + (endt - startt));
    }

    @SuppressWarnings("checkstyle:printLine") // Sample code
    void workload2() {
        /**
         * Instantiate a collection of Corfu streams, each one dedicated to an SMRmap instance
         */
        List<Map<String, Integer>> maparray = new ArrayList<Map<String, Integer>>(numBatches);
        for (int m = 0; m < numBatches; m++) {
            maparray.add(m,
                    getCorfuRuntime().getObjectsView()
                            .build()
                            .setStreamName("C" + m)
                            .setTypeToken(new TypeToken<SMRMap<String, Integer>>() {})
                            .open()
            );
        }

        long startt = System.currentTimeMillis();
        for (int i = 0; i < batchSize; i++) {
            getCorfuRuntime().getObjectsView().TXBegin();
            for (int j = 0; j < numBatches; j++)
                maparray.get(j).put("k2"+(i*numBatches + j), i);
            getCorfuRuntime().getObjectsView().TXEnd();
        }
        long endt = System.currentTimeMillis();
        System.out.println("time to populate map-array in batches (msecs): " + (endt-startt));

    }
}


