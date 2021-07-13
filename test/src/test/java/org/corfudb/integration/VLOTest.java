package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@SuppressWarnings("checkstyle:magicnumber")
public class VLOTest {

    private final String streamName = "VLO-Test-Stream";
    private final String keyPrefix = "entry_";

    private static final Random rand = new Random();

    @Test
    public void testVLO() throws Exception {
        // Configuration
        final int numRuntimes = 10;
        final String corfuServerEndpoint = "localhost:9999";
        final int numWorkersPerManager = 50;
        final int opsPerWorker = 500;

        final CorfuRuntime[] runtimes = new CorfuRuntime[numRuntimes];
        for (int x = 0; x < numRuntimes; x++) {
            runtimes[x] = new CorfuRuntime(corfuServerEndpoint).connect();
        }

        final CorfuRuntime rt = runtimes[0];
        CorfuTable<String, Integer> table = rt.getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                })
                .setStreamName(streamName)
                .open();

        // Populate default values
        rt.getObjectsView().TXBegin();
        for (int x = 0; x < numRuntimes; x ++) {
            table.put(keyPrefix + x, 0);
        }
        rt.getObjectsView().TXEnd();

        // Launch worker managers
        ExecutorService workerManagers = Executors.newFixedThreadPool(numRuntimes,
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat("WorkerManager-%d")
                        .build());

        WorkerManager [] managers = new WorkerManager[numRuntimes];

        for (int x = 0; x < numRuntimes; x++) {
            managers[x] = new WorkerManager(runtimes[x], numWorkersPerManager, opsPerWorker, x);
            workerManagers.submit(managers[x]);
        }

        workerManagers.shutdown();
        workerManagers.awaitTermination(5 * 4, TimeUnit.MINUTES);

        Integer [] values = new Integer[numRuntimes];

        for (int x = 0; x < numRuntimes; x++) {
            values[x] = table.get(keyPrefix + x);
        }

        for (int x = 0; x < numRuntimes; x++) {
            log.info("key={} val={} cleared={}", keyPrefix + x, values[x], managers[x].getTotalCleared());
            if (values[x] != (numWorkersPerManager * opsPerWorker - managers[x].getTotalCleared())) {
                throw new IllegalStateException("FAILED");
            }
        }
    }

    class Worker implements Runnable {
        private final CorfuRuntime rt;
        private final CorfuTable<String, Integer> table;
        private final String key;
        private int ops;

        @Getter
        private int totalCleared;

        public Worker(final CorfuRuntime rt, final String key, final int ops) {
            this.rt = rt;
            this.key = key;
            this.ops = ops;
            this.totalCleared = 0;

            this.table = this.rt.getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, Integer>>() {
                    })
                    .setStreamName(streamName)
                    .open();
        }

        @Override
        public void run() {
            log.info("Worker starting");
            final int clearOffset = rand.nextInt(49) + 2; // Generate a random number between [2, 50]

            while (ops > 0) {
                try {
                    rt.getObjectsView().TXBegin();
                    Integer v = table.get(key); // Assume key is present
                    if (ops % clearOffset == 0) {
                        table.delete(key);
                        table.put(key, 1);
                        rt.getObjectsView().TXEnd();
                        ops--;
                        totalCleared += v;
                    } else {
                        table.put(key, v + 1);
                        rt.getObjectsView().TXEnd();
                        ops--;
                    }
                } catch (TransactionAbortedException ignored) {
                } catch (Exception ex) {
                    log.warn("Exception during transaction", ex);
                }
            }

            log.info("Worker ending");
        }
    }

    class WorkerManager implements Runnable {
        private final CorfuRuntime rt;
        private final ExecutorService executor;
        private final int numWorkers;
        private final int numOpsPerWorker;
        private final int keyIndex;

        @Getter
        private int totalCleared;

        public WorkerManager(final CorfuRuntime rt, final int numWorkers, final int numOpsPerWorker, final int id) {
            this.rt = rt;
            this.numWorkers = numWorkers;
            this.numOpsPerWorker = numOpsPerWorker;
            this.keyIndex = id;
            this.totalCleared = 0;

            this.executor = Executors.newFixedThreadPool(numWorkers + 1,
                    new ThreadFactoryBuilder()
                            .setNameFormat("worker-thread_" + id + "-%d")
                            .build());
        }

        @SneakyThrows
        @Override
        public void run() {
            log.info("WorkerManager launching workers");
            Worker [] workers = new Worker[numWorkers];

            for (int x = 0; x < numWorkers; x++) {
                workers[x] = new Worker(rt, keyPrefix + keyIndex, numOpsPerWorker);
                executor.submit(workers[x]);
            }

            log.info("WorkerManager waiting for workers to finish");
            executor.shutdown();
            executor.awaitTermination(5 * 3, TimeUnit.MINUTES);
            log.info("WorkerManager done");

            for (int x = 0; x < numWorkers; x++) {
                totalCleared += workers[x].getTotalCleared();
            }
        }
    }
}
