package org.corfudb.benchmark;

import com.google.common.reflect.TypeToken;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * This class is the super class for all benchmark tests.
 * It set test parameters like how many clients to run, how many threads each client run,
 * and how many request one thread send.
 */
@Slf4j
public class BenchmarkTest {
    /**
     * Number of threads per client.
     */
    protected int numThreads;
    protected Runtimes runtimes;

    private final BlockingQueue<Operation> operationQueue;
    private final ExecutorService taskProducer;
    private final ExecutorService workers;
    final ScheduledExecutorService producerScheduler;

    private final long DURATION_IN_MS = 1000;
    static final int APPLICATION_TIMEOUT_IN_MS = 10000000;
    static final int QUEUE_CAPACITY = 1000000;

    BenchmarkTest(ParseArgs parseArgs) {
        runtimes = new Runtimes(parseArgs.getNumRuntimes(), parseArgs.getEndpoint());
        log.info("Connected {} runtimes...", parseArgs.getNumRuntimes());
        numThreads = parseArgs.getNumThreads();

        operationQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        taskProducer = Executors.newSingleThreadExecutor();
        workers = Executors.newFixedThreadPool(numThreads);
        producerScheduler = Executors.newScheduledThreadPool(1);
    }

    /**
     * producer puts one operation into operationQueue.
     * Operation can be Sequencer operation, CorfuTable operation, etc.
     * @param operation the operation pushed into operationQueue
     */
    void runProducer(Operation operation) {
        Runnable task = () -> {
            try {
                operationQueue.put(operation);
            } catch (InterruptedException e) {
                throw new UnrecoverableCorfuInterruptedError(e);
            } catch (Exception e) {
                log.error("operation error", e);
            }
        };
        taskProducer.execute(task);
        //producerScheduler.scheduleAtFixedRate(task, 100, 10, TimeUnit.MILLISECONDS);
    }

    /**
     * Comsumers use a thread pool to execute operations in operationQueue
     */
    void runConsumers() {
        for (int i = 0; i < numThreads; i++) {
            workers.execute(() -> {
                try {
                    long start = System.currentTimeMillis();
                    Operation op = operationQueue.take();
                    op.execute();
                    long latency = System.currentTimeMillis() - start;
                    log.info("operation latency(milliseconds): " + latency);
                } catch (Exception e) {
                    log.error("Operation failed with", e);
                }
            });
        }
    }

    /**
     * wait sometime for workers to finish the jobs.
     */
    void waitForAppToFinish() {
        workers.shutdown();
        try {
            boolean finishedInTime = workers.
                    awaitTermination(DURATION_IN_MS + APPLICATION_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS);

            if (!finishedInTime) {
                log.error("not finished in time.");
                System.exit(0);
            }
        } catch (InterruptedException e) {
            log.error(String.valueOf(e));
            throw new UnrecoverableCorfuInterruptedError(e);

        } finally {
            System.exit(1);
        }
    }
}