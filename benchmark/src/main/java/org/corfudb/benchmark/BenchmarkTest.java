package org.corfudb.benchmark;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;
import java.util.concurrent.*;

/**
 * This class is the super class for all benchmark tests.
 * It set test parameters like how many clients to run, how many threads each client run,
 * and how many request one thread send.
 */
@Slf4j
public class BenchmarkTest {
    /**
     * Number of clients
     */
    protected int numRuntimes = -1;
    /**
     * Number of threads per client.
     */
    protected int numThreads = -1;
    /**
     * Number of requests per thread.
     */
    protected int numRequests = -1;
    /**
     * Server endpoint.
     */
    protected String endpoint = null;

    protected CorfuRuntime[] rts = null;
    final BlockingQueue<Operation> operationQueue;
    final ExecutorService taskProducer;
    final ExecutorService workers;

    private final long durationMs = 1000;
    static final int APPLICATION_TIMEOUT_IN_MS = 10000000;
    static final int QUEUE_CAPACITY = 1000;

    BenchmarkTest(ParseArgs parseArgs) {
        setArgs(parseArgs);
        rts = new CorfuRuntime[numRuntimes];

        for (int x = 0; x < rts.length; x++) {
            rts[x] = new CorfuRuntime(endpoint).connect();
        }
        log.info("Connected {} runtimes...", numRuntimes);

        operationQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        taskProducer = Executors.newSingleThreadExecutor();
        workers = Executors.newFixedThreadPool(numThreads);
    }

    private void setArgs(ParseArgs parseArgs) {
        numRuntimes = parseArgs.getNumRuntimes();
        numThreads = parseArgs.getNumThreads();
        numRequests = parseArgs.getNumRequests();
        endpoint = parseArgs.getEndpoint();
    }

    /**
     * producer puts one operation into operationQueue.
     * Operation can be Sequencer operation, CorfuTable operation, etc.
     * @param operation the operation pushed into operationQueue
     */
    void runProducer(Operation operation) {
            taskProducer.execute(() -> {
                try {
                    operationQueue.put(operation);
                } catch (InterruptedException e) {
                    throw new UnrecoverableCorfuInterruptedError(e);
                } catch (Exception e) {
                    log.error("operation error", e);
                }
            });
    }

    /**
     * Comsumers use a thread pool to execute operations in operationQueue
     */
    void runConsumers() {
        for (int i = 0; i < numThreads; i++) {
            workers.execute(() -> {
                try {
                    long start = System.nanoTime();
                    Operation op = operationQueue.take();
                    op.execute();
                    long latency = System.nanoTime() - start;
                    System.out.println("nextToken request latency: " + latency);
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
                    awaitTermination(durationMs + APPLICATION_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS);

            if (!finishedInTime) {
                log.error("not finished in time.");
                System.exit(1);
            }
        } catch (InterruptedException e) {
            log.error(String.valueOf(e));
            throw new UnrecoverableCorfuInterruptedError(e);
        } finally {
            System.exit(0);
        }
    }
}
