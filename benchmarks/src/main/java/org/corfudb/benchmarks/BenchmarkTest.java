package org.corfudb.benchmarks;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;

import java.util.concurrent.*;

/**
 * This class is the super class for all performance tests.
 * It set test parameters like how many clients to run, how many threads each client run,
 * and how many request one thread send.
 */
@Slf4j
public class BenchmarkTest {
    private static class Args {
        @Parameter(names = {"-h", "--help"}, description = "Help message", help = true)
        boolean help;

        @Parameter(names = {"--endpoint"}, description = "Cluster endpoint", required = true)
        String endpoint; //ip:portnum

        @Parameter(names = {"--num-clients"}, description = "Number of clients", required = true)
        int numClients;

        @Parameter(names = {"--num-threads"}, description = "Total number of threads", required = true)
        int numThreads;

        @Parameter(names = {"--num-requests"}, description = "Number of requests per thread", required = true)
        int numRequests;
    }
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

    private final long durationMs = 100;
    static final int APPLICATION_TIMEOUT_IN_MS = 10000;
    static final int QUEUE_CAPACITY = 1000;

    BenchmarkTest(String[] args) {
        setArgs(args);
        rts = new CorfuRuntime[numRuntimes];

        for (int x = 0; x < rts.length; x++) {
            rts[x] = new CorfuRuntime(endpoint).connect();
        }
        log.info("Connected {} runtimes...", numRuntimes);

        operationQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        taskProducer = Executors.newSingleThreadExecutor();
        workers = Executors.newFixedThreadPool(numThreads);

    }

    private void setArgs(String[] args) {
        Args cmdArgs = new Args();
        JCommander jc = JCommander.newBuilder()
                .addObject(cmdArgs)
                .build();
        jc.parse(args);

        if (cmdArgs.help) {
            jc.usage();
            System.exit(0);
        }
        numRuntimes = cmdArgs.numClients;
        numThreads = cmdArgs.numThreads;
        numRequests = cmdArgs.numRequests;
        endpoint = cmdArgs.endpoint;
    }
    private void enQueue(String operationName, CorfuRuntime rt) {
        String[] names = operationName.split("_");
        if (names.length != 2) {
            log.error("invalid operation name");
        }
        Operation current = null;
        if (names[0].equals("Sequencer")) {
            current = new SequencerOps(names[1], rt);
        }
        if (current != null) {
            try {
                operationQueue.put(current);
            } catch (InterruptedException e) {
                throw new UnrecoverableCorfuInterruptedError(e);
            } catch (Exception e) {
                log.error("operation error", e);
            }
        } else {
            log.error("no such operation");
        }
    }

    private void runTaskProducer(String operationName) {
        for (int i = 0; i < numThreads; i++) {
            CorfuRuntime rt = rts[i % rts.length];
            for (int j = 0; j < numRequests; j++) {
                taskProducer.execute(() -> {
                    enQueue(operationName, rt);
                });
            }
        }
    }

    private void runConsumers() {
        for (int i = 0; i < numThreads; i++) {
            for (int j = 0; j < numRequests; j++) {
                workers.execute(() -> {
                    try {
                        long start = System.nanoTime();
                        Operation op = operationQueue.take();
                        op.execute();
                        long latency = System.nanoTime() - start;
                        log.info("nextToken request latency: "+ latency);
                    } catch (Exception e) {
                        log.error("Operation failed with", e);
                    }
                });
            }
        }
    }

    private void waitForAppToFinish() {
        workers.shutdown();
        try {
            boolean finishedInTime = workers.
                    awaitTermination(durationMs + APPLICATION_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS);

            if (!finishedInTime) {
                System.exit(1);
            }
        } catch (InterruptedException e) {
            throw new UnrecoverableCorfuInterruptedError(e);
        } finally {
            System.exit(0);
        }
    }

    private void runSequencerTest() {
        runTaskProducer("Sequencer_query");
        runConsumers();
        waitForAppToFinish();
    }

    public static void main(String[] args) {
        BenchmarkTest rbt = new BenchmarkTest(args);
        rbt.runSequencerTest();
    }
}
