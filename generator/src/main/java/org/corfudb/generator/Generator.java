package org.corfudb.generator;

import lombok.extern.slf4j.Slf4j;
import java.util.List;
import java.time.Duration;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;

import org.corfudb.generator.operations.CheckpointOperation;
import org.corfudb.generator.operations.Operation;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.SystemUnavailableError;
import org.corfudb.util.Sleep;

import static java.util.concurrent.Executors.newWorkStealingPool;

/**
 * Generator is a program that generates synthetic workloads that try to mimic
 * real applications that consume the CorfuDb client. Application's access
 * patterns can be approximated by setting different configurations like
 * data distributions, concurrency level, operation types, time skews etc.
 *
 * Created by maithem on 7/14/17.
 *
 * Modified by Miao Cheng on 10/15/18:
 *
 * Re-implemented the Generator class to provide APIs for running workloads that can be
 * 'divided' into chunks, of which transactions are decoupled from each other.
 * This can help do correctness verification for Longevity test chunk-by-chunk on the fly.
 */
@Slf4j
public class Generator {

    private long cpPeriod;
    private Duration totalRuntime;

    static final int numStreams = 50;
    static final int numKeys = 100;
    static final int numThreads = 10;

    BlockingQueue<Operation> operationQueue;
    CorfuRuntime rt;
    State state;
    ScheduledExecutorService scheduler;
    ExecutorService workers;

    // The lock prevents consuming the queue while it's being filled.
    volatile boolean operationQueueLock;

    // Given the above configuration (numStreams, numKeys, numThreads),
    // this queue capacity can be consumed for approximately 3 minutes.
    static final int QUEUE_CAPACITY = 100000;

    static final Duration CHUNK_RUNTIME = Duration.ofHours(1);
    static final Duration CHUNK_TIMEOUT = Duration.ofSeconds(5);
    static final Duration POLL_TIME = Duration.ofSeconds(3);
    static final Duration LIVENESS_TIMEOUT = Duration.ofMinutes(15);
    static final long TIME_TO_WAIT_FOR_RUNTIME_TO_CONNECT = 60000;


    public Generator(String endPoint, long cpPeriod, Duration totalRuntime) {

        this.cpPeriod = cpPeriod;
        this.totalRuntime = totalRuntime;

        rt = new CorfuRuntime(endPoint);

        try {
            tryToConnectTimeout(TIME_TO_WAIT_FOR_RUNTIME_TO_CONNECT);
        } catch (SystemUnavailableError e) {
            System.exit(1);
        }

        state = new State(numStreams, numKeys, rt);

        scheduler = Executors.newScheduledThreadPool(1);

        workers = newWorkStealingPool(numThreads);
        operationQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        operationQueueLock = false;
    }

    /**
     * Try to connect the runtime and throws a SystemUnavailableError if cannot connect
     * within the timeout.
     *
     * @param timeoutInMs
     * @throws SystemUnavailableError
     */
    private void tryToConnectTimeout (long timeoutInMs) throws SystemUnavailableError {
        try {
            CompletableFuture.supplyAsync(() -> rt.connect()).get(timeoutInMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            Correctness.recordOperation("Liveness, " + "Fail", false);
            throw new SystemUnavailableError(e.getMessage());
        }
    }

    /**
     * A method to generate operations, and then put the operations into the blocking queue.
     * The blocking queue serves as a regulator for how many operations exist at the same time.
     */
    private void fillOperationQueue() {
        // Fill the operationQueue to its full capacity
        List<Operation> operations = state.getOperations().sample(QUEUE_CAPACITY);

        operationQueueLock = true;

        for (Operation operation : operations) {
            try {
                operationQueue.put(operation);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("operation error", e);
            }
        }
        operationQueueLock = false;
    }

    /**
     * Each workers in the worker pool will consume an operation from the blocking queue.
     * Once the operation de-queued, it is executed.
     */
    private void consumeOperationQueue() {
        // Assign a worker for each thread in the pool
        for (int i = 0; i < numThreads; i++) {
            workers.execute(() -> {
                while (true) {
                    if (!operationQueueLock && !operationQueue.isEmpty()) {
                        try {
                            Operation op = operationQueue.take();
                            op.execute();
                        } catch (Exception e) {
                            log.error("Operation failed with", e);
                        }
                    }
                }
            });
        }
    }

    /**
     * A thread is tasked with checkpoint and trim activity. This is a cyclic operation.
     */
    private void runCpTrimOperation() {
        Runnable cpTrimTask = () -> {
            Operation op = new CheckpointOperation(state);
            op.execute();
        };
        scheduler.scheduleAtFixedRate(cpTrimTask, 30, cpPeriod, TimeUnit.SECONDS);
    }


    public void shutdown() {
        workers.shutdown();
        scheduler.shutdown();
    }


    /**
     * Run a Longevity test that 'freezes' the system every CHUNK_RUNTIME,
     * sends the correctness log (of one chunk) to verification, then resumes
     * the run.
     * The same action happens in a cyclic fashion until the totalRuntime.
     */
    public void runLongevityTestForVerification() {

        fillOperationQueue();
        long lastPollTime = System.currentTimeMillis();
        consumeOperationQueue();
        runCpTrimOperation();

        while (true) {
            // One round of 'chunk' starts
            long cumulativeRuntime = 0;
            while (true) {
                Sleep.sleepUninterruptibly(POLL_TIME);

                // Check if the operationQueue is drained or not,
                // if it's empty and has not reached CHUNK_RUNTIME,
                // then fill up the queue for further consuming.
                if (operationQueue.isEmpty()) {
                    // Calculate the 'cumulative' runtime so far,
                    // skipping the time for filling up the operation queue
                    cumulativeRuntime += System.currentTimeMillis() - lastPollTime;
                    // Break this while loop if it reaches CHUNK_RUNTIME
                    if (cumulativeRuntime >= CHUNK_RUNTIME.toMillis()) {
                        // At the end of the chunk, give some margin
                        // for the workers to finish their operations
                        Sleep.sleepUninterruptibly(CHUNK_TIMEOUT);
                        Correctness.recordOperation("Liveness, " + "Success", false);
                        break;
                    }
                    fillOperationQueue();
                    lastPollTime = System.currentTimeMillis();
                } else {
                    // If it's been more than LIVENESS_TIMEOUT, and the queue is still not drained,
                    // then the system might have got stuck and we record its 'Liveness' as 'Fail'.
                    if (System.currentTimeMillis() - lastPollTime > LIVENESS_TIMEOUT.toMillis()) {
                        Correctness.recordOperation("Liveness, " + "Fail", false);
                    }
                }
            }

            // TODO: send the correctness log (of this chunk) to verification and clean up.

            // Minus one CHUNK_RUNTIME from the TOTAL_RUNTIME
            totalRuntime = totalRuntime.minus(CHUNK_RUNTIME);
            if (totalRuntime.toHours() == 0) {
                break;
            }

            // TODO: checkpoint all the existing maps of last chunk and do prefix trim.

            // Fill up the queue for next round of 'chunk' run
            fillOperationQueue();
            lastPollTime = System.currentTimeMillis();
        }
    }

}
