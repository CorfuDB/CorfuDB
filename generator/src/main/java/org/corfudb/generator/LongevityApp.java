package org.corfudb.generator;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.operations.CheckpointOperation;
import org.corfudb.generator.operations.Operation;
import org.corfudb.runtime.CorfuRuntime;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by rmichoud on 7/25/17.
 */

/**
 * This Longevity app will stress test corfu using the Load
 * generator during a given duration.
 *
 * I use a blocking queue in order to limit the number of operation
 * created at any time. Operations as created and consumed as we go.
 *
 */
@Slf4j
public class LongevityApp {
    private long durationMs;
    private boolean checkPoint;
    BlockingQueue<Operation> operationQueue;
    CorfuRuntime rt;
    State state;

    ExecutorService taskProducer;
    ScheduledExecutorService checkpointer;
    ExecutorService workers;

    // How much time we live the application hangs once the duration is finished
    // and the application is hanged
    static final int APPLICATION_TIMEOUT_IN_MS = 10000;

    static final int QUEUE_CAPACITY = 1000;

    long startTime;
    int numberThreads;


    public LongevityApp(long durationMs, int numberThreads, String configurationString, boolean checkPoint) {
        this.durationMs = durationMs;
        this.checkPoint = checkPoint;
        this.numberThreads = numberThreads;

        operationQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
        rt = new CorfuRuntime(configurationString).connect();
        state = new State(50, 100, rt);

        taskProducer = Executors.newSingleThreadExecutor();

        workers = Executors.newFixedThreadPool(numberThreads);
        checkpointer = Executors.newScheduledThreadPool(1);
    }

    /**
     * Assess liveness of the application
     *
     * If the client was not able to do any operation during the last APPLICATION_TIMEOUT_IN_MS,
     * we declare liveness of the client as failed. Also, if the client was not able to finish
     * in time, it is marked as liveness failure.
     *
     * @param finishedInTime
     * @return
     */
    private boolean livenessSuccess(boolean finishedInTime) {
        if (!finishedInTime) {
            return false;
        }

        long timeSinceSuccessfulReadOperation = System.currentTimeMillis()
                - state.getLastSuccessfulReadOperationTimestamp();
        long timeSinceSuccessfulWriteOperation = System.currentTimeMillis()
                - state.getLastSuccessfulWriteOperationTimestamp();

        return (timeSinceSuccessfulReadOperation < APPLICATION_TIMEOUT_IN_MS
                && timeSinceSuccessfulWriteOperation < APPLICATION_TIMEOUT_IN_MS);
    }

    /**
     * Give a chance to the workers to finish naturally (thanks to the timer) and then kill
     * the producer and the checkpointer.
     *
     * At the end of the duration, we give some margin for the workers to finish their task before shutting
     * down the thread pool.
     *
     * If the application is hung (which can happen when something goes wrong) we do a hard exit. If any thread
     * is not playing nice with Interrupted exceptions (which can be a bug as well), this is the only way to
     * be sure we terminate.
     */
    private void waitForAppToFinish() {
        workers.shutdown();
        try {
            boolean finishedInTime = workers.
                    awaitTermination(durationMs + APPLICATION_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS);

            String livenessState = livenessSuccess(finishedInTime) ? "Success" : "Fail";

            Correctness.recordOperation("Liveness, " + livenessState, false);
            if (!finishedInTime) {
                System.exit(1);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            taskProducer.shutdownNow();
            checkpointer.shutdownNow();

            boolean checkpointHasFinished = false;
            int exitStatus;
            try {
                checkpointHasFinished = checkpointer.awaitTermination(APPLICATION_TIMEOUT_IN_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                exitStatus = 1;
            }

            exitStatus = checkpointHasFinished ? 0 : 1;
            System.exit(exitStatus);
        }
    }

    /**
     * Check if we should still execute tasks or if the duration has elapsed.
     *
     * @return If we are still within the duration limit
     */
    private boolean withinDurationLimit() {
        return System.currentTimeMillis() - startTime < durationMs;
    }

    /**
     * A thread is tasked to indefinitely create operation. Operations are then
     * put into a blocking queue. The blocking queue serves as a regulator for how
     * many operations exist at the same time. It allows us to regulate the memory footprint.
     *
     * The only way to terminate the producer is to call explicitly a shutdownNow() on
     * the executorService.
     */
    private void runTaskProducer() {
        taskProducer.execute(() -> {
            while (true) {
                Operation current = state.getOperations().getRandomOperation();
                try {
                    operationQueue.put(current);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("operation error", e);
                }
            }
        });

    }

    /**
     * Each workers in the worker pool will consume an operation
     * from the blocking queue. Once the operation dequeued, it is
     * executed. This happens while we are within the boundary of the
     * duration.
     */
    private void runTaskConsumers() {
        // Assign a worker for each thread in the pool
        for (int i = 0; i < numberThreads; i++) {
            workers.execute(() -> {
                while (withinDurationLimit()) {
                    try {
                        Operation op = operationQueue.take();
                        op.execute();
                    } catch (Exception e) {
                        log.error("Operation failed with", e);
                    }
                }
            });
        }
    }

    /**
     * A thread is tasked with checkpointing activity. This is a cyclic
     * operation.
     */
    private void runCpTrimTask() {
        Runnable cpTrimTask = () -> {
            Operation op = new CheckpointOperation(state);
            op.execute();
        };
        checkpointer.scheduleAtFixedRate(cpTrimTask, 30, 20, TimeUnit.SECONDS);
    }


    public void runLongevityTest() {
        startTime = System.currentTimeMillis();

        if (checkPoint) {
            runCpTrimTask();
        }

        runTaskProducer();
        runTaskConsumers();

        waitForAppToFinish();
    }
}
