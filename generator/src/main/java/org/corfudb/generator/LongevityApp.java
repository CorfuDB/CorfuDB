package org.corfudb.generator;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.generator.operations.CheckpointOperation;
import org.corfudb.generator.operations.Operation;
import org.corfudb.generator.operations.UpdateVersionHandler;
import org.corfudb.generator.state.State;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.unrecoverable.SystemUnavailableError;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This Longevity app will stress test corfu using the Load
 * generator during a given duration.
 * <p>
 * I use a blocking queue in order to limit the number of operation
 * created at any time. Operations as created and consumed as we go.
 */
@Slf4j
public class LongevityApp {

    private final boolean checkPoint;
    private final BlockingQueue<Operation> operationQueue;
    private final CorfuRuntime rt;
    private final State state;

    private final ExecutorService taskProducer;
    private final ScheduledExecutorService checkpointer;
    private final ExecutorService workers;

    // How much time we live the application hangs once the duration is finished
    // and the application is hanged
    public static final Duration APPLICATION_TIMEOUT = Duration.ofSeconds(10);
    public static final Duration TIME_TO_WAIT_FOR_RUNTIME_TO_CONNECT = Duration.ofMinutes(1);

    private static final int QUEUE_CAPACITY = 1000;

    private long startTime;
    private final Duration duration;

    private final int numberThreads;

    public LongevityApp(Duration duration, int numberThreads, String configurationString, boolean checkPoint) {
        this.duration = duration;
        this.checkPoint = checkPoint;
        this.numberThreads = numberThreads;

        operationQueue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

        rt = new CorfuRuntime(configurationString);

        try {
            tryToConnectTimeout(TIME_TO_WAIT_FOR_RUNTIME_TO_CONNECT);
        } catch (SystemUnavailableError e) {
            System.exit(ExitStatus.ERROR.getCode());
        }


        state = new State(50, 50, rt);

        taskProducer = Executors.newSingleThreadExecutor();

        workers = Executors.newFixedThreadPool(numberThreads);
        checkpointer = Executors.newScheduledThreadPool(1);
    }

    public ExitStatus runLongevityTest() {
        startTime = System.currentTimeMillis();

        new UpdateVersionHandler().handle(state);

        if (checkPoint) {
            runCpTrimTask();
        }

        runTaskProducer();
        runTaskConsumers();

        ExitStatus exitStatus = waitForAppToFinish();

        return exitStatus;
    }

    /**
     * Give a chance to the workers to finish naturally (thanks to the timer) and then kill
     * the producer and the checkpointer.
     * <p>
     * At the end of the duration, we give some margin for the workers to finish their task before shutting
     * down the thread pool.
     * <p>
     * If the application is hung (which can happen when something goes wrong) we do a hard exit. If any thread
     * is not playing nice with Interrupted exceptions (which can be a bug as well), this is the only way to
     * be sure we terminate.
     */
    private ExitStatus waitForAppToFinish() {
        ExitStatus exitStatus;

        workers.shutdown();

        try {
            long timeout = duration.plus(APPLICATION_TIMEOUT).toMillis();
            boolean finishedInTime = workers.awaitTermination(timeout, TimeUnit.MILLISECONDS);
            exitStatus = ExitStatus.fromBool(finishedInTime);
        } catch (Exception e) {
            exitStatus = ExitStatus.ERROR;
        }

        String livenessState = state.getCtx().livenessSuccess(exitStatus.toBool()) ? "Success" : "Fail";

        Correctness.recordOperation("Liveness, " + livenessState, false);

        taskProducer.shutdownNow();
        checkpointer.shutdownNow();

        if (exitStatus == ExitStatus.ERROR) {
            return ExitStatus.ERROR;
        }

        try {
            long timeout = APPLICATION_TIMEOUT.toMillis();
            boolean checkpointHasFinished = checkpointer.awaitTermination(timeout, TimeUnit.MILLISECONDS);
            exitStatus = ExitStatus.fromBool(checkpointHasFinished);
        } catch (Exception e) {
            exitStatus = ExitStatus.ERROR;
        }

        return exitStatus;
    }

    /**
     * Check if we should still execute tasks or if the duration has elapsed.
     *
     * @return If we are still within the duration limit
     */
    private boolean withinDurationLimit() {
        return System.currentTimeMillis() - startTime < duration.toMillis();
    }

    /**
     * A thread is tasked to indefinitely create operation. Operations are then
     * put into a blocking queue. The blocking queue serves as a regulator for how
     * many operations exist at the same time. It allows us to regulate the memory footprint.
     * <p>
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
                    throw new UnrecoverableCorfuInterruptedError(e);
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

    /**
     * Try to connect the runtime and throws a SystemUnavailableError if cannot connect
     * within the timeout.
     *
     * @param timeout timeout
     * @throws SystemUnavailableError error
     */
    private void tryToConnectTimeout(Duration timeout) throws SystemUnavailableError {
        try {
            CompletableFuture.supplyAsync(rt::connect).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            Correctness.recordOperation("Liveness, " + false, false);
            throw new SystemUnavailableError(e.getMessage());
        }
    }

    @AllArgsConstructor
    public enum ExitStatus {
        OK(0), ERROR(1);

        @Getter
        private final int code;

        public static ExitStatus fromBool(boolean boolStatus) {
            if (boolStatus) {
                return ExitStatus.OK;
            } else {
                return ExitStatus.ERROR;
            }
        }

        public boolean toBool() {
            return this == OK;
        }
    }
}
