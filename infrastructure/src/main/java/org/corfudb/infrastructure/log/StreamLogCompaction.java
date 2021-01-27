package org.corfudb.infrastructure.log;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuInterruptedError;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Scheduled log compaction manager.
 * Execute log compaction safely (prevent scheduled executor corruption if the task throws an exception)
 * https://stackoverflow.com/questions/6894595/scheduledexecutorservice-exception-handling
 */
@Slf4j
public class StreamLogCompaction {
    private final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("LogUnit-Maintenance-%d")
            .build();

    /**
     * A scheduler, which is used to schedule periodic stream log compaction for garbage collection.
     */
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
    private final ScheduledFuture<?> compactor;
    private final Duration shutdownTimer;

    public StreamLogCompaction(StreamLog streamLog, long initialDelay, long period, TimeUnit timeUnit,
                               Duration shutdownTimer) {
        this.shutdownTimer = shutdownTimer;

        Runnable task = () -> {
            log.debug("Start log compaction.");
            try {
                streamLog.compact();
            } catch (Exception ex) {
                log.error("Can't compact stream log.", ex);
            }
        };
        compactor = scheduler.scheduleWithFixedDelay(task, initialDelay, period, timeUnit);
    }

    public void shutdown() {
        compactor.cancel(true);
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(shutdownTimer.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            log.debug("Stream log compaction, awaitTermination interrupted : {}", ie);
            throw new UnrecoverableCorfuInterruptedError(ie);
        }
    }
}
