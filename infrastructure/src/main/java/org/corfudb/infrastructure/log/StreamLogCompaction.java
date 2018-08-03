package org.corfudb.infrastructure.log;

import com.codahale.metrics.Counter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.util.MetricsUtils;

import java.util.concurrent.*;

/**
 * Scheduled log compaction manager.
 * Execute log compaction safely (prevent scheduled executor corruption if the task throws an exception)
 * https://stackoverflow.com/questions/6894595/scheduledexecutorservice-exception-handling
 */
@Slf4j
public class StreamLogCompaction {
    private static final String METRIC_NAME = StreamLogCompaction.class.getName();

    private final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("LogUnit-Maintenance-%d")
            .build();

    /**
     * How many times log compaction executed
     */
    private final Counter gc;

    /**
     * A scheduler, which is used to schedule periodic garbage collection.
     */
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
    private final ScheduledFuture<?> compactor;

    public StreamLogCompaction(StreamLog streamLog, long initialDelay, long period, TimeUnit timeUnit) {
        gc = MetricsUtils.metrics.counter(String.format("%s-stream[%d]", METRIC_NAME, streamLog.hashCode()));

        Runnable task = () -> {
            gc.inc();
            log.debug("Start log compaction. Counter: {}", gc.getCount());
            try {
                streamLog.compact();
            } catch (Throwable ex) {
                log.error("Can't compact stream log. Counter: {}", gc.getCount());
            }
        };
        compactor = scheduler.scheduleAtFixedRate(task, initialDelay, period, timeUnit);
    }

    public void shutdown() {
        compactor.cancel(true);
        scheduler.shutdownNow();
    }

    /**
     * How many times log gc was executed (the log was compacted)
     *
     * @return counter
     */
    public long getGcCounter() {
        return gc.getCount();
    }
}
