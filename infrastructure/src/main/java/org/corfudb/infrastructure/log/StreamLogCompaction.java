package org.corfudb.infrastructure.log;

import com.codahale.metrics.Timer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.util.CorfuComponent;
import org.corfudb.util.MetricsUtils;

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
    static final String STREAM_COMPACT_METRIC = CorfuComponent.INFRA_STREAM_OPS + "compaction";

    private final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("LogUnit-Maintenance-%d")
            .build();

    /**
     * A timer that collect metrics about log compaction
     */
    private final Timer compactionTimer = ServerContext.getMetrics().timer(STREAM_COMPACT_METRIC);

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
            try (Timer.Context context = MetricsUtils.getConditionalContext(compactionTimer)){
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
            Thread.currentThread().interrupt();
        }
    }
}
