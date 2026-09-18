package org.corfudb.infrastructure.logreplication.replication.send;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;

/**
 * The element kept in the sliding window to remember the log entries sent over but hasn't been acknowledged by the
 * receiver and we use the time to decide when a re-send is necessary.
 */

@Data
@Slf4j
public class LogReplicationPendingEntry {
    // Retries measure monotonic elapsed time, so the resend cadence is independent of how often the
    // source happens to look at the entry.
    private final java.util.function.LongSupplier clock;

    @Getter
    private LogReplicationEntryMsg data;

    // The first time the log entry is sent over
    private long time;

    // The number of retries for this entry
    public int retry;

    public LogReplicationPendingEntry(LogReplicationEntryMsg data) {
        this(data, () -> java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
    }

    LogReplicationPendingEntry(LogReplicationEntryMsg data, java.util.function.LongSupplier clock) {
        this.data = data;
        this.clock = clock;
        this.time = clock.getAsLong();
    }

    public boolean timeout(long timer) {
        long ctime = getCurrentTime();
        log.trace("current time {} - original time {} = {} timer {}", ctime, this.time, timer);
        return  (ctime - this.time) > timer;
    }

    /**
     * update retry number and the time with current time.
     */
    public void retry() {
        this.time = getCurrentTime();
        retry++;
    }

    private long getCurrentTime() {
        return clock.getAsLong();
    }
}
