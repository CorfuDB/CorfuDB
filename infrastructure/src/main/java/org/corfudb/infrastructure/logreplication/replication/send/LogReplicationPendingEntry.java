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
    /*
     * For internal timer increasing for each message in milliseconds
     */
    private final static long TIME_INCREMENT = 100;

    private long currentTime = 0;

    @Getter
    private LogReplicationEntryMsg data;

    // The first time the log entry is sent over
    private long time;

    // The number of retries for this entry
    public int retry;

    // Set to force this entry to be resent on the very next resend() call, bypassing timeout()'s
    // cadence check entirely -- used when the receiver has explicitly confirmed it's still waiting
    // for this entry (see SnapshotSenderBufferManager.expediteResendFrom()), rather than relying on
    // this class's internal simulated clock (which advances a fixed amount per call, not per real
    // elapsed time, so there is no wall-clock value that reliably forces an "immediate" timeout).
    private boolean expedited = false;

    public LogReplicationPendingEntry(LogReplicationEntryMsg data) {
        this.data = data;
        this.time = getCurrentTime();
        this.retry = 0;
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
        currentTime += TIME_INCREMENT;
        return currentTime;
    }
}
