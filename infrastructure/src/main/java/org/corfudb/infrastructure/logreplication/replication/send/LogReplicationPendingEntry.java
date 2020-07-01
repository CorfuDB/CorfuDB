package org.corfudb.infrastructure.logreplication.replication.send;

import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;

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
    final static private long TIME_INCREMENT = 100;

    long currentTime = 0;

    @Getter
    private LogReplicationEntry data;

    // The first time the log entry is sent over
    long time;

    // The number of retries for this entry
    int retry;

    public LogReplicationPendingEntry(org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry data) {
        this.data = data;
        this.time = getCurrentTime();
        this.retry = 0;
    }

    boolean timeout(long timer) {
        long ctime = getCurrentTime();
        log.trace("current time {} - original time {} = {} timer {}", ctime, this.time, timer);
        return  (ctime - this.time) > timer;
    }

    /**
     * update retry number and the time with current time.
     */
    void retry() {
        this.time = getCurrentTime();
        retry++;
    }

    private long getCurrentTime() {
        currentTime += TIME_INCREMENT;
        return currentTime;
    }
}
