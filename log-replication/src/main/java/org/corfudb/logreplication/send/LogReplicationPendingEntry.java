package org.corfudb.logreplication.send;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;

/**
 * The element kept in the sliding window to remember the log entries sent over but hasn't been acknowledged by the
 * receiver and we use the time to decide when a re-send is necessary.
 */

@Data
@Slf4j
public class LogReplicationPendingEntry {

        LogReplicationEntry data;

        // The first time the log entry is sent over
        long time;

        // The number of retries for this entry
        int retry;

        public LogReplicationPendingEntry(org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry data, long time) {
            this.data = data;
            this.time = time;
            this.retry = 0;
        }

        boolean timeout(long ctime, long timer) {
            log.trace("current time {} - original time {} = {} timer {}", ctime, this.time, timer);
            return  (ctime - this.time) > timer;
        }

    /**
     * update retry number and the time with current time.
     * @param currentTime the current system time
     */
    void retry(long currentTime) {
            this.time = currentTime;
            retry++;
        }
}
