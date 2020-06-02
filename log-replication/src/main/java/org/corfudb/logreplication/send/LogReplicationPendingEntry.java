package org.corfudb.logreplication.send;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * The element kept in the sliding windown to remember the log entries sent over but hasn't been acknowledged by the
 * receiver and we use the time to decide when a re-send is necessary.
 */

@Data
@Slf4j
public class LogReplicationPendingEntry {

        // The address of the log entry
        org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry data;

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

        void retry(long time) {
            this.time = time;
            retry++;
        }
}
