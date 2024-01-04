package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;

import java.util.UUID;

/**
 * An Interface for Log Entry Reader
 *
 * A log entry reader provides the functionality for reading incremental updates from Corfu.
 */
public abstract class LogEntryReader {

    protected long topologyConfigId;

    public LogReplicationContext replicationContext;

    /**
     * Read a Log Entry.
     *
     * @param logEntryRequestId unique identifier of log entry sync request.
     *
     * @return a log replication entry.
     */
    public abstract LogReplicationEntryMsg read(UUID logEntryRequestId);

    public abstract void reset(long lastSentBaseSnapshotTimestamp, long lastAckedTimestamp);

    public abstract StreamIteratorMetadata getCurrentProcessedEntryMetadata();

    public void setTopologyConfigId(long topologyConfigId) {
        this.topologyConfigId = topologyConfigId;
    }

    public class StreamIteratorMetadata {
        private long timestamp;
        private boolean streamsToReplicatePresent;

        public StreamIteratorMetadata(long timestamp, boolean streamsToReplicatePresent) {
            this.timestamp = timestamp;
            this.streamsToReplicatePresent = streamsToReplicatePresent;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public boolean isStreamsToReplicatePresent() {
            return streamsToReplicatePresent;
        }
    }
}
