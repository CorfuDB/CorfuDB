package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.CorfuRuntime;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
/**
 * Reading transaction log changes after a snapshot transfer for a specific set of streams. The set of streams to replicate
 * will be synced by the config at the start of a log entry sync and when a new stream to replicate is discovered.
 */
public class StreamsLogEntryReader extends BaseLogEntryReader {

    public StreamsLogEntryReader(CorfuRuntime runtime, LogReplicationSession replicationSession,
                                 LogReplicationContext replicationContext) {
        super(runtime, replicationSession, replicationContext);
    }

    public static class StreamIteratorMetadata {
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

    @Override
    public void setTopologyConfigId(long topologyConfigId) {
        this.topologyConfigId = topologyConfigId;
    }

    public StreamsLogEntryReader(CorfuRuntime runtime, LogReplicationSession replicationSession,
                                 LogReplicationContext replicationContext) {
        super(runtime, replicationSession, replicationContext);
    }
}
