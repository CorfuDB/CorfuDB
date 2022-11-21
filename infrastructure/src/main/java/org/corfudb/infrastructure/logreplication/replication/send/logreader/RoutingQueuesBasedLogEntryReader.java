package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.exceptions.TrimmedException;
import java.util.UUID;

public class RoutingQueuesBasedLogEntryReader extends LogEntryReader {

    public RoutingQueuesBasedLogEntryReader(CorfuRuntime runtime, LogReplicationConfigManager configManager,
                                           ReplicationSession replicationSession) {

    }

    @Override
    public LogReplication.LogReplicationEntryMsg read(UUID logEntryRequestId) throws TrimmedException {
        // Reads from the queue corresponding to this destination.  The queue contains addresses in the main queue
        // from where the actual data(payload) is to be read.
        return null;
    }

    @Override
    public void reset(long lastSentBaseSnapshotTimestamp, long lastAckedTimestamp) {
    }

    @Override
    public StreamIteratorMetadata getCurrentProcessedEntryMetadata() {
        return null;
    }

    @Override
    public void setTopologyConfigId(long topologyConfigId) {

    }
}
