package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.exceptions.TrimmedException;
import java.util.UUID;


/**
 * Log entry reader implementation for Routing Queues Replication Model.
 *
 * This implementation reads off the routing queues, a special data structure for this model, which holds
 * the addresses to the actual data (one level of indirection). Then the data is read from the actual addresses
 * in the Data Queue.
 *
 */
public class RoutingQueuesLogEntryReader extends LogEntryReader {

    public RoutingQueuesLogEntryReader(CorfuRuntime runtime, LogReplicationContext replicationContext,
                                       ReplicationSession session) {
    }

    @Override
    public LogReplicationEntryMsg read(UUID logEntryRequestId) throws TrimmedException {
        return null;
    }

    @Override
    public void reset(long lastSentBaseSnapshotTimestamp, long lastAckedTimestamp) {
    }

    @Override
    public StreamIteratorMetadata getCurrentProcessedEntryMetadata() {
        return null;
    }
}
