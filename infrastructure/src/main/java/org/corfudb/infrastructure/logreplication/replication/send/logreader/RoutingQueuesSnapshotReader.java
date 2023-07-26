package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.runtime.LogReplication.LogReplicationSession;

/**
 * Snapshot reader implementation for Routing Queues Replication Model.
 */
public class RoutingQueuesSnapshotReader extends BaseSnapshotReader {

    public RoutingQueuesSnapshotReader(LogReplicationSession session,
                                       LogReplicationContext replicationContext) {
        super(session, replicationContext);
    }

    @Override
    protected void refreshStreamsToReplicateSet() {
        throw new IllegalStateException("Unexpected workflow encountered.  Stream UUIDs cannot be refreshed for this " +
            "model");
    }

    @Override
    public void reset(long ts) {
        // In addition to setting the snapshot timestamp to ts, write to the table subscribed to by the client
        // requesting for a snapshot sync
    }
}
