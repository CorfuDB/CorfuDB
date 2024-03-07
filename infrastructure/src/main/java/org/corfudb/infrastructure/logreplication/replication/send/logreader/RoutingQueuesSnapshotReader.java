package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.runtime.CorfuRuntime;
import java.util.UUID;

/**
 * Snapshot reader implementation for Routing Queues Replication Model.
 */
public class RoutingQueuesSnapshotReader extends SnapshotReader {

    public RoutingQueuesSnapshotReader(CorfuRuntime corfuRuntime, LogReplicationContext replicationContext,
                                       ReplicationSession session) {
    }

    @Override
    public SnapshotReadMessage read(UUID syncRequestId) {
        return null;
    }

    @Override
    public void reset(long ts) {
        // In addition to setting the snapshot timestamp to ts, write to the table subscribed to by the client
        // requesting for a snapshot sync
    }
}
