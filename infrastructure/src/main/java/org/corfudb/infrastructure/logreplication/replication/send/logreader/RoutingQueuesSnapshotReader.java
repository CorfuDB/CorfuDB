package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import java.util.UUID;

/**
 * Snapshot reader implementation for Routing Queues Replication Model.
 */
public class RoutingQueuesSnapshotReader extends SnapshotReader {

    public RoutingQueuesSnapshotReader(CorfuRuntime corfuRuntime, LogReplicationSession session,
                                       LogReplicationContext replicationContext) {
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
