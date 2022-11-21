package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import java.util.UUID;

public class RoutingQueuesBasedSnapshotReader implements SnapshotReader {

    public RoutingQueuesBasedSnapshotReader(CorfuRuntime corfuRuntime, LogReplicationConfigManager configManager,
                                           ReplicationSession replicationSession) {

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

    @Override
    public void setTopologyConfigId(long topologyConfigId) {

    }
}
