package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.runtime.CorfuRuntime;
import java.util.UUID;

/**
 * Snapshot reader implementation for Logical Grouping Replication Model.
 */
public class LogicalGroupSnapshotReader extends SnapshotReader {

    public LogicalGroupSnapshotReader(CorfuRuntime runtime, LogReplicationContext replicationContext,
                                      ReplicationSession session) {
    }

    @Override
    public SnapshotReadMessage read(UUID syncRequestId) {
        return null;
    }

    @Override
    public void reset(long ts) {
    }
}
