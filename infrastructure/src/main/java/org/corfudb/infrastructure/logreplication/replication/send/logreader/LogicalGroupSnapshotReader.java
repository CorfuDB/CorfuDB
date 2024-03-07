package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import java.util.UUID;

/**
 * Snapshot reader implementation for Logical Grouping Replication Model.
 */
public class LogicalGroupSnapshotReader extends SnapshotReader {

    public LogicalGroupSnapshotReader(CorfuRuntime runtime, LogReplicationSession session,
                                      LogReplicationContext replicationContext) {
    }

    @Override
    public SnapshotReadMessage read(UUID syncRequestId) {
        return null;
    }

    @Override
    public void reset(long ts) {
    }
}
