package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationSession;

/**
 * Snapshot reader implementation for Logical Grouping Replication Model.
 */
public class LogicalGroupSnapshotReader extends BaseSnapshotReader {

    public LogicalGroupSnapshotReader(CorfuRuntime runtime, LogReplicationSession session,
                                      LogReplicationContext replicationContext) {
        super(runtime, session, replicationContext);
    }

    @Override
    protected void refreshStreamsToReplicateSet() {
        streams = replicationContext.getConfig(session).getStreamsToReplicate();
    }
}
