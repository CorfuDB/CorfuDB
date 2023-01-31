package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationSession;

/**
 * Snapshot reader implementation for Logical Grouping Replication Model.
 */
@Slf4j
public class LogicalGroupSnapshotReader extends BaseSnapshotReader {

    public LogicalGroupSnapshotReader(CorfuRuntime runtime, LogReplicationContext replicationContext,
                                      ReplicationSession session) {
    }

    @Override
    protected void refreshStreamsToReplicateSet() {
        streams = replicationContext.getConfig(session).getStreamsToReplicate();
    }
}
