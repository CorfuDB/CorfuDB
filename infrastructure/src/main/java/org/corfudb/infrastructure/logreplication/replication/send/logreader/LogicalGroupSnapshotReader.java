package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.runtime.LogReplication.LogReplicationSession;

/**
 * Snapshot reader implementation for Logical Grouping Replication Model.
 */
@Slf4j
public class LogicalGroupSnapshotReader extends BaseSnapshotReader {

    public LogicalGroupSnapshotReader(LogReplicationSession session,
                                      LogReplicationContext replicationContext) {
        super(session, replicationContext);
    }

    @Override
    protected void refreshStreamsToReplicateSet() {
        streams = replicationContext.getConfig(session).getStreamsToReplicate();
    }
}
