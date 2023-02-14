package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationSession;

import java.util.HashSet;
import java.util.Set;


/**
 * Log entry reader implementation for Logical Grouping Replication Model.
 *
 * This implementation is very similar to the default implementation for the full table replication model,
 * with the exception that it will read from a different transactional stream for log entry sync (one that is
 * specific for this model).
 */
public class LogicalGroupLogEntryReader extends BaseLogEntryReader {

    public LogicalGroupLogEntryReader(CorfuRuntime runtime, LogReplicationSession session,
                                      LogReplicationContext replicationContext) {
        super(runtime, session, replicationContext);
    }

    @Override
    protected void refreshStreamUUIDs() {
        Set<String> streams = replicationContext.getConfig(session).getStreamsToReplicate();
        streamUUIDs = new HashSet<>();
        for (String s : streams) {
            streamUUIDs.add(CorfuRuntime.getStreamID(s));
        }
    }
}
