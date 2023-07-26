package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.runtime.LogReplication.LogReplicationSession;


/**
 * Log entry reader implementation for Logical Grouping Replication Model.
 *
 * This implementation is very similar to the default implementation for the full table replication model,
 * with the exception that it will read from a different transactional stream for log entry sync (one that is
 * specific for this model).
 */
public class LogicalGroupLogEntryReader extends BaseLogEntryReader {

    public LogicalGroupLogEntryReader(LogReplicationSession session, LogReplicationContext replicationContext) {
        super(session, replicationContext);
    }
}
