package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.exceptions.TrimmedException;
import java.util.UUID;

/**
 * Log entry reader implementation for Logical Grouping Replication Model.
 *
 * This implementation is very similar to the default implementation for the full table replication model,
 * with the exception that it will read from a different transactional stream for log entry sync (one that is
 * specific for this model).
 */
public class LogicalGroupLogEntryReader extends LogEntryReader {

    public LogicalGroupLogEntryReader(CorfuRuntime runtime, LogReplicationContext replicationContext,
                                      ReplicationSession session) {
    }

    @Override
    public LogReplicationEntryMsg read(UUID logEntryRequestId) throws TrimmedException {
        return null;
    }

    @Override
    public void reset(long lastSentBaseSnapshotTimestamp, long lastAckedTimestamp) {
    }

    @Override
    public StreamIteratorMetadata getCurrentProcessedEntryMetadata() {
        return null;
    }
}
