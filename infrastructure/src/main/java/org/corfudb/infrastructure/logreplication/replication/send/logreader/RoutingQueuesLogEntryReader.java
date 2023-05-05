package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.exceptions.TrimmedException;
import java.util.UUID;


/**
 * Log entry reader implementation for Routing Queues Replication Model.
 *
 * This implementation reads off the routing queues, a special data structure for this model, which holds
 * the addresses to the actual data (one level of indirection). Then the data is read from the actual addresses
 * in the Data Queue.
 *
 */
public class RoutingQueuesLogEntryReader extends BaseLogEntryReader {

    public RoutingQueuesLogEntryReader(CorfuRuntime runtime, LogReplicationSession session,
                                       LogReplicationContext replicationContext) {
        super(runtime, session, replicationContext);
    }

    @Override
    public LogReplicationEntryMsg read(UUID logEntryRequestId) throws TrimmedException {
        // Reads from the queue corresponding to this destination.  The queue contains addresses in the main queue
        // from where the actual data(payload) is to be read.
        return null;
    }
}
