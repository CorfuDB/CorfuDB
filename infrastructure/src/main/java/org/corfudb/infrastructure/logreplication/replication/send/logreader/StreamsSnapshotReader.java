package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.runtime.LogReplication.LogReplicationSession;
import org.corfudb.runtime.CorfuRuntime;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
/**
 *  Default snapshot reader implementation
 *
 *  The streams to replicate are read from registry table and will be refreshed at the start of a snapshot sync.
 *
 *  This implementation provides reads at the stream level (no coalesced state).
 *  It generates TxMessages which will be transmitted by the DataSender (provided by the application).
 */
public class StreamsSnapshotReader extends BaseSnapshotReader {

    public StreamsSnapshotReader(CorfuRuntime runtime, LogReplicationSession session,
                                 LogReplicationContext replicationContext) {
        super(runtime, session, replicationContext);
    }

    @Override
    protected void refreshStreamsToReplicateSet() {
        streams = replicationContext.getConfig(session).getStreamsToReplicate();
    }

}
