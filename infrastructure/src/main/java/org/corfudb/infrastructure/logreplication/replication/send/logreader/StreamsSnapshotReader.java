package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import java.util.PriorityQueue;

public class StreamsSnapshotReader extends BaseSnapshotReader {

    public StreamsSnapshotReader(CorfuRuntime runtime, LogReplicationConfigManager configManager,
                                                 ReplicationSession replicationSession) {
        super(runtime, configManager, replicationSession);
    }

    protected void refreshStreamsToReplicateSet() {
        super.refreshStreamsToReplicateSet();
        streams = config.getStreamsToReplicate();
        streamsToSend = new PriorityQueue<>(streams);
    }
}
