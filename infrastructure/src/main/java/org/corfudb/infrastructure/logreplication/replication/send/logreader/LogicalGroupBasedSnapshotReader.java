package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.LogicalGroupBasedReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import java.util.PriorityQueue;

public class LogicalGroupBasedSnapshotReader extends BaseSnapshotReader {

    public LogicalGroupBasedSnapshotReader(CorfuRuntime runtime, LogReplicationConfigManager configManager,
                                                 ReplicationSession replicationSession) {
        super(runtime, configManager, replicationSession);
    }

    @Override
    protected void refreshStreamsToReplicateSet() {
        super.refreshStreamsToReplicateSet();
        streams = config.getStreamsToReplicate();
        streamsToSend = new PriorityQueue<>(streams);
    }
}
