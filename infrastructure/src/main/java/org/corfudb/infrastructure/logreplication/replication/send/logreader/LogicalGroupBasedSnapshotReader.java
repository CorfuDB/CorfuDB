package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import java.util.UUID;

public class LogicalGroupBasedSnapshotReader implements SnapshotReader {

    public LogicalGroupBasedSnapshotReader(CorfuRuntime runtime, LogReplicationConfigManager configManager,
                                                 ReplicationSession replicationSession) {

    }

    @Override
    public SnapshotReadMessage read(UUID syncRequestId) {
        return null;
    }

    @Override
    public void reset(long ts) {
    }

    @Override
    public void setTopologyConfigId(long topologyConfigId) {

    }
}
