package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.exceptions.TrimmedException;
import java.util.UUID;

public class LogicalGroupBasedLogEntryReader implements LogEntryReader {

    public LogicalGroupBasedLogEntryReader(CorfuRuntime runtime, LogReplicationConfigManager configManager,
                                                 ReplicationSession replicationSession) {

    }

    @Override
    public LogReplication.LogReplicationEntryMsg read(UUID logEntryRequestId) throws TrimmedException {
        return null;
    }

    @Override
    public void reset(long lastSentBaseSnapshotTimestamp, long lastAckedTimestamp) {
    }

    @Override
    public StreamIteratorMetadata getCurrentProcessedEntryMetadata() {
        return null;
    }

    @Override
    public void setTopologyConfigId(long topologyConfigId) {

    }

    @Override
    public boolean hasMessageExceededSize() {
        return false;
    }
}
