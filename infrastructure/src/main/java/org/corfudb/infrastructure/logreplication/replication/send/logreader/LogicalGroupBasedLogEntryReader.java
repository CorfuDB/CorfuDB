package org.corfudb.infrastructure.logreplication.replication.send.logreader;

import org.corfudb.infrastructure.logreplication.LogicalGroupBasedReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.ReplicationSession;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import java.util.HashSet;
import java.util.Set;


public class LogicalGroupBasedLogEntryReader extends BaseLogEntryReader {

    public LogicalGroupBasedLogEntryReader(CorfuRuntime runtime, LogReplicationConfigManager configManager,
                                                 ReplicationSession replicationSession) {
        super(runtime, configManager, replicationSession);
    }

    @Override
    protected void refreshStreamsToReplicateSet() {
        super.refreshStreamsToReplicateSet();

        Set<String> streams = config.getStreamsToReplicate();
        streamsToReplicate = new HashSet<>();
        for (String s : streams) {
            streamsToReplicate.add(CorfuRuntime.getStreamID(s));
        }
    }
}
