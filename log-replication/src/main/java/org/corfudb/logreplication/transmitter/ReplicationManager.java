package org.corfudb.logreplication.transmitter;

import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.fsm.LogReplicationContext;
import org.corfudb.logreplication.fsm.LogReplicationFSM;

public class ReplicationManager {

    private LogReplicationContext context;

    private LogReplicationFSM logReplicationFSM;

    public ReplicationManager(LogListener snapshotListener,
                              LogListener logEntryListener,
                              LogReplicationConfig config) {
        this.context = LogReplicationContext.builder()
                .logEntryListener(logEntryListener)
                .snapshotListener(snapshotListener)
                .config(config)
                .build();
        this.logReplicationFSM = new LogReplicationFSM(context);
    }
}
