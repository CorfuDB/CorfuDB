package org.corfudb.logreplication.transmitter;

import lombok.Data;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.fsm.LogReplicationContext;
import org.corfudb.logreplication.fsm.LogReplicationFSM;
import org.corfudb.runtime.CorfuRuntime;

@Data
public class ReplicationManager {

    private LogReplicationContext context;

    private final LogReplicationFSM logReplicationFSM;

    public ReplicationManager(CorfuRuntime runtime,
                              LogListener snapshotListener,
                              LogListener logEntryListener,
                              LogReplicationConfig config) {
        this.context = LogReplicationContext.builder()
                .logEntryListener(logEntryListener)
                .snapshotListener(snapshotListener)
                .corfuRuntime(runtime)
                .replicationManager(this)
                .config(config)
                .build();
        this.logReplicationFSM = new LogReplicationFSM(context);
    }

    public void startSnapshotSync(SnapshotSyncContext context) {
        // Add SNAPSHOT_SYNC_REQUEST event to logReplicationFSM queue
        // logReplicationFSM.input(LogReplicationEvent.LogReplicationEventType.SNAPHOT_SYNC_REQUEST);
    }

    public void startReplication() {

    }

    public void stopReplication() {

    }

    public void cancelSnapshotSync(SnapshotSyncContext context) {

    }
}
