package org.corfudb.logreplication.transmitter;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.fsm.LogReplicationContext;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.fsm.LogReplicationFSM;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.logreplication.fsm.LogReplicationEvent.LogReplicationEventType;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

@Data
@Slf4j
/**
 * Add Comment [Anny todo]
 */
public class DataTransmitter {

    // ANNY TODO COMMENT
    // private LogReplicationContext context;

    // ANNY TODO COMMENT
    private final LogReplicationFSM logReplicationFSM;

    // ANNY TODO COMMENT
    public DataTransmitter(CorfuRuntime runtime,
                           SnapshotListener snapshotListener,
                           LogEntryListener logEntryListener,
                           LogReplicationConfig config) {
        LogReplicationContext context = LogReplicationContext.builder()
                .logEntryListener(logEntryListener)
                .snapshotListener(snapshotListener)
                .corfuRuntime(runtime)
                .dataTransmitter(this)
                .stateMachineWorker(Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("state-machine-worker-%d").build()))
                .config(config)
                .build();
        this.logReplicationFSM = new LogReplicationFSM(context);
    }

    public void startSnapshotSync(SnapshotSyncContext context) {
        // Verify if we really need this context, and if we need to pass to the fsm

        // Enqueue event into Log Replication FSM
        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST));
    }

    public void startReplication() {
        // Enqueue event into Log Replication FSM
        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.REPLICATION_START));
    }

    public void stopReplication() {
        // Enqueue event into Log Replication FSM
        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.REPLICATION_STOP));
    }

    public void cancelSnapshotSync(SnapshotSyncContext context) {
        // Enqueue event into Log Replication FSM
        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_CANCEL));
    }
}
