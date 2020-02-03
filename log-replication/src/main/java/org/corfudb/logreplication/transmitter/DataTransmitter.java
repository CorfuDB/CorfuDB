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

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

@Data
@Slf4j
public class DataTransmitter {

    private LogReplicationContext context;

    private final LogReplicationFSM logReplicationFSM;

    public DataTransmitter(CorfuRuntime runtime,
                           LogListener snapshotListener,
                           LogListener logEntryListener,
                           LogReplicationConfig config) {
        this.context = LogReplicationContext.builder()
                .logEntryListener(logEntryListener)
                .snapshotListener(snapshotListener)
                .corfuRuntime(runtime)
                .dataTransmitter(this)
                .blockingOpsScheduler(Executors.newScheduledThreadPool(6, (r) -> {
                    ThreadFactory threadFactory =
                            new ThreadFactoryBuilder().setNameFormat("replication-fsm-%d").build();
                    Thread t = threadFactory.newThread(r);
                    t.setDaemon(true);
                    return t;
                }))
                .config(config)
                .build();
        this.logReplicationFSM = new LogReplicationFSM(context);
    }

    public void startSnapshotSync(SnapshotSyncContext context) {
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
