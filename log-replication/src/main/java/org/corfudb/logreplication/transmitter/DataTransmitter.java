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

@Data
@Slf4j
/**
 * A class that represents the entry point to initiate log replication on the transmitter side.
 **/
public class DataTransmitter {

    /*
      Log Replication State Machine
     */
    private final LogReplicationFSM logReplicationFSM;

    /**
     * Constructor Data Transmitter
     *
     * @param runtime Corfu Runtime
     * @param snapshotListener implementation of a Snapshot Listener, this represents the application callback
     *                         for snapshot data transmission.
     * @param logEntryListener implementation of a Log Entry Listener, this represents the application callback
     *                         for log entry data transmission.
     * @param config Log Replication Configuration
     */
    public DataTransmitter(CorfuRuntime runtime,
                           SnapshotListener snapshotListener,
                           LogEntryListener logEntryListener,
                           LogReplicationConfig config) {
        LogReplicationContext context = LogReplicationContext.builder()
                .logEntryListener(logEntryListener)
                .corfuRuntime(runtime)
                .logReplicationFSM(this.getLogReplicationFSM())
                .snapshotReader(new StreamsSnapshotReader(runtime, snapshotListener, config))
                .stateMachineWorker(Executors.newSingleThreadExecutor(new
                        ThreadFactoryBuilder().setNameFormat("state-machine-worker-%d").build()))
                .config(config)
                .build();
        this.logReplicationFSM = new LogReplicationFSM(context);
    }

    /**
     * Constructor Data Transmitter
     *
     * @param runtime
     * @param logEntryListener
     * @param snapshotReader
     * @param config
     */
    public DataTransmitter(CorfuRuntime runtime,
                           LogEntryListener logEntryListener,
                           SnapshotReader snapshotReader,
                           LogReplicationConfig config) {
        LogReplicationContext context = LogReplicationContext.builder()
                .logEntryListener(logEntryListener)
                .corfuRuntime(runtime)
                .snapshotReader(snapshotReader)
                .logReplicationFSM(this.getLogReplicationFSM())
                .stateMachineWorker(Executors.newSingleThreadExecutor(new
                        ThreadFactoryBuilder().setNameFormat("state-machine-worker-%d").build()))
                .config(config)
                .build();
        this.logReplicationFSM = new LogReplicationFSM(context);
    }

    /**
     * Signal start of snapshot sync.
     *
     * A snapshot is a consistent view of the database at a given timestamp.
     *
     * @param context snapshot sync context
     */
    public void startSnapshotSync(SnapshotSyncContext context) {
        // Verify if we really need SnapshotSyncContext, and if we need to pass to the fsm

        // Enqueue event into Log Replication FSM
        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST));
    }

    /**
     * Signal start of replication.
     *
     * Connectivity and data transmission is provided by the application requiring log replication.
     * This method should be called upon connectivity to a remote site.
     */
    public void startReplication() {
        // Enqueue event into Log Replication FSM
        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.REPLICATION_START));
    }

    /**
     * Signal to stop log replication.
     */
    public void stopReplication() {
        // Enqueue event into Log Replication FSM
        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.REPLICATION_STOP));
    }

    /**
     * Signal to cancel snapshot sync.
     *
     * @param context snapshot sync context
     */
    public void cancelSnapshotSync(SnapshotSyncContext context) {
        // Enqueue event into Log Replication FSM
        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_CANCEL));
    }
}
