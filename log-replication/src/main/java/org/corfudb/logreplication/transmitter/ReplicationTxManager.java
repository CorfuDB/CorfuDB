package org.corfudb.logreplication.transmitter;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.fsm.LogReplicationFSM;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.logreplication.fsm.LogReplicationEvent.LogReplicationEventType;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A class that represents the entry point to initiate log replication on the transmitter side.
 **/
@Data
@Slf4j
public class ReplicationTxManager {

    private static final int DEFAULT_FSM_WORKER_THREADS = 1;

    /*
     *  Log Replication State Machine
     */
    private final LogReplicationFSM logReplicationFSM;

    /**
     * Constructor ReplicationTxManager (default)
     *
     * @param runtime Corfu Runtime
     * @param snapshotListener implementation of a Snapshot Listener, this represents the application callback
     *                         for snapshot data transmission
     * @param logEntryListener implementation of a Log Entry Listener, this represents the application callback
     *                         for log entry data transmission
     * @param config Log Replication Configuration
     */
    public ReplicationTxManager(CorfuRuntime runtime,
                                SnapshotListener snapshotListener,
                                LogEntryListener logEntryListener,
                                LogReplicationConfig config) {

        // Default to single dedicated thread for state machine workers (perform state tasks)
        ExecutorService logReplicationFSMWorkers = Executors.newFixedThreadPool(DEFAULT_FSM_WORKER_THREADS, new
                ThreadFactoryBuilder().setNameFormat("state-machine-worker").build());

        // Default to single dedicated thread for state machine consumer (poll of the event queue)
        ExecutorService logReplicationFSMConsumer = Executors.newSingleThreadExecutor(new
                ThreadFactoryBuilder().setNameFormat("state-machine-consumer").build());

        this.logReplicationFSM = new LogReplicationFSM(runtime, config, snapshotListener, logEntryListener,
                logReplicationFSMWorkers, logReplicationFSMConsumer);
    }

    /**
     * Constructor ReplicationxManager (default)
     *
     * @param runtime Corfu Runtime
     * @param snapshotListener implementation of a Snapshot Listener, this represents the application callback
     *                         for snapshot data transmission
     * @param logEntryListener implementation of a Log Entry Listener, this represents the application callback
     *                         for log entry data transmission
     * @param readProcessor implementation for reads processor (transformation)
     * @param config Log Replication Configuration
     */
    public ReplicationTxManager(CorfuRuntime runtime,
                                SnapshotListener snapshotListener,
                                LogEntryListener logEntryListener,
                                ReadProcessor readProcessor,
                                LogReplicationConfig config) {

        // Default to single dedicated thread for state machine workers (perform state tasks)
        ExecutorService logReplicationFSMWorkers = Executors.newFixedThreadPool(DEFAULT_FSM_WORKER_THREADS, new
                ThreadFactoryBuilder().setNameFormat("state-machine-worker").build());

        // Default to single dedicated thread for state machine consumer (poll of the event queue)
        ExecutorService logReplicationFSMConsumer = Executors.newSingleThreadExecutor(new
                ThreadFactoryBuilder().setNameFormat("state-machine-consumer").build());

        this.logReplicationFSM = new LogReplicationFSM(runtime, config, snapshotListener, logEntryListener,
                logReplicationFSMWorkers, logReplicationFSMConsumer);
    }

    /**
     * Constructor ReplicationTxManager to provide ExecutorServices for FSM
     *
     * For multi-site log replication multiple managers can share a common thread pool.
     *
     * @param runtime corfu runtime
     * @param snapshotListener implementation of a Snapshot Listener, this represents the application callback
     *      *                  for snapshot data transmission
     * @param logEntryListener implementation of a Log Entry Listener, this represents the application callback
     *      *                  for log entry data transmission
     * @param config Log Replication Configuration
     * @param logReplicationFSMWorkers worker thread pool (state tasks)
     * @param logReplicationFSMConsumers consumer thread pool (FSM poll event queue)
     */
    public ReplicationTxManager(CorfuRuntime runtime,
                                SnapshotListener snapshotListener,
                                LogEntryListener logEntryListener,
                                LogReplicationConfig config,
                                ExecutorService logReplicationFSMWorkers,
                                ExecutorService logReplicationFSMConsumers) {
        this.logReplicationFSM = new LogReplicationFSM(runtime, config, snapshotListener, logEntryListener,
                logReplicationFSMWorkers, logReplicationFSMConsumers);
    }

    /**
     * Signal start of snapshot sync.
     *
     * A snapshot is a consistent view of the database at a given timestamp.
     *
     * @return unique identifier for this snapshot sync request.
     */
    public UUID startSnapshotSync() {
        // Enqueue snapshot sync request into Log Replication FSM
        LogReplicationEvent snapshotSyncRequest = new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST);
        logReplicationFSM.input(snapshotSyncRequest);
        return snapshotSyncRequest.getEventID();
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
     * Signal to cancel snapshot transmit.
     *
     * @param snapshotSyncId identifier of the snapshot sync task to cancel.
     */
    public void cancelSnapshotSync(UUID snapshotSyncId) {
        // Enqueue event into Log Replication FSM
        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_CANCEL, snapshotSyncId));
    }

    /**
     * Shutdown Log Replication.
     *
     * Termination of the Log Replication State Machine, to enable replication a JVM restart is required.
     */
    public void shutdown() {
        // Enqueue event into Log Replication FSM
        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.REPLICATION_SHUTDOWN));
    }
}
