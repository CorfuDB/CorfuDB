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

    /*
      Log Replication State Machine
     */
    private final LogReplicationFSM logReplicationFSM;

    /**
     * Constructor ReplicationTxManager
     *
     * @param runtime Corfu Runtime
     * @param snapshotListener implementation of a Snapshot Listener, this represents the application callback
     *                         for snapshot data transmission.
     * @param logEntryListener implementation of a Log Entry Listener, this represents the application callback
     *                         for log entry data transmission.
     * @param config Log Replication Configuration
     */
    public ReplicationTxManager(CorfuRuntime runtime,
                                SnapshotListener snapshotListener,
                                LogEntryListener logEntryListener,
                                LogReplicationConfig config) {

        ExecutorService logReplicationFSMWorkers = Executors.newFixedThreadPool(config.getLogReplicationFSMNumWorkers(), new
                ThreadFactoryBuilder().setNameFormat("state-machine-worker-%d").build());

        this.logReplicationFSM = new LogReplicationFSM(runtime, config, snapshotListener, logEntryListener,
                logReplicationFSMWorkers);
    }

    /**
     * Constructor ReplicationTxManager
     *
     * @param runtime
     * @param logEntryListener
     * @param snapshotReader
     * @param config
     */
    public ReplicationTxManager(CorfuRuntime runtime,
                                LogEntryListener logEntryListener,
                                SnapshotListener snapshotListener,
                                SnapshotReader snapshotReader,
                                LogEntryReader logEntryReader,
                                LogReplicationConfig config) {
        ExecutorService logReplicationFSMWorkers = Executors.newSingleThreadExecutor(new
                ThreadFactoryBuilder().setNameFormat("state-machine-worker-%d").build());
        this.logReplicationFSM = new LogReplicationFSM(runtime, config, snapshotReader, snapshotListener,
                logEntryReader, logEntryListener, logReplicationFSMWorkers);
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
}
