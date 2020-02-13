package org.corfudb.logreplication;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.fsm.LogReplicationFSM;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.transmit.LogReplicationEventMetadata;
import org.corfudb.logreplication.transmit.ReadProcessor;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.logreplication.fsm.LogReplicationEvent.LogReplicationEventType;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A class that represents the entry point to initiate log replication on the transmit side.
 **/
@Data
@Slf4j
public class ReplicationSourceManager implements DataReceiver {

    private static final int DEFAULT_FSM_WORKER_THREADS = 1;

    /*
     *  Log Replication State Machine
     */
    private final LogReplicationFSM logReplicationFSM;

    /**
     * Constructor ReplicationSourceManager (default)
     *
     * @param runtime Corfu Runtime
     * @param dataSender implementation of a data sender, both snapshot and log entry, this represents
     *                   the application callback for data transmission
     * @param config Log Replication Configuration
     */
    public ReplicationSourceManager(CorfuRuntime runtime,
                                    DataSender dataSender,
                                    LogReplicationConfig config) {

        this(runtime, dataSender, config, Executors.newFixedThreadPool(DEFAULT_FSM_WORKER_THREADS, new
                ThreadFactoryBuilder().setNameFormat("state-machine-worker").build()));
    }

    /**
     * Constructor ReplicationSourceManager (default)
     *
     * @param runtime Corfu Runtime
     * @param dataSender implementation of a data sender, both snapshot and log entry, this represents
     *                   the application callback for data transmission
     * @param readProcessor implementation for reads processor (data transformation)
     * @param config Log Replication Configuration
     */
    public ReplicationSourceManager(CorfuRuntime runtime,
                                    DataSender dataSender,
                                    ReadProcessor readProcessor,
                                    LogReplicationConfig config) {

        // Default to single dedicated thread for state machine workers (perform state tasks)

        // Default to single dedicated thread for state machine consumer (poll of the event queue)
        this(runtime, dataSender, readProcessor, config, Executors.newFixedThreadPool(DEFAULT_FSM_WORKER_THREADS, new
                ThreadFactoryBuilder().setNameFormat("state-machine-worker").build()));
    }

    /**
     * Constructor ReplicationSourceManager to provide ExecutorServices for FSM
     *
     * For multi-site log replication multiple managers can share a common thread pool.
     *
     * @param runtime corfu runtime
     * @param dataSender implementation of a data sender, both snapshot and log entry, this represents
     *                   the application callback for data transmission
     * @param config Log Replication Configuration
     * @param logReplicationFSMWorkers worker thread pool (state tasks)
     */
    public ReplicationSourceManager(CorfuRuntime runtime,
                                    DataSender dataSender,
                                    LogReplicationConfig config,
                                    ExecutorService logReplicationFSMWorkers) {
        this.logReplicationFSM = new LogReplicationFSM(runtime, config, dataSender, logReplicationFSMWorkers);
    }

    /**
     * Constructor ReplicationSourceManager to provide ExecutorServices for FSM
     *
     * For multi-site log replication multiple managers can share a common thread pool.
     *
     * @param runtime corfu runtime
     * @param dataSender implementation of a data sender, both snapshot and log entry, this represents
     *                   the application callback for data transmission
     * @param readProcessor implementation for reads processor (transformation)
     * @param config Log Replication Configuration
     * @param logReplicationFSMWorkers worker thread pool (state tasks)
     */
    public ReplicationSourceManager(CorfuRuntime runtime,
                                    DataSender dataSender,
                                    ReadProcessor readProcessor,
                                    LogReplicationConfig config,
                                    ExecutorService logReplicationFSMWorkers) {
        this.logReplicationFSM = new LogReplicationFSM(runtime, config, dataSender, readProcessor,
                logReplicationFSMWorkers);
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
        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.SYNC_CANCEL,
                new LogReplicationEventMetadata(snapshotSyncId)));
    }

    /**
     * Shutdown Log Replication.
     *
     * Termination of the Log Replication State Machine, to enable replication a JVM restart is required.
     */
    public void shutdown() {
        System.out.println("Explicit shutdown");
        // Enqueue event into Log Replication FSM
        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.REPLICATION_SHUTDOWN));
    }

    @Override
    public void receive(DataMessage message) {

    }

    @Override
    public void receive(List<DataMessage> messages) {

    }

//    /**
//     * TODO add comment
//     *
//     * @param requestId unique identifier for the snapshot sync request
//     */
//    public void snapshotReplicated(UUID requestId) {
//        // Enqueue event into Log Replication FSM
//        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_COMPLETE,
//                new LogReplicationEventMetadata(requestId)));
//    }
//
//    /**
//     * Process ack from replication receive side.
//     *
//     * @param timestamp
//     */
//    public void logEntryReplicated(long timestamp) {
//        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.LOG_ENTRY_SYNC_REPLICATED,
//                new LogReplicationEventMetadata(timestamp)));
//    }

}
