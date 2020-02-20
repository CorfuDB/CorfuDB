package org.corfudb.logreplication;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.logreplication.fsm.LogReplicationConfig;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.fsm.LogReplicationFSM;
import org.corfudb.logreplication.fsm.ObservableValue;
import org.corfudb.logreplication.message.DataMessage;
import org.corfudb.logreplication.message.LogReplicationEntry;
import org.corfudb.logreplication.message.MessageType;
import org.corfudb.logreplication.send.DefaultReadProcessor;
import org.corfudb.logreplication.send.LogReplicationEventMetadata;
import org.corfudb.logreplication.send.ReadProcessor;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.logreplication.fsm.LogReplicationEvent.LogReplicationEventType;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class represents the Log Replication Manager at the source.
 *
 * It is the entry point for log replication at the sender.
 *
 **/
@Data
@Slf4j
public class SourceManager implements DataReceiver {

    /*
     * Default number of Log Replication State Machine Worker Threads
     */
    private static final int DEFAULT_FSM_WORKER_THREADS = 1;

    /*
     *  Log Replication State Machine
     */
    private final LogReplicationFSM logReplicationFSM;

    @VisibleForTesting
    private int countACKs = 0;

    @VisibleForTesting
    @Getter
    private ObservableValue ackMessages = new ObservableValue(countACKs);

    /**
     * Constructor Source (default)
     *
     * @param runtime Corfu Runtime
     * @param dataSender implementation of a data sender, both snapshot and log entry, this represents
     *                   the application callback for data transmission
     * @param config Log Replication Configuration
     */
    public SourceManager(CorfuRuntime runtime,
                         DataSender dataSender,
                         DataControl dataControl,
                         LogReplicationConfig config) {

        this(runtime, dataSender, config, Executors.newFixedThreadPool(DEFAULT_FSM_WORKER_THREADS, new
                ThreadFactoryBuilder().setNameFormat("state-machine-worker").build()));
    }

    /**
     * Constructor Source (default)
     *
     * @param runtime Corfu Runtime
     * @param dataSender implementation of a data sender, both snapshot and log entry, this represents
     *                   the application callback for data transmission
     * @
     * @param readProcessor implementation for reads processor (data transformation)
     * @param config Log Replication Configuration
     */
    public SourceManager(CorfuRuntime runtime,
                         DataSender dataSender,
                         DataControl dataControl,
                         ReadProcessor readProcessor,
                         LogReplicationConfig config) {
        // Default to single dedicated thread for state machine workers (perform state tasks)
        this(runtime, dataSender, readProcessor, config, Executors.newFixedThreadPool(DEFAULT_FSM_WORKER_THREADS, new
                ThreadFactoryBuilder().setNameFormat("state-machine-worker").build()));
    }

    /**
     * Constructor Source to provide ExecutorServices for FSM
     *
     * For multi-site log replication multiple managers can share a common thread pool.
     *
     * @param runtime corfu runtime
     * @param dataSender implementation of a data sender, both snapshot and log entry, this represents
     *                   the application callback for data transmission
     * @param config Log Replication Configuration
     * @param logReplicationFSMWorkers worker thread pool (state tasks)
     */
    public SourceManager(CorfuRuntime runtime,
                         DataSender dataSender,
                         LogReplicationConfig config,
                         ExecutorService logReplicationFSMWorkers) {
        this(runtime, dataSender, new DefaultReadProcessor(runtime), config, logReplicationFSMWorkers);
    }

    /**
     * Constructor Source to provide ExecutorServices for FSM
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
    public SourceManager(CorfuRuntime runtime,
                         DataSender dataSender,
                         ReadProcessor readProcessor,
                         LogReplicationConfig config,
                         ExecutorService logReplicationFSMWorkers) {
        if (config.getStreamsToReplicate() == null || config.getStreamsToReplicate().isEmpty()) {
            // Avoid FSM being initialized if there are no streams to replicate
            throw new IllegalArgumentException("Invalid Log Replication: Streams to replicate is EMPTY");
        }
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
     * Signal to cancel snapshot send.
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
        // Enqueue event into Log Replication FSM
        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.REPLICATION_SHUTDOWN));
    }

    @Override
    public void receive(DataMessage dataMessage) {
        // Convert from DataMessage to Corfu Internal (deserialize)
        LogReplicationEntry message = LogReplicationEntry.deserialize(dataMessage.getData());

        countACKs++;
        ackMessages.setValue(countACKs);

        // Process ACKs from Application, for both, log entry and snapshot sync.
        if(message.getMetadata().getMessageMetadataType() == MessageType.LOG_ENTRY_REPLICATED) {
            log.debug("Log entry sync ACK received on timestamp {}", message.getMetadata().getTimestamp());
            logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.LOG_ENTRY_SYNC_REPLICATED,
                new LogReplicationEventMetadata(message.getMetadata().getTimestamp())));
        } else if (message.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_REPLICATED) {
            log.debug("Snapshot sync ACK received on base timestamp {}", message.getMetadata().getSnapshotTimestamp());
            logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_COMPLETE,
                    new LogReplicationEventMetadata(message.getMetadata().getSnapshotRequestId(), message.getMetadata().getTimestamp())));
        } else {
            log.debug("Received data message of type {} not an ACK", message.getMetadata().getMessageMetadataType());
        }
    }

    @Override
    public void receive(List<DataMessage> messages) {
        messages.forEach(message -> receive(message));
    }
}
