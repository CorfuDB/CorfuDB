package org.corfudb.logreplication;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.receive.DataReceiver;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.logreplication.fsm.LogReplicationEvent;
import org.corfudb.logreplication.fsm.LogReplicationFSM;
import org.corfudb.logreplication.fsm.ObservableAckMsg;
import org.corfudb.logreplication.runtime.LogReplicationClient;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.logreplication.send.CorfuDataSender;
import org.corfudb.logreplication.send.logreader.DefaultReadProcessor;
import org.corfudb.logreplication.send.LogReplicationEventMetadata;
import org.corfudb.logreplication.send.logreader.ReadProcessor;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.logreplication.fsm.LogReplicationEvent.LogReplicationEventType;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class represents the Log Replication Manager at the source site.
 *
 * It is the entry point for log replication at the sender, it allows
 * initializing, stopping or cancelling log replication. It also
 * provides the interface to receive messages from the DataSender (Data Path)
 * or DataControl (Control Path).
 *
 **/
@Data
@Slf4j
public class LogReplicationSourceManager implements DataReceiver {

    private CorfuRuntime runtime;
    /*
     * Default number of Log Replication State Machine Worker Threads
     */
    private static final int DEFAULT_FSM_WORKER_THREADS = 1;

    /*
     *  Log Replication State Machine
     */
    @VisibleForTesting
    private final LogReplicationFSM logReplicationFSM;

    @VisibleForTesting
    private int countACKs = 0;

    @VisibleForTesting
    private ObservableAckMsg ackMessages = new ObservableAckMsg();

    /**
     * Constructor Source (default)
     *
     * @param runtime Corfu Runtime
     * @param dataSender implementation of a data sender, both snapshot and log entry, this represents
     *                   the application callback for data transmission
     * @param config Log Replication Configuration
     */
    public LogReplicationSourceManager(CorfuRuntime runtime,
                                       DataSender dataSender,
                                       LogReplicationConfig config) {

        this(runtime, dataSender, config, Executors.newFixedThreadPool(DEFAULT_FSM_WORKER_THREADS, new
                ThreadFactoryBuilder().setNameFormat("state-machine-worker").build()));
    }

    public LogReplicationSourceManager(String localEndpoint, LogReplicationClient client, LogReplicationConfig config) {
        this(CorfuRuntime.fromParameters(CorfuRuntimeParameters.builder().build()).parseConfigurationString(localEndpoint).connect(),
                client, config);
    }

    /**
     * Constructor LogReplicationSourceManager
     *
     * @param runtime Corfu Runtime
     * @param config Log Replication Configuration
     */
    public LogReplicationSourceManager(CorfuRuntime runtime, LogReplicationClient client, LogReplicationConfig config) {
        this(runtime, new CorfuDataSender(client), config);
    }

    /**
     * Constructor Source (default)
     *
     * @param runtime Corfu Runtime
     * @param dataSender implementation of a data sender, both snapshot and log entry, this represents
     *                   the application callback for data transmission
     * @param readProcessor implementation for reads processor (data transformation)
     * @param config Log Replication Configuration
     */
    public LogReplicationSourceManager(CorfuRuntime runtime,
                                       DataSender dataSender,
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
    public LogReplicationSourceManager(CorfuRuntime runtime,
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
    public LogReplicationSourceManager(CorfuRuntime runtime,
                                       DataSender dataSender,
                                       ReadProcessor readProcessor,
                                       LogReplicationConfig config,
                                       ExecutorService logReplicationFSMWorkers) {
        if (config.getStreamsToReplicate() == null || config.getStreamsToReplicate().isEmpty()) {
            // Avoid FSM being initialized if there are no streams to replicate
            throw new IllegalArgumentException("Invalid Log Replication: Streams to replicate is EMPTY");
        }

        // If this runtime has opened other streams, it appends non opaque entries and because
        // the cache is shared we end up doing deserialization. We need guarantees that this runtime is dedicated
        // for log replication exclusively.
        this.runtime = CorfuRuntime.fromParameters(runtime.getParameters());
        this.runtime.parseConfigurationString(runtime.getLayoutServers().get(0)).connect();

        this.logReplicationFSM = new LogReplicationFSM(this.runtime, config, dataSender, readProcessor,
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
        log.info("Start Snapshot Sync for request: {}", snapshotSyncRequest.getEventID());
        logReplicationFSM.input(snapshotSyncRequest);
        return snapshotSyncRequest.getEventID();
    }

    /**
     * Signal start of replication.
     *
     * Connectivity and data transmission is provided by the application requiring log replication.
     * This method should be called upon connectivity to a remote site.
     */
    public UUID startReplication() {
        // Enqueue event into Log Replication FSM
        LogReplicationEvent replicationStart = new LogReplicationEvent(LogReplicationEventType.REPLICATION_START);
        log.info("Start Log Entry Sync for request: {}", replicationStart.getEventID());
        logReplicationFSM.input(replicationStart);
        return replicationStart.getEventID();
    }

    /**
     * Signal to cancel snapshot send.
     *
     * @param snapshotSyncId identifier of the snapshot sync task to cancel.
     */
    public void cancelSnapshotSync(UUID snapshotSyncId) {
        log.info("Cancel Snapshot Sync for request: {}", snapshotSyncId);
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
        LogReplicationEvent logReplicationEvent = new LogReplicationEvent(LogReplicationEventType.REPLICATION_STOP);
        logReplicationFSM.input(logReplicationEvent);

        try {
            synchronized (logReplicationEvent) {
                logReplicationEvent.wait();
            }
        } catch (InterruptedException e) {
            log.error("Caught an exception ", e);
            //System.out.print("\n**** caught an exception");
        }

        log.info("Shutdown Log Replication.");
        this.runtime.shutdown();
    }

    @Override
    public LogReplicationEntry receive(LogReplicationEntry message) {
        log.trace("Data Message received on source");

        countACKs++;
        ackMessages.setValue(message);

        // Process ACKs from Application, for both, log entry and snapshot sync.
        if(message.getMetadata().getMessageMetadataType() == MessageType.LOG_ENTRY_REPLICATED) {
            log.debug("Log entry sync ACK received on timestamp {}", message.getMetadata().getTimestamp());
            logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.LOG_ENTRY_SYNC_REPLICATED,
                new LogReplicationEventMetadata(message.getMetadata().getSyncRequestId(), message.getMetadata().getTimestamp())));
        } else if (message.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_REPLICATED) {
            log.debug("Snapshot sync ACK received on base timestamp {}", message.getMetadata().getSnapshotTimestamp());
            logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_COMPLETE,
                    new LogReplicationEventMetadata(message.getMetadata().getSyncRequestId(), message.getMetadata().getTimestamp())));
        } else {
            log.debug("Received data message of type {} not an ACK", message.getMetadata().getMessageMetadataType());
        }

        return null;
    }

    @Override
    public LogReplicationEntry receive(List<LogReplicationEntry> messages) {
        messages.forEach(message -> receive(message));
        return null;
    }
}
