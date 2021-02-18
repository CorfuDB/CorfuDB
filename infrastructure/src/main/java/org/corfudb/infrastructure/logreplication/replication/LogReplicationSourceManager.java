package org.corfudb.infrastructure.logreplication.replication;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.fsm.ObservableAckMsg;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.send.CorfuDataSender;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationEventMetadata;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.DefaultReadProcessor;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.ReadProcessor;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClient;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class represents the Log Replication Manager at the source cluster.
 *
 * It is the entry point for log replication at the sender, it allows
 * initializing, stopping or cancelling log replication. It also
 * provides the interface to receive messages from the DataSender (Data Path)
 * or DataControl (Control Path).
 *
 **/
@Data
@Slf4j
public class LogReplicationSourceManager {

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

    /*
     * Log Replication Runtime Parameters
     */
    private final LogReplicationRuntimeParameters parameters;

    /*
     * Log Replication Configuration
     */
    private final LogReplicationConfig config;

    /*
     * Log Replication MetadataManager.
     */
    private final LogReplicationMetadataManager metadataManager;

    private final LogReplicationAckReader ackReader;

    @VisibleForTesting
    private int countACKs = 0;

    @VisibleForTesting
    private ObservableAckMsg ackMessages = new ObservableAckMsg();

    /**
     * @param params Log Replication parameters
     * @param client LogReplication client, which is a data sender, both snapshot and log entry, this represents
     *              the application callback for data transmission
     * @param metadataManager Replication Metadata Manager
     */
    public LogReplicationSourceManager(LogReplicationRuntimeParameters params, LogReplicationClient client,
                                       LogReplicationMetadataManager metadataManager) {
        this(params, metadataManager, new CorfuDataSender(client));
    }

    @VisibleForTesting
    public LogReplicationSourceManager(LogReplicationRuntimeParameters params,
                                       LogReplicationMetadataManager metadataManager,
                                       DataSender dataSender) {

        this.runtime = CorfuRuntime.fromParameters(CorfuRuntimeParameters.builder()
                .trustStore(params.getTrustStore())
                .tsPasswordFile(params.getTsPasswordFile())
                .keyStore(params.getKeyStore())
                .ksPasswordFile(params.getKsPasswordFile())
                .systemDownHandler(params.getSystemDownHandler())
                .tlsEnabled(params.isTlsEnabled()).build());
        runtime.parseConfigurationString(params.getLocalCorfuEndpoint()).connect();

        this.parameters = params;

        this.config = parameters.getReplicationConfig();

        log.debug("{}", config);

        if (config.getStreamsToReplicate() == null || config.getStreamsToReplicate().isEmpty()) {
            // Avoid FSM being initialized if there are no streams to replicate
            throw new IllegalArgumentException("Invalid Log Replication: Streams to replicate is EMPTY");
        }

        ExecutorService logReplicationFSMWorkers = Executors.newFixedThreadPool(DEFAULT_FSM_WORKER_THREADS, new
                ThreadFactoryBuilder().setNameFormat("state-machine-worker").build());
        ReadProcessor readProcessor = new DefaultReadProcessor(runtime);
        this.metadataManager = metadataManager;
        // Ack Reader for Snapshot and LogEntry Sync
        this.ackReader = new LogReplicationAckReader(this.metadataManager, config, runtime,
                params.getRemoteClusterDescriptor().getClusterId());

        this.logReplicationFSM = new LogReplicationFSM(this.runtime, config, params.getRemoteClusterDescriptor(),
                dataSender, readProcessor, logReplicationFSMWorkers, ackReader);

        this.logReplicationFSM.setTopologyConfigId(params.getTopologyConfigId());
        this.ackReader.startAckReader(this.logReplicationFSM.getLogEntryReader());
    }

    /**
     * Signal start of snapshot sync.
     *
     * A snapshot is a consistent view of the database at a given timestamp.
     *
     * @return unique identifier for this snapshot sync request.
     */
    public UUID startSnapshotSync() {
        return startSnapshotSync(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST), false);
    }

    /**
     * Signal start of a forced snapshot sync
     *
     * @param snapshotSyncRequestId unique identifier of the forced snapshot sync (already provided to the caller)
     */
    public void startForcedSnapshotSync(UUID snapshotSyncRequestId) {
        startSnapshotSync(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST, snapshotSyncRequestId), true);
    }

    private UUID startSnapshotSync(LogReplicationEvent snapshotSyncRequest, boolean forced) {
        log.info("Start Snapshot Sync, requestId={}, forced={}", snapshotSyncRequest.getEventId(), forced);
        // Enqueue snapshot sync request into Log Replication FSM
        logReplicationFSM.input(snapshotSyncRequest);
        return snapshotSyncRequest.getEventId();
    }

    /**
     * Signal start of replication.
     *
     * Connectivity and data transmission is provided by the application requiring log replication.
     * This method should be called upon connectivity to a remote cluster.
     */
    public void startReplication(LogReplicationEvent replicationEvent) {
        // Enqueue event into Log Replication FSM
        log.info("Start replication event {}", replicationEvent);
        logReplicationFSM.input(replicationEvent);
    }

    /**
     * Stop Log Replication
     */
    public void stopLogReplication() {
        log.info("Stop Log Replication");
        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.REPLICATION_STOP));
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
        }

        log.info("Shutdown Log Replication.");
        ackReader.shutdown();
        this.runtime.shutdown();
    }

    /**
     * Resume a snapshot sync that is in progress.
     *
     * To resume a snapshot sync means that the data transfer has completed,
     * and we're waiting for the apply to complete on the receiver's end.
     *
     * If a past snapshot sync transfer has not finished, a new snapshot sync is started.
     *
     * @param metadata
     */
    public void resumeSnapshotSync(LogReplicationEventMetadata metadata) {
        LogReplicationEvent replicationEvent = new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_TRANSFER_COMPLETE, metadata);
        logReplicationFSM.input(replicationEvent);
    }
}
