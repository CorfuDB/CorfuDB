package org.corfudb.infrastructure.logreplication.replication.send;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.fsm.ObservableAckMsg;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.DefaultReadProcessor;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.ReadProcessor;
import org.corfudb.infrastructure.logreplication.transport.IClientServerRouter;
import org.corfudb.runtime.LogReplication.LogReplicationSession;

import java.util.Set;
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

    private static final int DEFAULT_FSM_WORKER_THREADS = 1;

    private final LogReplicationFSM logReplicationFSM;

    private final LogReplicationMetadataManager metadataManager;

    private final LogReplicationAckReader ackReader;

    private int countACKs = 0;

    private ObservableAckMsg ackMessages = new ObservableAckMsg();

    private boolean isShutdown = false;


    public LogReplicationSourceManager(IClientServerRouter router,
                                       LogReplicationMetadataManager metadataManager,
                                       LogReplicationSession session, LogReplicationContext replicationContext) {
        this(metadataManager, new CorfuDataSender(router, session), session, replicationContext);
    }

    @VisibleForTesting
    public LogReplicationSourceManager(LogReplicationMetadataManager metadataManager, DataSender dataSender,
                                       LogReplicationSession session, LogReplicationContext replicationContext) {

        Set<String> streamsToReplicate = replicationContext.getConfig(session).getStreamsToReplicate();
        if (streamsToReplicate == null || streamsToReplicate.isEmpty()) {
            // Avoid FSM being initialized if there are no streams to replicate
            throw new IllegalArgumentException("Invalid Log Replication: Streams to replicate is EMPTY");
        }

        ExecutorService logReplicationFSMWorkers = Executors.newFixedThreadPool(DEFAULT_FSM_WORKER_THREADS,
                new ThreadFactoryBuilder().setNameFormat("state-machine-worker-" + session.hashCode()).build());

        this.metadataManager = metadataManager;

        // Ack Reader for Snapshot and LogEntry Sync
        this.ackReader = new LogReplicationAckReader(this.metadataManager, session, replicationContext);

        this.logReplicationFSM = new LogReplicationFSM(dataSender,
                logReplicationFSMWorkers, ackReader, session, replicationContext);
        this.ackReader.setLogEntryReader(this.logReplicationFSM.getLogEntryReader());
        this.ackReader.setLogEntrySender(this.logReplicationFSM.getLogEntrySender());
    }

    /**
     * Signal start of snapshot sync.
     *
     * A snapshot is a consistent view of the database at a given timestamp.
     *
     * @return unique identifier for this snapshot sync request.
     */
    public UUID startSnapshotSync() {
        return startSnapshotSync(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST));
    }

    private UUID startSnapshotSync(LogReplicationEvent snapshotSyncRequest) {
        log.info("Start Snapshot Sync, requestId={}, forced={}", snapshotSyncRequest.getMetadata().getSyncId(),
                snapshotSyncRequest.getMetadata().isForcedSnapshotSync());
        // Enqueue snapshot sync request into Log Replication FSM
        logReplicationFSM.input(snapshotSyncRequest);
        return snapshotSyncRequest.getMetadata().getSyncId();
    }

    /**
     * Signal start of a forced snapshot sync
     *
     * @param snapshotSyncRequestId unique identifier of the forced snapshot sync (already provided to the caller)
     */
    public void startForcedSnapshotSync(UUID snapshotSyncRequestId) {
        startSnapshotSync(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST,
                new LogReplicationEventMetadata(snapshotSyncRequestId, true), logReplicationFSM));
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
        logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.REPLICATION_STOP, logReplicationFSM));
    }

    /**
     * Shutdown Log Replication.
     *
     * Termination of the Log Replication State Machine, to enable replication a JVM restart is required.
     */
    public void shutdown() {
        // Enqueue event into Log Replication FSM
        LogReplicationEvent logReplicationEvent = new LogReplicationEvent(LogReplicationEventType.REPLICATION_STOP, logReplicationFSM);
        logReplicationFSM.input(logReplicationEvent);

        try {
            synchronized (logReplicationEvent) {
                logReplicationEvent.wait();
            }
        } catch (InterruptedException e) {
            log.error("Caught an exception during source manager shutdown ", e);
        }

        log.info("Shutdown Log Replication.");
        isShutdown = true;
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
        LogReplicationEvent replicationEvent = new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_TRANSFER_COMPLETE,
                metadata, logReplicationFSM);
        logReplicationFSM.input(replicationEvent);
    }
}
