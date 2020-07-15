package org.corfudb.infrastructure.logreplication.replication;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.LogReplicationRuntimeParameters;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.replication.fsm.ObservableAckMsg;
import org.corfudb.infrastructure.logreplication.replication.receive.DataReceiver;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClient;
import org.corfudb.infrastructure.logreplication.replication.send.CorfuDataSender;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.DefaultReadProcessor;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationEventMetadata;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.ReadProcessor;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuRuntime.CorfuRuntimeParameters;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.runtime.view.Address;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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

    /*
     * Last ack'd timestamp from Receiver
     */
    private AtomicLong lastAckedTimestamp;

    /*
     * Periodic Thread which reads the last Acked Timestamp and writes it to the metadata table
     */
    ScheduledExecutorService lastAckedTsPoller = Executors.newSingleThreadScheduledExecutor();

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
        this.runtime = CorfuRuntime.fromParameters(CorfuRuntimeParameters.builder()
                .trustStore(params.getTrustStore())
                .tsPasswordFile(params.getTsPasswordFile())
                .keyStore(params.getKeyStore())
                .ksPasswordFile(params.getKsPasswordFile())
                .tlsEnabled(params.isTlsEnabled()).build());
        runtime.parseConfigurationString(runtime.getLayoutServers().get(0)).connect();

        this.parameters = params;

        this.config = parameters.getReplicationConfig();
        if (config.getStreamsToReplicate() == null || config.getStreamsToReplicate().isEmpty()) {
            // Avoid FSM being initialized if there are no streams to replicate
            throw new IllegalArgumentException("Invalid Log Replication: Streams to replicate is EMPTY");
        }

        DataSender dataSender = new CorfuDataSender(client);
        ExecutorService logReplicationFSMWorkers = Executors.newFixedThreadPool(DEFAULT_FSM_WORKER_THREADS, new
                ThreadFactoryBuilder().setNameFormat("state-machine-worker").build());
        ReadProcessor readProcessor = new DefaultReadProcessor(runtime);
        this.logReplicationFSM = new LogReplicationFSM(this.runtime, config, params.getRemoteClusterDescriptor(),
                dataSender, readProcessor, logReplicationFSMWorkers);

        this.logReplicationFSM.setTopologyConfigId(params.getTopologyConfigId());

        this.metadataManager = metadataManager;
        lastAckedTsPoller.scheduleWithFixedDelay(new TsPollingTask(), 0, 15, TimeUnit.SECONDS);
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
     * This method should be called upon connectivity to a remote cluster.
     */
    public void startReplication(LogReplicationEvent replicationEvent) {
        // Enqueue event into Log Replication FSM
        log.info("Start replication event {}", replicationEvent);
        logReplicationFSM.input(replicationEvent);
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
            lastAckedTimestamp.set(message.getMetadata().getTimestamp());
            logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.LOG_ENTRY_SYNC_REPLICATED,
                new LogReplicationEventMetadata(message.getMetadata().getSyncRequestId(), message.getMetadata().getTimestamp())));
        } else if (message.getMetadata().getMessageMetadataType() == MessageType.SNAPSHOT_REPLICATED) {
            log.debug("Snapshot sync ACK received on base timestamp {}", message.getMetadata().getSnapshotTimestamp());
            lastAckedTimestamp.set(message.getMetadata().getTimestamp());
            logReplicationFSM.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_COMPLETE,
                    new LogReplicationEventMetadata(message.getMetadata().getSyncRequestId(), message.getMetadata().getTimestamp())));
        } else {
            log.debug("Received data message of type {} not an ACK", message.getMetadata().getMessageMetadataType());
        }
        return null;
    }

    /**
     * For the given replication runtime, query max stream tail for all streams to be replicated.
     *
     * @return max tail of all streams to be replicated for the given runtime
     */
    private long queryStreamTail() {
        Map<UUID, Long> tailMap = runtime.getAddressSpaceView().getAllTails().getStreamTails();
        long maxTail = Address.NON_ADDRESS;
        for (String s : config.getStreamsToReplicate()) {
            UUID streamUuid = CorfuRuntime.getStreamID(s);
            if (tailMap.get(streamUuid) != null) {
                long streamTail = tailMap.get(streamUuid);
                maxTail = Math.max(maxTail, streamTail);
            }
        }
        return maxTail;
    }

    /**
     * Given a timestamp, calculate how many entries to be sent for all replicated streams.
     *
     * @param
     */
    private int calculateRemainingEntriesToSend(long ackedTimestamp) {
        long timestamp = queryStreamTail();
        long remainingEntriesToSend = timestamp - ackedTimestamp;
        int percentDone = (int) (ackedTimestamp/timestamp * 100);
        return percentDone;
    }

    /**
     * Task which periodically updates the metadata table with replication completion percentage
     */
    private class TsPollingTask implements Runnable {
        @Override
        public void run() {
            int percentComplete = calculateRemainingEntriesToSend(lastAckedTimestamp.get());
            metadataManager.setReplicationStatus(Integer.toString(percentComplete));
        }
    }
}
