package org.corfudb.infrastructure.logreplication.replication.send;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.TextFormat;
import io.micrometer.core.instrument.Tag;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MeterRegistryProvider;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.infrastructure.LogReplicationContext;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.ReadProcessor;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReadMessage;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.corfudb.infrastructure.logreplication.config.LogReplicationConfig.DEFAULT_MAX_NUM_MSG_PER_BATCH;
import static org.corfudb.infrastructure.logreplication.config.LogReplicationConfig.DEFAULT_TIMEOUT_MS;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLrEntryAckMsg;

/**
 * This class is responsible of transmitting a consistent view of the data at a given timestamp,
 * i.e, reading and sending a snapshot of the data for the requested streams.
 * <p>
 * It reads log entries from the data-store through the SnapshotReader, and hands it to the
 * DataSender (the application specific callback for sending data to the remote cluster).
 * <p>
 * The SnapshotReader has a default implementation based on reads at the stream layer
 * (no serialization/deserialization) required.
 * <p>
 * DataSender is implemented by the application, as communication channels between sites are out of the scope
 * of CorfuDB.
 */
@Slf4j
public class SnapshotSender {

    private CorfuRuntime runtime;
    private SnapshotReader snapshotReader;
    @Getter
    private SenderBufferManager dataSenderBufferManager;
    private LogReplicationFSM fsm;

    @Getter
    private long baseSnapshotTimestamp;

    // The max number of message can be sent over in burst for a snapshot cycle.
    private final int maxNumSnapshotMsgPerBatch;

    // This flag will indicate the start of a snapshot sync, so start snapshot marker is sent once.
    private boolean startSnapshotSync = true;

    private boolean completed = false;

    @Getter
    @VisibleForTesting
    // For testing purposes, used to count the number of messages sent in order to interrupt snapshot sync
    private ObservableValue observedCounter = new ObservableValue(0);

    private final Optional<AtomicLong> messageCounter;

    private volatile AtomicBoolean stopSnapshotSync = new AtomicBoolean(false);

    public SnapshotSender(LogReplicationContext replicationContext, SnapshotReader snapshotReader, DataSender dataSender,
                          LogReplicationFSM fsm) {
        this.runtime = replicationContext.getCorfuRuntime();
        this.snapshotReader = snapshotReader;
        this.fsm = fsm;
        int snapshotSyncBatchSize = replicationContext.getConfig(fsm.getSession()).getMaxNumMsgPerBatch();
        this.maxNumSnapshotMsgPerBatch = snapshotSyncBatchSize <= 0 ? DEFAULT_MAX_NUM_MSG_PER_BATCH : snapshotSyncBatchSize;
        this.dataSenderBufferManager = new SnapshotSenderBufferManager(dataSender, fsm.getAckReader());
        this.messageCounter = MeterRegistryProvider.getInstance().map(registry ->
                registry.gauge("logreplication.messages",
                        ImmutableList.of(Tag.of("replication.type", "snapshot")),
                        new AtomicLong(0)));
    }

    private CompletableFuture<LogReplicationEntryMsg> snapshotSyncAck;

    /**
     * Initiate Snapshot Sync, this entails reading and sending data for a given snapshot.
     *
     * @param snapshotSyncEventId identifier of the event that initiated the snapshot sync
     */
    public void transmit(UUID snapshotSyncEventId) {

        log.info("Running snapshot sync for {} on baseSnapshot {}", snapshotSyncEventId,
                baseSnapshotTimestamp);

        boolean cancel = false;     // Flag indicating snapshot sync needs to be canceled
        int messagesSent = 0;       // Limit the number of messages to maxNumSnapshotMsgPerBatch. The reason we need to limit
        // is because by design several state machines can share the same thread pool,
        // therefore, we need to hand the thread for other workers to execute.
        SnapshotReadMessage snapshotReadMessage;

        // Skip if no data is present in the log
        if (Address.isAddress(baseSnapshotTimestamp)) {
            // Read and Send Batch Size messages, unless snapshot is completed before (endRead)
            // or snapshot sync is stopped
            dataSenderBufferManager.resend();

            while (messagesSent < maxNumSnapshotMsgPerBatch && !dataSenderBufferManager.getPendingMessages().isFull() &&
                    !completed && !stopSnapshotSync.get()) {

                try {
                    snapshotReadMessage = snapshotReader.read(snapshotSyncEventId);
                    completed = snapshotReadMessage.isEndRead();
                    // Data Transformation / Processing
                    // readProcessor.process(snapshotReadMessage.getMessages())
                } catch (TrimmedException te) {
                    log.warn("Cancel snapshot sync due to trimmed exception.", te);
                    dataSenderBufferManager.reset(Address.NON_ADDRESS);
                    snapshotSyncCancel(snapshotSyncEventId, LogReplicationError.TRIM_SNAPSHOT_SYNC, false);
                    cancel = true;
                    break;
                } catch (Exception e) {
                    log.error("Caught exception during snapshot sync", e);
                    log.error("Print stacktrace from thread {}", Arrays.toString(Thread.currentThread().getStackTrace()));

                    boolean timeoutException = false;
                    if (e instanceof RuntimeException && e.getCause() instanceof TimeoutException) {
                        log.info("Snapshot sync timed out waiting for data.  Will request a new snapshot sync");
                        timeoutException = true;
                    }
                    snapshotSyncCancel(snapshotSyncEventId, LogReplicationError.UNKNOWN, timeoutException);
                    cancel = true;
                    break;
                }

                messagesSent += processReads(snapshotReadMessage.getMessages(), snapshotSyncEventId, completed);
                final long messagesSentSnapshot = messagesSent;
                messageCounter.ifPresent(counter -> counter.addAndGet(messagesSentSnapshot));
                observedCounter.setValue(messagesSent);
            }

            if (completed && dataSenderBufferManager.pendingMessages.isEmpty()) {
                // Snapshot Sync Transfer Completed
                log.info("Snapshot sync transfer completed for {} on timestamp={}", snapshotSyncEventId,
                        baseSnapshotTimestamp);
                snapshotSyncTransferComplete(snapshotSyncEventId);
                completed = false;
            } else if (!cancel && !stopSnapshotSync.get()) {
                // Maximum number of batch messages sent. This snapshot sync needs to continue.

                // Snapshot Sync is not performed in a single run, as for the case of multi-cluster replication
                // the shared thread pool could be lower than the number of sites, so we assign resources in
                // a round robin fashion.
                log.trace("Snapshot sync continue for {} on timestamp {}", snapshotSyncEventId, baseSnapshotTimestamp);
                fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_CONTINUE,
                        new LogReplicationEventMetadata(snapshotSyncEventId)));
            }
        } else {
            log.info("Snapshot sync completed for {} as there is no data in the log.", snapshotSyncEventId);

            try {
                dataSenderBufferManager.sendWithBuffering(getSnapshotSyncStartMarker(snapshotSyncEventId));
                snapshotSyncAck = dataSenderBufferManager.sendWithBuffering(getSnapshotSyncEndMarker(snapshotSyncEventId));
                snapshotSyncAck.get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                snapshotSyncTransferComplete(snapshotSyncEventId);
            } catch (Exception e) {
                log.warn("Caught exception while sending data to sink.", e);
                snapshotSyncCancel(snapshotSyncEventId, LogReplicationError.UNKNOWN, false);
            }
        }
    }

    private int processReads(List<LogReplicationEntryMsg> logReplicationEntries,
                             UUID snapshotSyncEventId,
                             boolean completed) {
        int numMessages = 0;

        // If we are starting a snapshot sync, send a start marker.
        if (startSnapshotSync) {
            if (fsm.getSession().getSubscriber().getModel() == LogReplication.ReplicationModel.ROUTING_QUEUES) {
                resetBaseSnapshotTimestamp(logReplicationEntries.get(0).getMetadata().getSnapshotTimestamp());
            }

            dataSenderBufferManager.sendWithBuffering(getSnapshotSyncStartMarker(snapshotSyncEventId));
            startSnapshotSync = false;
            numMessages++;
        }

        if (MeterRegistryProvider.getInstance().isPresent()) {
            dataSenderBufferManager.sendWithBuffering(logReplicationEntries,
                    "logreplication.sender.duration.nanoseconds",
                    Tag.of("replication.type", "snapshot"));
        } else {
            dataSenderBufferManager.sendWithBuffering(logReplicationEntries);
        }

        // If Snapshot is complete, add end marker
        if (completed) {
            LogReplicationEntryMsg endDataMessage = getSnapshotSyncEndMarker(snapshotSyncEventId);
            log.info("SnapshotSender sent out SNAPSHOT_END message {} ", endDataMessage.getMetadata());
            snapshotSyncAck = dataSenderBufferManager.sendWithBuffering(endDataMessage);
            numMessages++;
        }

        return numMessages + logReplicationEntries.size();
    }

    /**
     * Prepare a Snapshot Sync Replication start marker.
     *
     * @param snapshotSyncEventId snapshot sync event identifier
     * @return snapshot sync start marker as LogReplicationEntry
     */
    private LogReplicationEntryMsg getSnapshotSyncStartMarker(UUID snapshotSyncEventId) {
        LogReplication.LogReplicationEntryMetadataMsg metadata = LogReplication.LogReplicationEntryMetadataMsg.newBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_START)
                .setTopologyConfigID(fsm.getTopologyConfigId())
                .setSyncRequestId(getUuidMsg(snapshotSyncEventId))
                .setTimestamp(Address.NON_ADDRESS)
                .setPreviousTimestamp(Address.NON_ADDRESS)
                .setSnapshotTimestamp(baseSnapshotTimestamp)
                .setSnapshotSyncSeqNum(Address.NON_ADDRESS)
                .build();
        return getLrEntryAckMsg(metadata);
    }

    private LogReplicationEntryMsg getSnapshotSyncStartMarker(long timestamp, UUID snapshotSyncEventId) {
        LogReplication.LogReplicationEntryMetadataMsg metadata = LogReplication.LogReplicationEntryMetadataMsg.newBuilder()
            .setEntryType(LogReplicationEntryType.SNAPSHOT_START)
            .setTopologyConfigID(fsm.getTopologyConfigId())
            .setSyncRequestId(getUuidMsg(snapshotSyncEventId))
            .setTimestamp(Address.NON_ADDRESS)
            .setPreviousTimestamp(Address.NON_ADDRESS)
            .setSnapshotTimestamp(timestamp)
            .setSnapshotSyncSeqNum(Address.NON_ADDRESS)
            .build();
        return getLrEntryAckMsg(metadata);
    }

    private LogReplicationEntryMsg getSnapshotSyncEndMarker(UUID snapshotSyncEventId) {
        LogReplication.LogReplicationEntryMetadataMsg metadata = LogReplication.LogReplicationEntryMetadataMsg.newBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_END)
                .setTopologyConfigID(fsm.getTopologyConfigId())
                .setSyncRequestId(getUuidMsg(snapshotSyncEventId))
                .setTimestamp(Address.NON_ADDRESS)
                .setPreviousTimestamp(Address.NON_ADDRESS)
                .setSnapshotTimestamp(baseSnapshotTimestamp)
                .setSnapshotSyncSeqNum(Address.NON_ADDRESS)
                .build();
        return getLrEntryAckMsg(metadata);
    }

    private LogReplicationEntryMsg getSnapshotSyncEndMarker(long timestamp, UUID snapshotSyncEventId) {
        LogReplication.LogReplicationEntryMetadataMsg metadata = LogReplication.LogReplicationEntryMetadataMsg.newBuilder()
            .setEntryType(LogReplicationEntryType.SNAPSHOT_END)
            .setTopologyConfigID(fsm.getTopologyConfigId())
            .setSyncRequestId(getUuidMsg(snapshotSyncEventId))
            .setTimestamp(Address.NON_ADDRESS)
            .setPreviousTimestamp(Address.NON_ADDRESS)
            .setSnapshotTimestamp(timestamp)
            .setSnapshotSyncSeqNum(Address.NON_ADDRESS)
            .build();
        return getLrEntryAckMsg(metadata);
    }

    /**
     * Complete Snapshot Sync transfer, insert completion event in the FSM queue.
     *
     * @param snapshotSyncEventId unique identifier for the completed snapshot sync.
     */
    private void snapshotSyncTransferComplete(UUID snapshotSyncEventId) {
        // We need to bind the internal event (COMPLETE) to the snapshotSyncEventId that originated it, this way
        // the state machine can correlate to the corresponding state (in case of delayed events)
        fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_TRANSFER_COMPLETE,
                new LogReplicationEventMetadata(snapshotSyncEventId, baseSnapshotTimestamp, baseSnapshotTimestamp)));
    }

    /**
     * Cancel Snapshot Sync due to an error.
     *
     * @param snapshotSyncEventId unique identifier for the snapshot sync task
     * @param error               specific error cause
     */
    private void snapshotSyncCancel(UUID snapshotSyncEventId, LogReplicationError error, boolean timeoutException) {
        // Report error to the application through the dataSender
        dataSenderBufferManager.onError(error);

        log.error("SNAPSHOT SYNC is being CANCELED, due to {}", error.getDescription());

        LogReplicationEventMetadata metadata = new LogReplicationEventMetadata(snapshotSyncEventId);
        metadata.setTimeoutException(timeoutException);
        // Enqueue cancel event, this will cause re-entrance to snapshot sync to start a new cycle
        fsm.input(new LogReplicationEvent(LogReplicationEventType.SYNC_CANCEL, metadata));
    }

    /**
     * Reset due to the start of a new snapshot sync.
     */
    public void reset() {
        // TODO: Do we need to persist the lastTransferDone in the event of failover?
        // Get global tail, this will represent the timestamp for a consistent snapshot/cut of the data
        baseSnapshotTimestamp = runtime.getAddressSpaceView().getLogTail();
        fsm.getAckReader().setBaseSnapshot(baseSnapshotTimestamp);

        // Starting a new snapshot sync, reset the log reader's snapshot timestamp
        snapshotReader.reset(baseSnapshotTimestamp);
        dataSenderBufferManager.reset(Address.NON_ADDRESS);

        stopSnapshotSync.set(false);
        startSnapshotSync = true;
        completed = false;
    }

    private void resetBaseSnapshotTimestamp(long timestamp) {
        baseSnapshotTimestamp = timestamp;
        fsm.setBaseSnapshot(timestamp);
        fsm.getAckReader().setBaseSnapshot(timestamp);
    }

    /**
     * Stop Snapshot Sync
     */
    public void stop() {
        stopSnapshotSync.set(true);
    }

    public void updateTopologyConfigId(long topologyConfigId) {
        dataSenderBufferManager.updateTopologyConfigId(topologyConfigId);
    }
}
