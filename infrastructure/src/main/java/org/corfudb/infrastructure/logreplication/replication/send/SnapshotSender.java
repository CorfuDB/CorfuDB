package org.corfudb.infrastructure.logreplication.replication.send;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.ObservableValue;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.ReadProcessor;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReadMessage;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReader;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntry;
import org.corfudb.protocols.wireprotocol.logreplication.LogReplicationEntryMetadata;
import org.corfudb.protocols.wireprotocol.logreplication.MessageType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.DEFAULT_MAX_NUM_MSG_PER_BATCH;
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.DEFAULT_TIMEOUT_MS;

/**
 *  This class is responsible of transmitting a consistent view of the data at a given timestamp,
 *  i.e, reading and sending a snapshot of the data for the requested streams.
 *
 *  It reads log entries from the data-store through the SnapshotReader, and hands it to the
 *  DataSender (the application specific callback for sending data to the remote cluster).
 *
 *  The SnapshotReader has a default implementation based on reads at the stream layer
 *  (no serialization/deserialization) required.
 *
 *  DataSender is implemented by the application, as communication channels between sites are out of the scope
 *  of CorfuDB.
 */
@Slf4j
public class SnapshotSender {

    private CorfuRuntime runtime;
    private SnapshotReader snapshotReader;
    private SenderBufferManager dataSenderBufferManager;
    private LogReplicationFSM fsm;

    @Getter
    private long baseSnapshotTimestamp;

    // The max number of message can be sent over in burst for a snapshot cycle.
    private final int maxNumSnapshotMsgPerBatch;

    // This flag will indicate the start of a snapshot sync, so start snapshot marker is sent once.
    private boolean startSnapshotSync = true;

    @Getter
    @VisibleForTesting
    // For testing purposes, used to count the number of messages sent in order to interrupt snapshot sync
    private ObservableValue observedCounter = new ObservableValue(0);

    private volatile boolean stopSnapshotSync = false;

    public SnapshotSender(CorfuRuntime runtime, SnapshotReader snapshotReader, DataSender dataSender,
                          ReadProcessor readProcessor, int snapshotSyncBatchSize, LogReplicationFSM fsm) {
        this.runtime = runtime;
        this.snapshotReader = snapshotReader;
        this.fsm = fsm;
        this.maxNumSnapshotMsgPerBatch = snapshotSyncBatchSize <= 0 ? DEFAULT_MAX_NUM_MSG_PER_BATCH : snapshotSyncBatchSize;
        this.dataSenderBufferManager = new SnapshotSenderBufferManager(dataSender, fsm.getAckReader());
    }

    private CompletableFuture<LogReplicationEntry> snapshotSyncAck;

    /**
     * Initiate Snapshot Sync, this entails reading and sending data for a given snapshot.
     *
     * @param snapshotSyncEventId identifier of the event that initiated the snapshot sync
     */
    public void transmit(UUID snapshotSyncEventId) {

        log.info("Running snapshot sync for {} on baseSnapshot {}", snapshotSyncEventId,
                baseSnapshotTimestamp);

        boolean completed = false;  // Flag indicating the snapshot sync is completed
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
            while (messagesSent < maxNumSnapshotMsgPerBatch && !dataSenderBufferManager.getPendingMessages().isFull() && !completed && !stopSnapshotSync) {

                try {
                    snapshotReadMessage = snapshotReader.read(snapshotSyncEventId);
                    completed = snapshotReadMessage.isEndRead();
                    // Data Transformation / Processing
                    // readProcessor.process(snapshotReadMessage.getMessages())
                } catch (TrimmedException te) {
                    log.warn("Cancel snapshot sync due to trimmed exception.", te);
                    dataSenderBufferManager.reset(Address.NON_ADDRESS);
                    snapshotSyncCancel(snapshotSyncEventId, LogReplicationError.TRIM_SNAPSHOT_SYNC);
                    cancel = true;
                    break;
                } catch (Exception e) {
                    log.error("Caught exception during snapshot sync", e);
                    snapshotSyncCancel(snapshotSyncEventId, LogReplicationError.UNKNOWN);
                    cancel = true;
                    break;
                }

                messagesSent += processReads(snapshotReadMessage.getMessages(), snapshotSyncEventId, completed);
                observedCounter.setValue(messagesSent);
            }

            if (completed) {
                // Block until ACK from last sent message is received
                try {
                    LogReplicationEntry ack = snapshotSyncAck.get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    if (ack.getMetadata().getSnapshotTimestamp() == baseSnapshotTimestamp &&
                            ack.getMetadata().getMessageMetadataType().equals(MessageType.SNAPSHOT_TRANSFER_COMPLETE)) {
                        // Snapshot Sync Transfer Completed
                        log.info("Snapshot sync transfer completed for {} on timestamp={}, ack={}", snapshotSyncEventId,
                                baseSnapshotTimestamp, ack.getMetadata());
                        snapshotSyncTransferComplete(snapshotSyncEventId);
                    } else {
                        log.warn("Expected ack for {}, but received for a different snapshot {}", baseSnapshotTimestamp,
                                ack.getMetadata());
                        throw new Exception("Wrong base snapshot ack");
                    }
                } catch (Exception e) {
                    log.error("Exception caught while blocking on snapshot sync {}, ack for {}",
                            snapshotSyncEventId, baseSnapshotTimestamp, e);
                    if (snapshotSyncAck.isCompletedExceptionally()) {
                        log.error("Snapshot Sync completed exceptionally", e);
                    }
                    snapshotSyncCancel(snapshotSyncEventId, LogReplicationError.UNKNOWN);
                } finally {
                    snapshotSyncAck = null;
                }
            } else if (!cancel) {
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
                //todo: generate an event for discovery service
                log.warn("While sending data, caught an exception. Will notify discovery service");
            }
        }
    }

    private int processReads(List<LogReplicationEntry> logReplicationEntries, UUID snapshotSyncEventId, boolean completed) {
        int numMessages = 0;

        // If we are starting a snapshot sync, send a start marker.
        if (startSnapshotSync) {
            dataSenderBufferManager.sendWithBuffering(getSnapshotSyncStartMarker(snapshotSyncEventId));
            startSnapshotSync = false;
            numMessages++;
        }

        dataSenderBufferManager.sendWithBuffering(logReplicationEntries);

        // If Snapshot is complete, add end marker
        if (completed) {
            LogReplicationEntry endDataMessage = getSnapshotSyncEndMarker(snapshotSyncEventId);
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
    private LogReplicationEntry getSnapshotSyncStartMarker(UUID snapshotSyncEventId) {
        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(MessageType.SNAPSHOT_START, fsm.getTopologyConfigId(),
                snapshotSyncEventId, Address.NON_ADDRESS, Address.NON_ADDRESS, baseSnapshotTimestamp, Address.NON_ADDRESS);
        LogReplicationEntry emptyEntry = new LogReplicationEntry(metadata);
        return emptyEntry;
    }

    private LogReplicationEntry getSnapshotSyncEndMarker(UUID snapshotSyncEventId) {
        LogReplicationEntryMetadata metadata = new LogReplicationEntryMetadata(MessageType.SNAPSHOT_END, fsm.getTopologyConfigId(), snapshotSyncEventId,
                Address.NON_ADDRESS, Address.NON_ADDRESS, baseSnapshotTimestamp, Address.NON_ADDRESS);
        LogReplicationEntry emptyEntry = new LogReplicationEntry(metadata);
        return emptyEntry;
    }

    /**
     * Complete Snapshot Sync transfer, insert completion event in the FSM queue.
     *
     * @param snapshotSyncEventId unique identifier for the completed snapshot sync.
     */
    public void snapshotSyncTransferComplete(UUID snapshotSyncEventId) {
        // We need to bind the internal event (COMPLETE) to the snapshotSyncEventId that originated it, this way
        // the state machine can correlate to the corresponding state (in case of delayed events)
        fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_TRANSFER_COMPLETE,
                new LogReplicationEventMetadata(snapshotSyncEventId, baseSnapshotTimestamp, baseSnapshotTimestamp)));
    }

    /**
     * Cancel Snapshot Sync due to an error.
     *
     * @param snapshotSyncEventId unique identifier for the snapshot sync task
     * @param error specific error cause
     */
    private void snapshotSyncCancel(UUID snapshotSyncEventId, LogReplicationError error) {
        // Report error to the application through the dataSender
        dataSenderBufferManager.onError(error);

        log.error("SNAPSHOT SYNC is being CANCELED, due to {}", error.getDescription());

        // Enqueue cancel event, this will cause re-entrance to snapshot sync to start a new cycle
        fsm.input(new LogReplicationEvent(LogReplicationEventType.SYNC_CANCEL,
                new LogReplicationEventMetadata(snapshotSyncEventId)));
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

        stopSnapshotSync = false;
        startSnapshotSync = true;
    }

    /**
     * Stop Snapshot Sync
     */
    public void stop() {
        stopSnapshotSync = true;
    }

    public void updateTopologyConfigId(long topologyConfigId) {
        dataSenderBufferManager.updateTopologyConfigId(topologyConfigId);
    }
}
