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
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.ReadProcessor;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReadMessage;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReader;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.DEFAULT_MAX_NUM_MSG_PER_BATCH;
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.DEFAULT_TIMEOUT_MS;
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.SNAPSHOT_SYNC_ACK_MAX_RETRIES;
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
    private final DataSender dataSender;

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

    private final Optional<AtomicLong> messageCounter;

    @Getter
    @VisibleForTesting
    private volatile AtomicBoolean stopSnapshotSync = new AtomicBoolean(false);

    private boolean snapshotCompleted = false;  // Flag indicating the snapshot sync is completed

    // Consecutive genuine (non-busy) mid-transfer stall checks, mirroring the genuineTimeouts counter
    // in waitForSnapshotSyncAck -- reset as soon as either progress resumes or the sink reports busy.
    @VisibleForTesting
    int consecutiveGenuineStallChecks = 0;

    // Wall-clock time of the last mid-transfer stall check, so a tight SNAPSHOT_SYNC_CONTINUE
    // self-loop (pending buffer permanently full) can't hammer the sink with metadata-poll requests
    // more often than the final-ack path already does.
    @VisibleForTesting
    long lastStallCheckTimeMs = 0;

    public SnapshotSender(CorfuRuntime runtime, SnapshotReader snapshotReader, DataSender dataSender,
                          ReadProcessor readProcessor, int snapshotSyncBatchSize, LogReplicationFSM fsm) {
        this.runtime = runtime;
        this.snapshotReader = snapshotReader;
        this.fsm = fsm;
        this.dataSender = dataSender;
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
    public void transmit(UUID snapshotSyncEventId, boolean forcedSnapshotSync) {
        if (snapshotCompleted) {
            // Since FSM is a perpetually running machine, InSnapshotSync.onEntry() is called even when an incoming
            // event is ignored for any reason.
            // This translates to transmit() being called even if the data has been successfully transferred. So we check
            // the flag and return immediately to avoid sending any un-required data
            log.info("The snapshot sync data for {} has already been sent to remote.", snapshotSyncEventId);
            return;
        }

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
                    !snapshotCompleted && !stopSnapshotSync.get()) {

                try {
                    snapshotReadMessage = snapshotReader.read(snapshotSyncEventId);
                    snapshotCompleted = snapshotReadMessage.isEndRead();
                    // Data Transformation / Processing
                    // readProcessor.process(snapshotReadMessage.getMessages())
                } catch (TrimmedException te) {
                    log.warn("Cancel snapshot sync due to trimmed exception.", te);
                    dataSenderBufferManager.reset(Address.NON_ADDRESS);
                    snapshotSyncCancel(snapshotSyncEventId, LogReplicationError.TRIM_SNAPSHOT_SYNC, forcedSnapshotSync);
                    cancel = true;
                    break;
                } catch (Exception e) {
                    log.error("Caught exception during snapshot sync", e);
                    snapshotSyncCancel(snapshotSyncEventId, LogReplicationError.UNKNOWN, forcedSnapshotSync);
                    cancel = true;
                    break;
                }

                messagesSent += processReads(snapshotReadMessage.getMessages(), snapshotSyncEventId, snapshotCompleted);
                final long messagesSentSnapshot = messagesSent;
                messageCounter.ifPresent(counter -> counter.addAndGet(messagesSentSnapshot));
                observedCounter.setValue(messagesSent);
            }

            if (snapshotCompleted) {
                // Block until ACK from last sent message is received
                try {
                    LogReplicationEntryMsg ack = waitForSnapshotSyncAck(snapshotSyncEventId, snapshotSyncAck);
                    if (ack.getMetadata().getSnapshotTimestamp() == baseSnapshotTimestamp &&
                            ack.getMetadata().getEntryType().equals(LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE)) {
                        // Snapshot Sync Transfer Completed
                        log.info("Snapshot sync transfer completed for {} on timestamp={}, ack={}", snapshotSyncEventId,
                                baseSnapshotTimestamp, TextFormat.shortDebugString(ack.getMetadata()));
                        snapshotSyncTransferComplete(snapshotSyncEventId, forcedSnapshotSync);
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
                    snapshotSyncCancel(snapshotSyncEventId, LogReplicationError.UNKNOWN, forcedSnapshotSync);
                } finally {
                    snapshotSyncAck = null;
                }
            } else if (!cancel && !stopSnapshotSync.get()) {
                if (checkForGenuineMidTransferStall(snapshotSyncEventId, forcedSnapshotSync)) {
                    // Cancellation already triggered for this attempt; do not also continue it.
                    return;
                }

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
                waitForSnapshotSyncAck(snapshotSyncEventId, snapshotSyncAck);
                snapshotSyncTransferComplete(snapshotSyncEventId, forcedSnapshotSync);
            } catch (Exception e) {
                log.warn("Caught exception while sending data to sink.", e);
                snapshotSyncCancel(snapshotSyncEventId, LogReplicationError.UNKNOWN, forcedSnapshotSync);
            }
        }
    }

    /**
     * Wait for the ack of the final snapshot sync message, tolerating a busy-but-alive sink instead
     * of giving up on the first timeout. On each timeout with no ack, poll the sink directly: if it
     * reports it's still actively processing this snapshot sync, keep waiting without counting it
     * against the retry budget; a sink that stays genuinely silent (no ack, and either no response
     * or a not-busy response to the poll) for SNAPSHOT_SYNC_ACK_MAX_RETRIES consecutive attempts is
     * treated as stalled and the caller cancels the sync.
     *
     * @param snapshotSyncEventId identifier of the snapshot sync being waited on, for logging only
     * @param ackFuture completes with the ack for the last message sent for this attempt
     * @return the received ack
     * @throws Exception if the ack does not arrive and the sink is not reporting activity for
     *                    SNAPSHOT_SYNC_ACK_MAX_RETRIES consecutive attempts, or some other failure occurs
     */
    LogReplicationEntryMsg waitForSnapshotSyncAck(UUID snapshotSyncEventId,
                                                  CompletableFuture<LogReplicationEntryMsg> ackFuture) throws Exception {
        int genuineTimeouts = 0;
        while (true) {
            if (stopSnapshotSync.get()) {
                // A stop was requested (e.g. a new cancellation/request) while waiting on this
                // attempt's ack. Without this check, a sink that always reports busy=true would keep
                // this loop (and, transitively, cancelSnapshotSync()'s blocking wait on
                // transmitFuture) alive indefinitely, since genuineTimeouts only advances on a
                // non-busy timeout.
                throw new InterruptedException("Snapshot sync " + snapshotSyncEventId
                        + " stopped while waiting for ack.");
            }
            try {
                return ackFuture.get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (TimeoutException te) {
                if (isSinkStillProcessing()) {
                    log.info("Sink reported busy while waiting for ack for snapshot sync {}; extending wait.",
                            snapshotSyncEventId);
                    continue;
                }
                genuineTimeouts++;
                log.warn("Ack timeout {}/{} waiting for snapshot sync {} to be acked; sink not reporting activity.",
                        genuineTimeouts, SNAPSHOT_SYNC_ACK_MAX_RETRIES, snapshotSyncEventId);
                if (genuineTimeouts >= SNAPSHOT_SYNC_ACK_MAX_RETRIES) {
                    throw te;
                }
            }
        }
    }

    /**
     * Actively poll the sink for whether it is still working on the current snapshot sync. Used only
     * to extend patience on an otherwise-unexplained ack timeout, so any failure to determine this
     * (including the poll itself timing out) is conservatively treated as "not processing".
     *
     * isProcessing alone is sink-wide, not attempt-scoped: it's true whenever the sink is busy on
     * *any* snapshot-sync work, including a stale attempt this source has already canceled and
     * moved past (e.g. the sink is still resuming an old, abandoned apply after its own restart --
     * see LogReplicationSinkManager.resumeSnapshotApply()). Trusting it blindly in that case would
     * let this source wait indefinitely on someone else's progress for an attempt it isn't even
     * asking about, compounding the (separately tracked, deliberately unrecoverable-without-an-
     * operator) risk of a genuinely hung apply: instead of that hang bounding just the one stuck
     * attempt, it would also stall every subsequent attempt's transfer phase forever, since
     * genuineTimeouts/consecutiveGenuineStallChecks never advance while isProcessing reports true.
     * A sink new enough to report processingSnapshotTimestamp lets this be resolved: only honor the
     * busy signal when it names *this* attempt's baseSnapshotTimestamp. An old sink (field absent)
     * is trusted at face value, matching the pre-existing behavior -- not a regression, just not
     * yet able to make the distinction.
     */
    private boolean isSinkStillProcessing() {
        try {
            LogReplicationMetadataResponseMsg response = dataSender.sendMetadataRequest()
                    .get(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (!response.getIsProcessing()) {
                return false;
            }
            if (response.hasProcessingSnapshotTimestamp()
                    && response.getProcessingSnapshotTimestamp() != baseSnapshotTimestamp) {
                log.warn("Sink reports busy, but on a different attempt (its baseSnapshot={}, ours={}); " +
                                "treating as not processing our attempt.",
                        response.getProcessingSnapshotTimestamp(), baseSnapshotTimestamp);
                return false;
            }
            return true;
        } catch (Exception e) {
            log.warn("Failed to query sink status while waiting for snapshot sync ack.", e);
            return false;
        }
    }

    /**
     * If the pending buffer has been genuinely stalled (no ack has advanced maxAckTimestamp) for at
     * least as long as the final-ack path's own genuine-silence bound, and the sink isn't reporting
     * activity on a paced busy-signal poll, treat this as a stalled mid-transfer attempt and cancel
     * it -- the same busy-aware give-up waitForSnapshotSyncAck applies to the final ack, just
     * triggered from the buffer-full self-loop instead of the tail-end blocking wait. Without this,
     * a sink that stops acking before the source finishes reading all data never reaches
     * waitForSnapshotSyncAck at all (snapshotCompleted never becomes true), so the final-ack fix
     * never engages and SenderBufferManager.resend() -- which has no retry cap of its own -- would
     * retry forever with no path to cancellation.
     *
     * Paces its own busy-signal polls at DEFAULT_TIMEOUT_MS so a tight SNAPSHOT_SYNC_CONTINUE
     * self-loop (buffer permanently full) can't hammer the sink with metadata requests every
     * iteration.
     *
     * @return true if a cancellation was triggered this cycle (caller must not also fire
     *         SNAPSHOT_SYNC_CONTINUE)
     */
    @VisibleForTesting
    boolean checkForGenuineMidTransferStall(UUID snapshotSyncEventId, boolean forcedSnapshotSync) {
        if (dataSenderBufferManager.getMillisSinceLastAckAdvance()
                < SNAPSHOT_SYNC_ACK_MAX_RETRIES * DEFAULT_TIMEOUT_MS) {
            consecutiveGenuineStallChecks = 0;
            return false;
        }

        long now = System.currentTimeMillis();
        if (now - lastStallCheckTimeMs < DEFAULT_TIMEOUT_MS) {
            return false; // paced -- don't poll more often than the final-ack path does
        }
        lastStallCheckTimeMs = now;

        if (isSinkStillProcessing()) {
            log.info("Sink reported busy while its oldest pending snapshot entry is unacked for {}; extending patience.",
                    snapshotSyncEventId);
            consecutiveGenuineStallChecks = 0;
            return false;
        }

        consecutiveGenuineStallChecks++;
        log.warn("Mid-transfer stall {}/{} for snapshot sync {}: no ack has advanced and sink not reporting activity.",
                consecutiveGenuineStallChecks, SNAPSHOT_SYNC_ACK_MAX_RETRIES, snapshotSyncEventId);
        if (consecutiveGenuineStallChecks < SNAPSHOT_SYNC_ACK_MAX_RETRIES) {
            return false;
        }

        log.error("Canceling snapshot sync {}: mid-transfer stall persisted for {} genuine checks.",
                snapshotSyncEventId, consecutiveGenuineStallChecks);
        consecutiveGenuineStallChecks = 0;
        snapshotSyncCancel(snapshotSyncEventId, LogReplicationError.UNKNOWN, forcedSnapshotSync);
        return true;
    }

    private int processReads(List<LogReplicationEntryMsg> logReplicationEntries,
                             UUID snapshotSyncEventId,
                             boolean completed) {
        int numMessages = 0;

        // If we are starting a snapshot sync, send a start marker.
        if (startSnapshotSync) {
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

    /**
     * Complete Snapshot Sync transfer, insert completion event in the FSM queue.
     *
     * @param snapshotSyncEventId unique identifier for the completed snapshot sync.
     */
    private void snapshotSyncTransferComplete(UUID snapshotSyncEventId, boolean forcedSnapshotSync) {
        // We need to bind the internal event (COMPLETE) to the snapshotSyncEventId that originated it, this way
        // the state machine can correlate to the corresponding state (in case of delayed events)
        fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_TRANSFER_COMPLETE,
                new LogReplicationEventMetadata(snapshotSyncEventId, baseSnapshotTimestamp, baseSnapshotTimestamp, forcedSnapshotSync)));
    }

    /**
     * Cancel Snapshot Sync due to an error.
     *
     * @param snapshotSyncEventId unique identifier for the snapshot sync task
     * @param error               specific error cause
     */
    private void snapshotSyncCancel(UUID snapshotSyncEventId, LogReplicationError error, boolean forcedSnapshotSync) {
        // Report error to the application through the dataSender
        dataSenderBufferManager.onError(error);

        log.error("SNAPSHOT SYNC is being CANCELED for {}, due to {}", snapshotSyncEventId, error.getDescription());

        // Enqueue cancel event, this will cause re-entrance to snapshot sync to start a new cycle
        fsm.input(new LogReplicationEvent(LogReplicationEventType.SYNC_CANCEL,
                new LogReplicationEventMetadata(snapshotSyncEventId, forcedSnapshotSync)));
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
        snapshotCompleted = false;
        consecutiveGenuineStallChecks = 0;
        lastStallCheckTimeMs = 0;
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
