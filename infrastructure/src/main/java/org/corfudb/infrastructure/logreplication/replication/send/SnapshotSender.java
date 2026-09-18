package org.corfudb.infrastructure.logreplication.replication.send;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Outcome;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Phase;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.SnapshotSyncLease;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.DEFAULT_MAX_NUM_MSG_PER_BATCH;
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.DEFAULT_TIMEOUT_MS;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.corfudb.protocols.service.CorfuProtocolLogReplication.getLrEntryAckMsg;

/**
 * This class is responsible of transmitting a consistent view of the data at a given timestamp,
 * i.e, reading and sending a snapshot of the data for the requested streams.
 * <p>
 * It reads log entries from the data-store through the SnapshotReader, and hands it to the
 * DataSender (the application specific callback for sending data to the remote cluster).
 * <p>
 * The sink owns the lifecycle of a snapshot sync (see the snapshot lease). This sender polls the
 * sink's status, proposes a START using the sink's admission epoch once admission is open, sends
 * bulk data only after the sink explicitly accepted that START, and cancels locally when the sink
 * reports the attempt gone. It has no give-up timer of its own: a slow sink is never restarted by
 * the source, and only the sink's deadline ends an attempt. RPC timeouts and ordinary
 * retransmission of unacknowledged data remain.
 * <p>
 * DataSender is implemented by the application, as communication channels between sites are out of the scope
 * of CorfuDB.
 */
@Slf4j
public class SnapshotSender {

    private static final long REQUEST_TIMEOUT_NANOS = TimeUnit.MILLISECONDS.toNanos(DEFAULT_TIMEOUT_MS);
    private static final long STATUS_POLL_MS = 2000;
    private static final long STATUS_POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(STATUS_POLL_MS);
    // Shortest wait of a step that can neither send nor consume anything right now.
    private static final long MIN_WAIT_MS = 50;

    private static final ScheduledExecutorService CONTINUATIONS = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("snapshot-admission-poll-%d").build());

    private final CorfuRuntime runtime;
    private final SnapshotReader snapshotReader;
    @Getter
    private final SenderBufferManager dataSenderBufferManager;
    private final LogReplicationFSM fsm;
    private final DataSender dataSender;
    private final LongSupplier nanoTime;

    @Getter
    private volatile long baseSnapshotTimestamp;

    // The max number of message can be sent over in burst for a snapshot cycle.
    private final int maxNumSnapshotMsgPerBatch;

    @Getter
    @VisibleForTesting
    // For testing purposes, used to count the number of messages sent in order to interrupt snapshot sync
    private ObservableValue observedCounter = new ObservableValue(0);

    private final Optional<AtomicLong> messageCounter;

    @Getter
    @VisibleForTesting
    private volatile AtomicBoolean stopSnapshotSync = new AtomicBoolean(false);

    private boolean snapshotCompleted = false;  // Flag indicating the snapshot sync is completed

    // Identity of this attempt on the wire. The id is chosen here; the generation is granted by the
    // sink when it accepts the START. Events of another attempt are ignored by the FSM states.
    @Getter
    private volatile UUID wireAttemptId;
    @Getter
    private volatile long wireAttemptGeneration;

    // Written by the FSM worker that runs the steps, read by stop(), which the FSM consumer calls.
    private volatile boolean admitted;
    private volatile boolean transferFinished;
    private volatile boolean cancelSent;
    // This run gave up and asked the FSM for a new one; nothing of it runs any more.
    private volatile boolean cancelled;
    private boolean unsupportedSinkReported;
    private CompletableFuture<Object> replySignal;
    private CompletableFuture<LogReplicationMetadataResponseMsg> statusFuture;
    private CompletableFuture<LogReplicationEntryMsg> admissionFuture;
    private long statusRequestNanos;
    private long admissionRequestNanos;
    private long lastStatusPollNanos;
    private SnapshotSyncLeaseRecord remoteLease = SnapshotSyncLeaseRecord.getDefaultInstance();
    private LogReplicationEntryMsg admissionRequest;
    private long runGeneration;
    private ScheduledFuture<?> continuation;

    // Retain the public constructor signature for existing callers; snapshot reads do not use a ReadProcessor.
    @SuppressWarnings("PMD.UnusedFormalParameter")
    public SnapshotSender(CorfuRuntime runtime, SnapshotReader snapshotReader, DataSender dataSender,
                          ReadProcessor readProcessor, int snapshotSyncBatchSize, LogReplicationFSM fsm) {
        this(runtime, snapshotReader, dataSender, snapshotSyncBatchSize, fsm, System::nanoTime);
    }

    @VisibleForTesting
    SnapshotSender(CorfuRuntime runtime, SnapshotReader snapshotReader, DataSender dataSender,
                   int snapshotSyncBatchSize, LogReplicationFSM fsm,
                   LongSupplier nanoTime) {
        this.runtime = runtime;
        this.snapshotReader = snapshotReader;
        this.fsm = fsm;
        this.dataSender = dataSender;
        this.nanoTime = nanoTime;
        this.maxNumSnapshotMsgPerBatch = snapshotSyncBatchSize <= 0 ? DEFAULT_MAX_NUM_MSG_PER_BATCH : snapshotSyncBatchSize;
        this.dataSenderBufferManager = new SnapshotSenderBufferManager(dataSender, fsm.getAckReader());
        this.messageCounter = MeterRegistryProvider.getInstance().map(registry ->
                registry.gauge("logreplication.messages",
                        ImmutableList.of(Tag.of("replication.type", "snapshot")),
                        new AtomicLong(0)));
    }

    @VisibleForTesting
    void pollStatusNow() {
        lastStatusPollNanos = 0;
    }

    /**
     * Run one step of the snapshot sync and yield the FSM worker. Every wait (for the sink's status,
     * for admission, for acknowledgements) is a scheduled continuation rather than a blocked thread,
     * since several state machines share the same worker pool.
     *
     * @param snapshotSyncEventId identifier of the event that initiated the snapshot sync
     */
    public void transmit(UUID snapshotSyncEventId, boolean forcedSnapshotSync) {
        if (stopSnapshotSync.get() || transferFinished || cancelled) {
            return;
        }
        try {
            if (wireAttemptId == null) {
                reset(); // Never started: the state that drives this sender always resets it first.
            }
            step(snapshotSyncEventId, forcedSnapshotSync);
        } catch (RuntimeException e) {
            log.warn("Snapshot transport operation failed; reconcile without renewing the sink lease", e);
            scheduleContinuation(snapshotSyncEventId, STATUS_POLL_MS);
        } catch (Error e) {
            // Without a continuation nothing would ever run this snapshot sync again, and the FSM
            // would sit in snapshot sync silently: the worker only records the failure in a Future.
            log.error("Snapshot sync step failed; it is retried", e);
            scheduleContinuation(snapshotSyncEventId, STATUS_POLL_MS);
            throw e;
        }
    }

    private void step(UUID eventId, boolean forced) {
        pollStatus();
        if (remoteLease.getSchemaVersion() != SnapshotSyncLease.VERSION) {
            // No usable status yet: the reply is outstanding, was lost, or the sink has not
            // initialized its lease (it is not the leader yet).
            scheduleContinuation(eventId, STATUS_POLL_MS);
            return;
        }
        boolean sameTopology = remoteLease.getTopologyConfigId() == fsm.getTopologyConfigId();
        // Identity and topology are separate questions: an attempt of this sender that was admitted
        // under a topology this source has since moved on from is still this sender's to cancel.
        boolean mine = remoteLease.hasAttemptId() && remoteLease.getAttemptId().equals(getUuidMsg(wireAttemptId))
                && remoteLease.getSourceSnapshot() == baseSnapshotTimestamp
                && (!admitted || remoteLease.getGeneration() == wireAttemptGeneration);
        boolean ours = mine && sameTopology;
        if ((ours && remoteLease.getOutcome() == Outcome.COMPLETED)
                || (remoteLease.getPhase() == Phase.APPLYING && (ours || (sameTopology && !forced && !admitted)))) {
            // The transfer is already durable on the sink (our END reply was lost, or a previous
            // leader of this source completed it): follow that apply instead of sending it again.
            wireAttemptId = CorfuProtocolCommon.getUUID(remoteLease.getAttemptId());
            wireAttemptGeneration = remoteLease.getGeneration();
            baseSnapshotTimestamp = remoteLease.getSourceSnapshot();
            fsm.getAckReader().setBaseSnapshot(baseSnapshotTimestamp);
            transferFinished = true;
            snapshotSyncTransferComplete(eventId, forced);
            return;
        }
        if ((mine && (remoteLease.getOutcome() == Outcome.ABORTED || !sameTopology))
                || (admitted && remoteLease.getGeneration() > wireAttemptGeneration)) {
            // The sink abandoned this attempt (deadline, failure, ownership or topology change), or
            // the topology changed under it: every message would now carry a topology the attempt
            // was not admitted for, and the sink would reject it forever.
            snapshotSyncCancel(eventId, LogReplicationError.UNKNOWN, forced);
            return;
        }
        if (!admitted && !negotiateAdmission(eventId, ours)) {
            scheduleContinuation(eventId, STATUS_POLL_MS);
            return;
        }
        transfer(eventId, forced);
    }

    /**
     * DataSender implementations need not complete lost RPCs themselves. Expire the local wait, not
     * the snapshot attempt: a late reply cannot authorize this run, and a lost START reply must be
     * reconciled/retried using the identical proposal.
     */
    private void pollStatus() {
        long now = nanoTime.getAsLong();
        if (statusFuture != null && !statusFuture.isDone() && now - statusRequestNanos >= REQUEST_TIMEOUT_NANOS) {
            statusFuture = null;
            lastStatusPollNanos = 0;
        }
        if (admissionFuture != null && !admissionFuture.isDone() && now - admissionRequestNanos >= REQUEST_TIMEOUT_NANOS) {
            admissionFuture = null;
        }
        if (statusFuture == null && (lastStatusPollNanos == 0 || now - lastStatusPollNanos >= STATUS_POLL_NANOS)) {
            lastStatusPollNanos = now;
            statusRequestNanos = now;
            statusFuture = observeReply(dataSender.sendMetadataRequest(), statusRequestNanos);
        }
        if (statusFuture == null || !statusFuture.isDone()) {
            return;
        }
        try {
            LogReplicationMetadataResponseMsg response = statusFuture.join();
            if (response.hasSnapshotLease()) {
                unsupportedSinkReported = false;
                snapshotReader.setSnapshotBatchSizeHint(response.getSnapshotTransferWriteSize());
                remoteLease = response.getSnapshotLease();
            } else {
                // Negotiation refuses such a sink, so this is only reachable when replication is
                // driven without it. Keep polling: nothing is ever sent without admission.
                if (!unsupportedSinkReported) {
                    log.error("The sink does not report a snapshot lease; it predates the snapshot lease "
                            + "protocol and cannot admit a snapshot sync from this source");
                    unsupportedSinkReported = true;
                }
                remoteLease = SnapshotSyncLeaseRecord.getDefaultInstance();
            }
        } catch (CompletionException e) {
            log.debug("Snapshot status unavailable; retry after transport recovery", e);
        } finally {
            statusFuture = null;
        }
    }

    /** @return true once the sink has explicitly accepted this attempt's START. */
    private boolean negotiateAdmission(UUID eventId, boolean ours) {
        if (consumeAdmissionReply(eventId)) {
            return true;
        }
        if (remoteLease.getPhase() == Phase.READY && (admissionRequest == null
                || admissionRequest.getMetadata().getAdmissionEpoch() != remoteLease.getAdmissionEpoch()
                || admissionRequest.getMetadata().getTopologyConfigID() != fsm.getTopologyConfigId())) {
            // Choose a usable source cut after sink recovery, not at the beginning of its cooldown.
            baseSnapshotTimestamp = runtime.getAddressSpaceView().getLogTail();
            snapshotReader.reset(baseSnapshotTimestamp);
            fsm.getAckReader().setBaseSnapshot(baseSnapshotTimestamp);
            LogReplicationEntryMsg start = getSnapshotSyncStartMarker(wireAttemptId);
            admissionRequest = start.toBuilder().setMetadata(start.getMetadata().toBuilder()
                    .setSnapshotLifecycleVersion(SnapshotSyncLease.VERSION)
                    .setAdmissionEpoch(remoteLease.getAdmissionEpoch()).setExternalRequestId(getUuidMsg(eventId))).build();
        }
        if (admissionFuture == null && admissionRequest != null && (remoteLease.getPhase() == Phase.READY || ours)) {
            admissionRequestNanos = nanoTime.getAsLong();
            admissionFuture = observeReply(dataSender.send(admissionRequest), admissionRequestNanos);
            // A transport that answers synchronously already has the reply.
            return consumeAdmissionReply(eventId);
        }
        return false;
    }

    private boolean consumeAdmissionReply(UUID eventId) {
        if (admitted) {
            return true;
        }
        if (admissionFuture == null || !admissionFuture.isDone()) {
            return false;
        }
        try {
            LogReplicationEntryMsg accepted = admissionFuture.join();
            if (accepted.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_START_ACCEPTED
                    && accepted.getMetadata().getSyncRequestId().equals(getUuidMsg(wireAttemptId))
                    && accepted.getMetadata().getSnapshotTimestamp() == baseSnapshotTimestamp
                    && accepted.getMetadata().getTopologyConfigID() == fsm.getTopologyConfigId()
                    && accepted.getMetadata().getAttemptGeneration() > 0) {
                wireAttemptGeneration = accepted.getMetadata().getAttemptGeneration();
                ((SnapshotSenderBufferManager) dataSenderBufferManager).beginLease(wireAttemptId, wireAttemptGeneration);
                admitted = true;
                log.info("Snapshot sync {} admitted by the sink as generation {} on baseSnapshot {}",
                        eventId, wireAttemptGeneration, baseSnapshotTimestamp);
            }
        } catch (CompletionException | java.util.concurrent.CancellationException e) {
            log.debug("START not yet accepted; reconcile and retry the same proposal", e);
        } finally {
            admissionFuture = null;
        }
        return admitted;
    }

    private void transfer(UUID eventId, boolean forced) {
        try {
            // Take every acknowledgement that has arrived, then resend what waited too long. Neither
            // blocks: a step never holds the worker while it waits for the sink.
            LogReplicationEntryMsg ack = dataSenderBufferManager.pollAcks();
            dataSenderBufferManager.resendTimedOut();
            if (ack != null && ack.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE
                    && ack.getMetadata().getSyncRequestId().equals(getUuidMsg(wireAttemptId))
                    && ack.getMetadata().getAttemptGeneration() == wireAttemptGeneration) {
                transferFinished = true;
                snapshotSyncTransferComplete(eventId, forced);
                return;
            }
            int sent = 0;
            while (!snapshotCompleted && !stopSnapshotSync.get() && sent < maxNumSnapshotMsgPerBatch
                    && !dataSenderBufferManager.getPendingMessages().isFull()) {
                if (!Address.isAddress(baseSnapshotTimestamp)) {
                    log.info("Snapshot sync {} has no data in the log; sending the end marker only", eventId);
                    snapshotCompleted = true;
                    dataSenderBufferManager.sendWithBuffering(getSnapshotSyncEndMarker(wireAttemptId));
                    break;
                }
                SnapshotReadMessage batch = snapshotReader.read(wireAttemptId);
                snapshotCompleted = batch.isEndRead();
                sent += processReads(batch.getMessages(), wireAttemptId, snapshotCompleted);
                final long sentSoFar = sent;
                messageCounter.ifPresent(counter -> counter.addAndGet(sentSoFar));
                observedCounter.setValue(sent);
            }
            if ((!snapshotCompleted && !dataSenderBufferManager.getPendingMessages().isFull())
                    || dataSenderBufferManager.hasArrivedReplies()) {
                // More to send, or replies that arrived while this step was sending (on a fast link,
                // or an in-process transport, every one of them has): the next step takes them.
                scheduleContinuation(eventId, 0);
            } else {
                // Nothing more can go out until a reply frees the window, or, with everything sent,
                // until the end marker is acknowledged. The next step runs as soon as the first
                // outstanding reply arrives; the timer only covers status polls and resends.
                scheduleContinuation(eventId, Math.max(MIN_WAIT_MS,
                        Math.min(STATUS_POLL_MS, dataSenderBufferManager.getResendTimerMs())));
                continueOnReply(eventId);
            }
        } catch (TrimmedException e) {
            log.warn("Source snapshot cut became unusable", e);
            snapshotSyncCancel(eventId, LogReplicationError.TRIM_SNAPSHOT_SYNC, forced);
        } catch (Exception e) {
            log.warn("Source snapshot cut became unusable", e);
            snapshotSyncCancel(eventId, LogReplicationError.UNKNOWN, forced);
        }
    }

    private <T> CompletableFuture<T> observeReply(CompletableFuture<T> reply, long sentAtNanos) {
        // Check arrival in the completion callback, not when the FSM next gets a worker.
        // A prompt reply remains valid if another workflow delays the next continuation.
        return reply.thenApply(value -> {
            if (nanoTime.getAsLong() - sentAtNanos >= REQUEST_TIMEOUT_NANOS) {
                throw new CompletionException(new TimeoutException("Snapshot RPC reply arrived after its deadline"));
            }
            return value;
        });
    }

    /**
     * Transfer speed must follow the sink's acknowledgements, not a timer: a window of a few
     * messages refilled once per timer period could never move a large snapshot inside its budget.
     */
    private synchronized void continueOnReply(UUID eventId) {
        if (replySignal != null && !replySignal.isDone()) {
            return; // Still waiting on requests that are older than anything sent since.
        }
        List<CompletableFuture<LogReplicationEntryMsg>> outstanding = dataSenderBufferManager.outstandingRequests();
        if (outstanding.isEmpty()) {
            return;
        }
        long captured = runGeneration;
        replySignal = CompletableFuture.anyOf(outstanding.toArray(new CompletableFuture<?>[0]));
        replySignal.whenComplete((reply, failure) -> continueNow(eventId, captured));
    }

    private synchronized void continueNow(UUID eventId, long captured) {
        if (captured != runGeneration || stopSnapshotSync.get()) {
            return;
        }
        if (continuation != null) { continuation.cancel(false); continuation = null; }
        scheduleContinuation(eventId, 0);
    }

    private synchronized void scheduleContinuation(UUID eventId, long delayMs) {
        if (stopSnapshotSync.get() || (continuation != null && !continuation.isDone())) { return; }
        long captured = runGeneration;
        continuation = CONTINUATIONS.schedule(() -> {
            synchronized (SnapshotSender.this) {
                if (captured != runGeneration || stopSnapshotSync.get()) { return; }
                continuation = null;
            }
            fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_SYNC_CONTINUE,
                    new LogReplicationEventMetadata(eventId)));
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private int processReads(List<LogReplicationEntryMsg> logReplicationEntries,
                             UUID snapshotSyncEventId,
                             boolean completed) {
        int numMessages = 0;

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
            dataSenderBufferManager.sendWithBuffering(endDataMessage);
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
        if (stopSnapshotSync.get()) { return; }
        synchronized (this) {
            runGeneration++;
            if (continuation != null) { continuation.cancel(false); continuation = null; }
        }
        // We need to bind the internal event (COMPLETE) to the snapshotSyncEventId that originated it, this way
        // the state machine can correlate to the corresponding state (in case of delayed events)
        fsm.input(new LogReplicationEvent(LogReplicationEventType.SNAPSHOT_TRANSFER_COMPLETE,
                new LogReplicationEventMetadata(snapshotSyncEventId, baseSnapshotTimestamp, baseSnapshotTimestamp, forcedSnapshotSync)
                        .setSnapshotAttempt(wireAttemptId, wireAttemptGeneration)));
    }

    /**
     * Cancel Snapshot Sync due to an error.
     *
     * @param snapshotSyncEventId unique identifier for the snapshot sync task
     * @param error               specific error cause
     */
    private void snapshotSyncCancel(UUID snapshotSyncEventId, LogReplicationError error, boolean forcedSnapshotSync) {
        if (stopSnapshotSync.get()) { return; }
        synchronized (this) {
            runGeneration++;
            if (continuation != null) { continuation.cancel(false); continuation = null; }
        }
        cancelled = true;
        cancelAdmittedAttempt();
        // Nothing of this attempt will be resent: do not keep its window of messages in memory
        // until the next attempt replaces it.
        dataSenderBufferManager.reset(Address.NON_ADDRESS);
        // Report error to the application through the dataSender
        dataSenderBufferManager.onError(error);

        log.error("SNAPSHOT SYNC is being CANCELED for {}, due to {}", snapshotSyncEventId, error.getDescription());

        // Enqueue cancel event, this will cause re-entrance to snapshot sync to start a new cycle
        fsm.input(new LogReplicationEvent(LogReplicationEventType.SYNC_CANCEL,
                new LogReplicationEventMetadata(snapshotSyncEventId, forcedSnapshotSync)
                        .setSnapshotAttempt(wireAttemptId, wireAttemptGeneration)));
    }

    /**
     * Tells the sink that an admitted transfer will not be finished, so that it releases its
     * protection right away instead of noticing the silence minutes later. Best effort: if this is
     * lost, the sink's own inactivity and deadline checks still end the attempt. An apply that is
     * already running on the sink is never cancelled: it does not need the source any more.
     */
    private void cancelAdmittedAttempt() {
        UUID attemptId = wireAttemptId;
        if (!admitted || transferFinished || cancelSent || attemptId == null) {
            return;
        }
        cancelSent = true;
        LogReplicationEntryMsg end = getSnapshotSyncEndMarker(attemptId);
        LogReplicationEntryMsg cancel = end.toBuilder().setMetadata(end.getMetadata().toBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_CANCEL)
                .setSnapshotLifecycleVersion(SnapshotSyncLease.VERSION).setAttemptGeneration(wireAttemptGeneration)).build();
        try {
            dataSender.send(cancel).exceptionally(failure -> null);
        } catch (RuntimeException e) {
            log.debug("Cancellation delivery failed; the sink deadline still applies", e);
        }
    }

    /**
     * Reset due to the start of a new snapshot sync.
     */
    public void reset() {
        synchronized (this) {
            runGeneration++;
            if (continuation != null) { continuation.cancel(false); continuation = null; }
            replySignal = null;
        }
        // The identity below is about to be replaced, after which the attempt could never be
        // cancelled or resumed by anyone.
        cancelAdmittedAttempt();
        wireAttemptId = UUID.randomUUID();
        wireAttemptGeneration = 0;
        admitted = false;
        transferFinished = false;
        cancelSent = false;
        cancelled = false;
        statusFuture = null;
        admissionFuture = null;
        admissionRequest = null;
        lastStatusPollNanos = 0;
        remoteLease = SnapshotSyncLeaseRecord.getDefaultInstance();
        // TODO: Do we need to persist the lastTransferDone in the event of failover?
        // Get global tail, this will represent the timestamp for a consistent snapshot/cut of the data.
        // It is chosen again when the sink opens admission, so that it is not older than the wait.
        baseSnapshotTimestamp = runtime.getAddressSpaceView().getLogTail();
        fsm.getAckReader().setBaseSnapshot(baseSnapshotTimestamp);

        // Starting a new snapshot sync, reset the log reader's snapshot timestamp
        snapshotReader.reset(baseSnapshotTimestamp);
        dataSenderBufferManager.reset(Address.NON_ADDRESS);

        stopSnapshotSync.set(false);
        snapshotCompleted = false;
    }

    /**
     * Stop Snapshot Sync
     */
    public void stop() {
        stopSnapshotSync.set(true);
        synchronized (this) {
            runGeneration++;
            if (continuation != null) { continuation.cancel(false); continuation = null; }
        }
        // A stop (connection flap, forced sync, leadership loss, shutdown) abandons the transfer
        // from the source's side; the identity is discarded by the next reset().
        cancelAdmittedAttempt();
    }

    public void updateTopologyConfigId(long topologyConfigId) {
        dataSenderBufferManager.updateTopologyConfigId(topologyConfigId);
    }
}
