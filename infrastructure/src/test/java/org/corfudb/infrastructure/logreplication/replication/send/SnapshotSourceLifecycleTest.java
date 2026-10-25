package org.corfudb.infrastructure.logreplication.replication.send;

import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationAckReader;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReadMessage;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.*;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.SnapshotSyncLease;
import org.corfudb.runtime.exceptions.LogReplicationBusyException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.AddressSpaceView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.DEFAULT_TIMEOUT_MS;

/**
 * The source side of the snapshot lease: it polls the sink's status, proposes START with the sink's
 * admission epoch, sends bulk data only after explicit acceptance, and has no give-up timer of its
 * own. There is no fallback protocol for a sink that does not report a lease.
 */
class SnapshotSourceLifecycleTest {
    private CorfuRuntime runtime;
    private DataSender transport;
    private SnapshotSender source;
    private SnapshotReader reader;
    private LogReplicationFSM fsm;
    private final UUID eventId = UUID.randomUUID();
    private final AtomicReference<SnapshotSyncLeaseRecord> status = new AtomicReference<>(SnapshotSyncLease.seedIdle("sink", -1, 0));
    private final List<LogReplicationEntryMsg> sent = new ArrayList<>();
    private CompletableFuture<LogReplicationEntryMsg> admission;
    private final AtomicLong nanoTime = new AtomicLong(1);
    // Stubbed once, in setup. The sender's continuations call this mock from another thread, and a
    // mock must not be stubbed again while it is being called.
    private final AtomicLong topology = new AtomicLong(0);

    @BeforeEach
    void setup() {
        runtime = mock(CorfuRuntime.class);
        AddressSpaceView addressSpace = mock(AddressSpaceView.class);
        when(runtime.getAddressSpaceView()).thenReturn(addressSpace);
        when(addressSpace.getLogTail()).thenReturn(50L);
        fsm = mock(LogReplicationFSM.class);
        when(fsm.getAckReader()).thenReturn(mock(LogReplicationAckReader.class));
        when(fsm.getTopologyConfigId()).thenAnswer(invocation -> topology.get());
        reader = mock(SnapshotReader.class, CALLS_REAL_METHODS);
        transport = mock(DataSender.class);
        when(transport.sendMetadataRequest()).thenAnswer(invocation -> CompletableFuture.completedFuture(
                LogReplicationMetadataResponseMsg.newBuilder().setSnapshotLease(status.get()).build()));
        admission = new CompletableFuture<>();
        when(transport.send(any(LogReplicationEntryMsg.class))).thenAnswer(invocation -> {
            LogReplicationEntryMsg message = invocation.getArgument(0);
            sent.add(message);
            if (message.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_START) { return admission; }
            return CompletableFuture.completedFuture(message.toBuilder().setMetadata(message.getMetadata().toBuilder()
                    .setEntryType(message.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_END
                            ? LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE : LogReplicationEntryType.SNAPSHOT_REPLICATED)).build());
        });
        source = new SnapshotSender(runtime, reader, transport, 1, fsm, nanoTime::get);
        source.reset();
        when(reader.read(any())).thenReturn(new SnapshotReadMessage(Collections.singletonList(
                LogReplicationEntryMsg.newBuilder().setMetadata(LogReplicationEntryMetadataMsg.newBuilder()
                        .setEntryType(LogReplicationEntryType.SNAPSHOT_MESSAGE)).build()), true));
    }

    @AfterEach
    void cleanup() { source.stop(); }

    private void drive() {
        source.pollStatusNow();
        source.transmit(eventId, false);
    }

    /**
     * Counts down when the sender asks the FSM for its next step from now on. A verification with a
     * timeout cannot be used for this: LogReplicationFSM.input is synchronized, and Mockito holds
     * the mock's monitor for as long as such a verification polls, so the thread that delivers the
     * event could never get in while the test waits for it.
     */
    private CountDownLatch nextContinuation() {
        CountDownLatch requested = new CountDownLatch(1);
        doAnswer(invocation -> {
            LogReplicationEvent event = invocation.getArgument(0);
            if (event.getType() == LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_CONTINUE) {
                requested.countDown();
            }
            return null;
        }).when(fsm).input(any());
        return requested;
    }

    private void accept() {
        drive();
        LogReplicationEntryMsg start = sent.get(0);
        SnapshotSyncLeaseRecord reserved = SnapshotSyncLease.reserve(status.get(), start.getMetadata(), 1000, 10000, 20, "protection");
        status.set(SnapshotSyncLease.prepared(reserved));
        admission.complete(start.toBuilder().setMetadata(start.getMetadata().toBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_START_ACCEPTED).setAttemptGeneration(reserved.getGeneration())).build());
        drive();
    }

    @Test
    void sinkWriteBudgetReachesReaderBeforeStartAndBeforeAnyDataRead() {
        when(transport.sendMetadataRequest()).thenReturn(CompletableFuture.completedFuture(
                LogReplicationMetadataResponseMsg.newBuilder().setSnapshotLease(status.get())
                        .setSnapshotTransferWriteSize(10000).build()));
        drive();
        var order = inOrder(reader, transport);
        order.verify(reader).setSnapshotBatchSizeHint(10000);
        order.verify(transport).send(any(LogReplicationEntryMsg.class));
        verify(reader, never()).read(any());
        verify(runtime, never()).getParameters();
    }

    @Test
    void aNewAdmissionEpochSelectsAFreshSourceCutBeforeRetryingStart() {
        drive();
        LogReplicationEntryMsg original = sent.get(0);
        admission.completeExceptionally(new LogReplicationBusyException(LogReplicationBusyResponseMsg.getDefaultInstance()));
        admission = new CompletableFuture<>();
        status.set(status.get().toBuilder().setAdmissionEpoch(status.get().getAdmissionEpoch() + 1).build());
        when(runtime.getAddressSpaceView().getLogTail()).thenReturn(75L);

        drive();

        assertEquals(2, sent.size());
        LogReplicationEntryMsg refreshed = sent.get(1);
        assertEquals(LogReplicationEntryType.SNAPSHOT_START, refreshed.getMetadata().getEntryType());
        assertEquals(original.getMetadata().getSyncRequestId(), refreshed.getMetadata().getSyncRequestId());
        assertEquals(status.get().getAdmissionEpoch(), refreshed.getMetadata().getAdmissionEpoch());
        assertEquals(75, refreshed.getMetadata().getSnapshotTimestamp());
        verify(reader).reset(75);
        verify(fsm.getAckReader()).setBaseSnapshot(75);
        verify(reader, never()).read(any());
    }

    @Test
    void noStartDuringRecoveryAndNoBulkDataBeforeExplicitAcceptance() {
        status.set(status.get().toBuilder().setPhase(SnapshotSyncLeaseRecord.Phase.RECOVERING).build());
        drive();
        assertTrue(sent.isEmpty());
        verify(reader, never()).read(any());
        status.set(SnapshotSyncLease.seedIdle("sink", -1, 0));
        drive();
        drive();
        assertEquals(1, sent.size());
        assertEquals(LogReplicationEntryType.SNAPSHOT_START, sent.get(0).getMetadata().getEntryType());
        verify(reader, never()).read(any());
    }

    @Test
    void acceptedAttemptUsesSeparateInternalIdentityAndStartsDataAtZero() {
        accept();
        assertNotEquals(eventId, source.getWireAttemptId());
        assertEquals(org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg(eventId), sent.get(0).getMetadata().getExternalRequestId());
        assertEquals(3, sent.size());
        assertEquals(0, sent.get(1).getMetadata().getSnapshotSyncSeqNum());
        assertEquals(1, sent.get(2).getMetadata().getSnapshotSyncSeqNum());
        assertEquals(status.get().getAttemptId(), sent.get(1).getMetadata().getSyncRequestId());
        assertEquals(status.get().getGeneration(), sent.get(1).getMetadata().getAttemptGeneration());
    }

    @Test
    void lostStartReplyRetriesIdenticalProposalWithoutNewSnapshotOrBulkData() {
        drive();
        LogReplicationEntryMsg original = sent.get(0);
        status.set(SnapshotSyncLease.reserve(status.get(), original.getMetadata(), 1000, 10000, 20, "protection"));
        admission.completeExceptionally(new TimeoutException("lost START reply"));
        admission = new CompletableFuture<>();
        drive();
        assertEquals(2, sent.size());
        assertEquals(original, sent.get(1));
        verify(reader, never()).read(any());
    }

    @Test
    void unfinishedMetadataRequestExpiresAndALateLeaselessReplyCannotBypassRecovery() {
        CompletableFuture<LogReplicationMetadataResponseMsg> lost = new CompletableFuture<>();
        status.set(status.get().toBuilder().setPhase(SnapshotSyncLeaseRecord.Phase.RECOVERING).build());
        when(transport.sendMetadataRequest()).thenReturn(lost).thenAnswer(call -> CompletableFuture.completedFuture(
                LogReplicationMetadataResponseMsg.newBuilder().setSnapshotLease(status.get()).build()));
        drive();
        nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(DEFAULT_TIMEOUT_MS) - 1);
        drive();
        verify(transport, times(1)).sendMetadataRequest();
        assertTrue(sent.isEmpty());

        nanoTime.incrementAndGet();
        drive();
        verify(transport, times(2)).sendMetadataRequest();
        lost.complete(LogReplicationMetadataResponseMsg.getDefaultInstance());
        drive();
        assertTrue(sent.isEmpty());
        verify(reader, never()).read(any());

        status.set(SnapshotSyncLease.seedIdle("sink", -1, 0));
        drive();
        assertEquals(1, sent.size());
        assertEquals(LogReplicationEntryType.SNAPSHOT_START, sent.get(0).getMetadata().getEntryType());
    }

    @Test
    void timelyRepliesRemainUsableWhenAnotherWorkflowDelaysTheSourceWorker() {
        CompletableFuture<LogReplicationMetadataResponseMsg> metadataReply = new CompletableFuture<>();
        when(transport.sendMetadataRequest()).thenReturn(metadataReply).thenAnswer(call -> CompletableFuture.completedFuture(
                LogReplicationMetadataResponseMsg.newBuilder().setSnapshotLease(status.get()).build()));
        drive();
        nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(DEFAULT_TIMEOUT_MS) - 1);
        metadataReply.complete(LogReplicationMetadataResponseMsg.newBuilder().setSnapshotLease(status.get()).build());
        nanoTime.addAndGet(2);
        drive();
        verify(transport, times(1)).sendMetadataRequest();
        assertEquals(1, sent.size());

        LogReplicationEntryMsg proposal = sent.get(0);
        SnapshotSyncLeaseRecord reserved = SnapshotSyncLease.reserve(status.get(), proposal.getMetadata(),
                1000, 10000, 20, "protection");
        status.set(SnapshotSyncLease.prepared(reserved));
        admission.complete(proposal.toBuilder().setMetadata(proposal.getMetadata().toBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_START_ACCEPTED)
                .setAttemptGeneration(reserved.getGeneration())).build());
        nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(DEFAULT_TIMEOUT_MS) + 1);
        drive();
        verify(reader).read(any());
        assertEquals(reserved.getGeneration(), source.getWireAttemptGeneration());
        assertEquals(1, sent.stream().filter(message -> message.getMetadata().getEntryType()
                == LogReplicationEntryType.SNAPSHOT_START).count());
    }

    @Test
    void choosingTheSourceCutDoesNotConsumeTheStartReplyBudget() {
        when(runtime.getAddressSpaceView().getLogTail()).thenAnswer(call -> {
            nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(DEFAULT_TIMEOUT_MS) + 1);
            return 50L;
        });
        drive();
        LogReplicationEntryMsg proposal = sent.get(0);
        SnapshotSyncLeaseRecord reserved = SnapshotSyncLease.reserve(status.get(), proposal.getMetadata(),
                1000, 10000, 20, "protection");
        status.set(SnapshotSyncLease.prepared(reserved));
        admission.complete(proposal.toBuilder().setMetadata(proposal.getMetadata().toBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_START_ACCEPTED)
                .setAttemptGeneration(reserved.getGeneration())).build());
        drive();
        verify(reader).read(any());
        assertEquals(reserved.getGeneration(), source.getWireAttemptGeneration());
    }

    @Test
    void aMetadataReplyArrivingAfterItsDeadlineIsIgnored() {
        CompletableFuture<LogReplicationMetadataResponseMsg> lateReply = new CompletableFuture<>();
        status.set(status.get().toBuilder().setPhase(SnapshotSyncLeaseRecord.Phase.RECOVERING).build());
        when(transport.sendMetadataRequest()).thenReturn(lateReply).thenAnswer(call -> CompletableFuture.completedFuture(
                LogReplicationMetadataResponseMsg.newBuilder().setSnapshotLease(status.get()).build()));
        drive();
        nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(DEFAULT_TIMEOUT_MS));
        lateReply.complete(LogReplicationMetadataResponseMsg.newBuilder().setSnapshotTransferWriteSize(1).build());
        drive();
        assertTrue(sent.isEmpty());
        verify(reader, never()).setSnapshotBatchSizeHint(1);
        drive();
        assertTrue(sent.isEmpty());
        verify(reader, never()).read(any());
    }

    @Test
    void aSinkThatReportsNoLeaseIsNeverSentAnything() {
        // A sink that predates the snapshot lease. Negotiation already refuses it; if replication is
        // driven without negotiation, the sender must still never fall back to an unadmitted transfer.
        when(transport.sendMetadataRequest()).thenAnswer(call -> CompletableFuture.completedFuture(
                LogReplicationMetadataResponseMsg.newBuilder().setSnapshotStart(40).setSnapshotTransferred(40)
                        .setSnapshotApplied(40).setLastLogEntryTimestamp(40).build()));
        for (int poll = 0; poll < 4; poll++) {
            drive();
        }
        assertTrue(sent.isEmpty());
        verify(reader, never()).read(any());
        verify(fsm, never()).input(any());

        // Once the sink is upgraded and reports a lease, the same attempt proceeds.
        when(transport.sendMetadataRequest()).thenAnswer(call -> CompletableFuture.completedFuture(
                LogReplicationMetadataResponseMsg.newBuilder().setSnapshotLease(status.get()).build()));
        drive();
        assertEquals(LogReplicationEntryType.SNAPSHOT_START, sent.get(0).getMetadata().getEntryType());
    }

    @Test
    void aSynchronousAcceptanceIsUsedInTheSameStep() {
        // An in-process or very fast transport answers START before send() returns.
        when(transport.send(any(LogReplicationEntryMsg.class))).thenAnswer(invocation -> {
            LogReplicationEntryMsg message = invocation.getArgument(0);
            sent.add(message);
            LogReplicationEntryType type = message.getMetadata().getEntryType();
            if (type == LogReplicationEntryType.SNAPSHOT_START) {
                SnapshotSyncLeaseRecord reserved = SnapshotSyncLease.reserve(status.get(), message.getMetadata(),
                        1000, 10000, 20, "protection");
                status.set(SnapshotSyncLease.prepared(reserved));
                return CompletableFuture.completedFuture(message.toBuilder().setMetadata(message.getMetadata().toBuilder()
                        .setEntryType(LogReplicationEntryType.SNAPSHOT_START_ACCEPTED)
                        .setAttemptGeneration(reserved.getGeneration())).build());
            }
            return CompletableFuture.completedFuture(message.toBuilder().setMetadata(message.getMetadata().toBuilder()
                    .setEntryType(type == LogReplicationEntryType.SNAPSHOT_END
                            ? LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE : LogReplicationEntryType.SNAPSHOT_REPLICATED)).build());
        });
        drive();
        assertEquals(3, sent.size(), "START, one data message and END went out in a single step");
        assertEquals(1, source.getWireAttemptGeneration());
        assertEquals(2, source.getObservedCounter().getValue(), "data and END of this step are observable");
    }

    @Test
    void aStoppedOrFinishedSenderDoesNothing() {
        accept();
        drive(); // the data message's acknowledgement
        drive(); // the END acknowledgement completes the transfer
        verify(fsm, times(1)).input(argThat(event -> event.getType() == LogReplicationEvent.LogReplicationEventType.SNAPSHOT_TRANSFER_COMPLETE));
        int count = sent.size();
        clearInvocations(transport);
        drive();
        assertEquals(count, sent.size());
        verify(transport, never()).sendMetadataRequest();

        source.reset();
        source.stop();
        drive();
        verify(transport, never()).sendMetadataRequest();
    }

    @Test
    void neverCompletedStartReplyIsRetriedWithTheSameProposal() {
        drive();
        LogReplicationEntryMsg proposal = sent.get(0);
        status.set(SnapshotSyncLease.reserve(status.get(), proposal.getMetadata(), 1000, 10000, 20, "protection"));
        CompletableFuture<LogReplicationEntryMsg> lostReply = admission;
        admission = new CompletableFuture<>();
        nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(DEFAULT_TIMEOUT_MS));
        drive();
        assertEquals(List.of(proposal, proposal), sent);
        assertFalse(lostReply.isDone());
        verify(reader, never()).read(any());
    }

    @Test
    void unfinishedStartReplyExpiresWithoutRenewingReservationOrTrustingLateAck() {
        drive();
        LogReplicationEntryMsg proposal = sent.get(0);
        SnapshotSyncLeaseRecord reserved = SnapshotSyncLease.reserve(status.get(), proposal.getMetadata(),
                1000, 10000, 20, "protection");
        status.set(SnapshotSyncLease.prepared(reserved));
        CompletableFuture<LogReplicationEntryMsg> lost = admission;
        admission = new CompletableFuture<>();
        nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(DEFAULT_TIMEOUT_MS) - 1);
        drive();
        assertEquals(1, sent.size());
        nanoTime.incrementAndGet();
        lost.complete(proposal.toBuilder().setMetadata(proposal.getMetadata().toBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_START_ACCEPTED).setAttemptGeneration(99)).build());
        drive();
        assertEquals(List.of(proposal, proposal), sent);
        assertEquals(reserved.getDeadlineMs(), status.get().getDeadlineMs());

        drive();
        verify(reader, never()).read(any());
        assertEquals(0, source.getWireAttemptGeneration());
        admission.complete(proposal.toBuilder().setMetadata(proposal.getMetadata().toBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_START_ACCEPTED)
                .setAttemptGeneration(reserved.getGeneration())).build());
        drive();
        verify(reader).read(any());
        assertEquals(reserved.getGeneration(), source.getWireAttemptGeneration());
    }

    @Test
    void resetDiscardsOutstandingAdmissionAndOldAcceptance() {
        drive();
        LogReplicationEntryMsg oldProposal = sent.get(0);
        CompletableFuture<LogReplicationEntryMsg> oldReply = admission;
        source.reset();
        admission = new CompletableFuture<>();
        drive();
        oldReply.complete(oldProposal.toBuilder().setMetadata(oldProposal.getMetadata().toBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_START_ACCEPTED).setAttemptGeneration(1)).build());
        drive();
        // The replaced proposal is cancelled, in case the sink reserved it; then a new one is made.
        assertEquals(LogReplicationEntryType.SNAPSHOT_CANCEL, sent.get(1).getMetadata().getEntryType());
        assertEquals(oldProposal.getMetadata().getSyncRequestId(), sent.get(1).getMetadata().getSyncRequestId());
        assertEquals(LogReplicationEntryType.SNAPSHOT_START, sent.get(2).getMetadata().getEntryType());
        assertNotEquals(oldProposal.getMetadata().getSyncRequestId(), sent.get(2).getMetadata().getSyncRequestId());
        verify(reader, never()).read(any());
        assertEquals(0, source.getWireAttemptGeneration());
    }

    @Test
    void unfinishedAdmissionDoesNotPreventObservingSinkAbandonment() {
        drive();
        SnapshotSyncLeaseRecord reserved = SnapshotSyncLease.reserve(status.get(), sent.get(0).getMetadata(),
                1000, 10000, 20, "protection");
        status.set(SnapshotSyncLease.abandon(reserved, 11000, "deadline"));
        drive();
        // Reconciliation cancels locally and neither retries START nor sends data. The wire CANCEL
        // carries no generation, since none was ever accepted; the sink matches it by attempt id.
        assertEquals(2, sent.size());
        assertEquals(LogReplicationEntryType.SNAPSHOT_CANCEL, sent.get(1).getMetadata().getEntryType());
        assertEquals(0, sent.get(1).getMetadata().getAttemptGeneration());
        verify(reader, never()).read(any());
        verify(fsm).input(argThat(event -> event.getType() == LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL));
    }

    @Test
    void lostEndReplyCanBeReconciledFromDurableApplyStatus() {
        accept();
        status.set(SnapshotSyncLease.transferred(status.get()));
        drive();
        verify(fsm).input(argThat(event -> event.getType() == LogReplicationEvent.LogReplicationEventType.SNAPSHOT_TRANSFER_COMPLETE
                && event.getMetadata().getSyncId().equals(eventId)));
    }

    @Test
    void abandonedAttemptCancelsAndNeverSendsAnotherBatch() {
        accept();
        int count = sent.size();
        status.set(SnapshotSyncLease.abandon(status.get(), 2000, "sink deadline"));
        drive();
        assertEquals(count + 1, sent.size());
        assertEquals(LogReplicationEntryType.SNAPSHOT_CANCEL, sent.get(sent.size() - 1).getMetadata().getEntryType());
        verify(fsm).input(argThat(event -> event.getType() == LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL));
    }

    @Test
    void sourceTrimRequestsAbandonmentInsteadOfRenewingTheAttempt() {
        when(reader.read(any())).thenThrow(new TrimmedException());
        accept();
        assertEquals(LogReplicationEntryType.SNAPSHOT_CANCEL, sent.get(sent.size() - 1).getMetadata().getEntryType());
        verify(fsm).input(argThat(event -> event.getType() == LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL));
        verify(transport).onError(LogReplicationError.TRIM_SNAPSHOT_SYNC);
    }

    @Test
    void sourceReadFailureCancelsWithUnknownErrorInsteadOfRenewingTheAttempt() {
        when(reader.read(any())).thenThrow(new IllegalStateException("source read failed"));
        accept();
        assertEquals(LogReplicationEntryType.SNAPSHOT_CANCEL, sent.get(sent.size() - 1).getMetadata().getEntryType());
        verify(fsm).input(argThat(event -> event.getType() == LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL));
        verify(transport).onError(LogReplicationError.UNKNOWN);
    }

    @Test
    void busyFutureDoesNotAcknowledgeOrPoisonPendingData() throws Exception {
        accept();
        SenderBufferManager buffer = source.getDataSenderBufferManager();
        buffer.getPendingCompletableFutureForAcks().clear();
        buffer.getPendingCompletableFutureForAcks().put(0L, CompletableFuture.failedFuture(
                new LogReplicationBusyException(LogReplicationBusyResponseMsg.getDefaultInstance())));
        int pending = buffer.getPendingMessages().getSize();
        assertNull(buffer.processAcks());
        assertEquals(pending, buffer.getPendingMessages().getSize());
        assertTrue(buffer.getPendingCompletableFutureForAcks().isEmpty());
        buffer.getPendingMessages().getPendingEntries().forEach(entry -> entry.setTime(entry.getTime() - 10000));
        buffer.resend();
        assertFalse(buffer.getPendingCompletableFutureForAcks().isEmpty());
        assertNotNull(buffer.processAcks());
    }

    @Test
    void staleAckCannotEvictCurrentAttempt() {
        accept();
        SnapshotSenderBufferManager buffer = (SnapshotSenderBufferManager) source.getDataSenderBufferManager();
        LogReplicationEntryMsg valid = sent.get(2).toBuilder().setMetadata(sent.get(2).getMetadata().toBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE)).build();
        int size = buffer.getPendingMessages().getSize();
        buffer.updateAck(valid.toBuilder().setMetadata(valid.getMetadata().toBuilder().setAttemptGeneration(99)).build());
        assertEquals(size, buffer.getPendingMessages().getSize());
        buffer.updateAck(valid);
        assertEquals(0, buffer.getPendingMessages().getSize());
    }

    @Test
    void unavailableStatusYieldsAndRetriesWithoutStartingOrReading() {
        when(transport.sendMetadataRequest()).thenReturn(CompletableFuture.failedFuture(new TimeoutException("status timeout")))
                .thenThrow(new IllegalStateException("disconnected"))
                .thenReturn(CompletableFuture.completedFuture(LogReplicationMetadataResponseMsg.newBuilder()
                        .setSnapshotLease(status.get()).build()));
        drive();
        drive();
        assertTrue(sent.isEmpty());
        verify(reader, never()).read(any());
        drive();
        assertEquals(LogReplicationEntryType.SNAPSHOT_START, sent.get(0).getMetadata().getEntryType());
    }

    @Test
    void wrongStartAcceptanceCannotAuthorizeBulkData() {
        drive();
        LogReplicationEntryMsg start = sent.get(0);
        admission.complete(start.toBuilder().setMetadata(start.getMetadata().toBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_START_ACCEPTED).setAttemptGeneration(1)
                .setSyncRequestId(org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg(UUID.randomUUID()))).build());
        admission = new CompletableFuture<>();
        drive();
        verify(reader, never()).read(any());
        assertTrue(sent.stream().allMatch(entry -> entry.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_START));
    }

    @Test
    void lostCancelTransportDoesNotPreventLocalCancellation() {
        accept();
        doThrow(new IllegalStateException("disconnected during cancellation")).when(transport).send(
                argThat((LogReplicationEntryMsg message) -> message.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_CANCEL));
        status.set(SnapshotSyncLease.abandon(status.get(), 2000, "deadline"));
        drive();
        verify(fsm).input(argThat(event -> event.getType() == LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL));
    }

    @Test
    void transferAckCompletesOnceWithoutWaitingForStatusToReachApply() {
        accept();
        drive();
        drive();
        verify(fsm, times(1)).input(argThat(event -> event.getType() == LogReplicationEvent.LogReplicationEventType.SNAPSHOT_TRANSFER_COMPLETE));
    }

    @Test
    void sourceReconnectFollowsExistingApplyWithoutResendingSnapshot() {
        UUID attemptId = UUID.randomUUID();
        status.set(status.get().toBuilder().setPhase(SnapshotSyncLeaseRecord.Phase.APPLYING)
                .setAttemptId(org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg(attemptId))
                .setGeneration(10).setSourceSnapshot(40).build());
        drive();
        assertTrue(sent.isEmpty());
        assertEquals(attemptId, source.getWireAttemptId());
        assertEquals(10, source.getWireAttemptGeneration());
        verify(fsm).input(argThat(event -> event.getType() == LogReplicationEvent.LogReplicationEventType.SNAPSHOT_TRANSFER_COMPLETE));
    }

    @Test
    void emptySourceStillRequiresAdmissionAndSendsAnEndMarker() {
        when(runtime.getAddressSpaceView().getLogTail()).thenReturn(-1L);
        accept();
        verify(reader, never()).read(any());
        assertEquals(2, sent.size());
        assertEquals(LogReplicationEntryType.SNAPSHOT_END, sent.get(1).getMetadata().getEntryType());
        assertEquals(0, sent.get(1).getMetadata().getSnapshotSyncSeqNum());
    }

    // ---------------------------------------------------------------- topology changes

    /**
     * The topology id can be bumped without a role change, so the snapshot FSM keeps running. Every
     * message would then carry a topology the attempt was not admitted for, the sink would answer
     * each of them STALE_ATTEMPT, and a BUSY reply alone never cancels anything.
     */
    @Test
    void aTopologyChangeUnderAnAdmittedAttemptCancelsItInsteadOfResendingForever() {
        accept();
        int count = sent.size();
        topology.set(7);

        drive();

        assertEquals(count + 1, sent.size());
        assertEquals(LogReplicationEntryType.SNAPSHOT_CANCEL, sent.get(count).getMetadata().getEntryType());
        verify(fsm).input(argThat(event -> event.getType() == LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL));
        drive();
        assertEquals(count + 1, sent.size(), "nothing of a run that gave up is sent any more");
    }

    @Test
    void aProposalBuiltUnderAnOlderTopologyIsRebuilt() {
        drive();
        assertEquals(0, sent.get(0).getMetadata().getTopologyConfigID());
        admission.completeExceptionally(new LogReplicationBusyException(LogReplicationBusyResponseMsg.getDefaultInstance()));
        admission = new CompletableFuture<>();
        // The lease is READY with the same admission epoch: only the topology moved on.
        topology.set(7);

        drive();

        assertEquals(2, sent.size());
        assertEquals(LogReplicationEntryType.SNAPSHOT_START, sent.get(1).getMetadata().getEntryType());
        assertEquals(7, sent.get(1).getMetadata().getTopologyConfigID());
        assertEquals(sent.get(0).getMetadata().getSyncRequestId(), sent.get(1).getMetadata().getSyncRequestId());
    }

    // ---------------------------------------------------------------- handshake pacing

    /**
     * Status, START, the retry of a START the sink was still preparing, acceptance: on a poll period
     * each, the smallest snapshot sync would spend several periods before its first byte.
     */
    @Test
    void theStatusReplyWakesTheSenderUp() throws Exception {
        CompletableFuture<LogReplicationMetadataResponseMsg> reply = new CompletableFuture<>();
        when(transport.sendMetadataRequest()).thenReturn(reply);
        drive();
        assertTrue(sent.isEmpty());
        CountDownLatch continued = nextContinuation();

        reply.complete(LogReplicationMetadataResponseMsg.newBuilder().setSnapshotLease(status.get()).build());

        assertTrue(continued.await(1, TimeUnit.SECONDS), "the status reply did not wake the sender up");
        source.transmit(eventId, false);
        assertEquals(LogReplicationEntryType.SNAPSHOT_START, sent.get(0).getMetadata().getEntryType());
    }

    @Test
    void theAcceptanceWakesTheSenderUp() throws Exception {
        drive();
        LogReplicationEntryMsg start = sent.get(0);
        SnapshotSyncLeaseRecord reserved = SnapshotSyncLease.reserve(status.get(), start.getMetadata(), 1000, 10000, 20, "protection");
        status.set(SnapshotSyncLease.prepared(reserved));
        CountDownLatch continued = nextContinuation();

        admission.complete(start.toBuilder().setMetadata(start.getMetadata().toBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_START_ACCEPTED).setAttemptGeneration(reserved.getGeneration())).build());

        assertTrue(continued.await(1, TimeUnit.SECONDS), "the acceptance did not wake the sender up");
    }

    /**
     * The sink reserves the attempt on the first START and answers BUSY while it prepares, with the
     * time after which the same proposal will do. The reply wakes the sender up at once, so that
     * time is what paces the retry: not sooner, which would be a busy loop, and not a poll period.
     */
    @Test
    void aStartTheSinkIsStillPreparingIsRetriedWhenTheSinkSaysSo() throws Exception {
        drive();
        LogReplicationEntryMsg proposal = sent.get(0);
        status.set(SnapshotSyncLease.reserve(status.get(), proposal.getMetadata(), 1000, 10000, 20, "protection"));
        CompletableFuture<LogReplicationEntryMsg> refused = admission;
        admission = new CompletableFuture<>();
        CountDownLatch continued = nextContinuation();

        refused.completeExceptionally(new LogReplicationBusyException(LogReplicationBusyResponseMsg.newBuilder()
                .setReason(LogReplicationBusyResponseMsg.Reason.ADMISSION_CLOSED).setRetryAfterMs(200).build()));

        assertTrue(continued.await(1, TimeUnit.SECONDS), "the refusal did not wake the sender up");
        drive();
        assertEquals(1, sent.size(), "not before the sink said");
        nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(200) - 1);
        drive();
        assertEquals(1, sent.size(), "not before the sink said");

        nanoTime.addAndGet(1);
        drive();
        assertEquals(2, sent.size());
        assertEquals(proposal, sent.get(1), "the identical proposal");
        verify(reader, never()).read(any());
    }

    /**
     * The wait is handed to a timer in milliseconds. Cut off instead of rounded up, it would end just
     * before the START may go out: that step would find nothing to do, see less than a millisecond
     * left, take that for nothing pending, and come back a whole poll period later.
     */
    @Test
    void theWaitForARefusedStartNeverEndsBeforeTheStartMayGoOut() {
        drive();
        admission.completeExceptionally(new LogReplicationBusyException(LogReplicationBusyResponseMsg.newBuilder()
                .setReason(LogReplicationBusyResponseMsg.Reason.ADMISSION_CLOSED).setRetryAfterMs(200).build()));
        admission = new CompletableFuture<>();
        drive(); // Takes the refusal.
        assertEquals(200, source.admissionWaitMs());

        nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(100) + 1);
        assertEquals(100, source.admissionWaitMs(), "99.999999 ms are left: not 99");

        nanoTime.addAndGet(TimeUnit.MILLISECONDS.toNanos(100) - 300_001);
        assertEquals(50, source.admissionWaitMs(), "0.3 ms are left: the shortest wait, not a poll period");

        nanoTime.addAndGet(300_000);
        assertEquals(2000, source.admissionWaitMs(), "nothing is pending any more");
        drive();
        assertEquals(2, sent.size());
    }

    @Test
    void aRefusalWithoutAHintIsRetriedOnThePollPeriod() {
        drive();
        admission.completeExceptionally(new LogReplicationBusyException(LogReplicationBusyResponseMsg.getDefaultInstance()));
        admission = new CompletableFuture<>();
        drive(); // Takes the refusal.
        assertEquals(1, sent.size());
        nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(2) - 1);
        drive();
        assertEquals(1, sent.size());
        nanoTime.addAndGet(1);
        drive();
        assertEquals(2, sent.size());
    }

    /** A transport that fails at once must not be retried at once: that is left to the poll period. */
    @Test
    void aStartThatFailsInTheTransportDoesNotWakeTheSenderUp() throws Exception {
        drive();
        CountDownLatch continued = nextContinuation();
        admission.completeExceptionally(new TimeoutException("transport"));
        assertFalse(continued.await(500, TimeUnit.MILLISECONDS));
    }

    // ---------------------------------------------------------------- transfer pacing

    /**
     * The window holds a handful of messages. Refilling it once per timer period would move about
     * one message every two seconds, and no large snapshot could ever finish inside its budget.
     */
    @Test
    void aFullWindowIsRefilledAsSoonAsAReplyArrivesNotOnATimer() throws Exception {
        List<CompletableFuture<LogReplicationEntryMsg>> replies = new ArrayList<>();
        when(reader.read(any())).thenReturn(new SnapshotReadMessage(Collections.singletonList(
                LogReplicationEntryMsg.newBuilder().setMetadata(LogReplicationEntryMetadataMsg.newBuilder()
                        .setEntryType(LogReplicationEntryType.SNAPSHOT_MESSAGE)).build()), false));
        when(transport.send(any(LogReplicationEntryMsg.class))).thenAnswer(invocation -> {
            LogReplicationEntryMsg message = invocation.getArgument(0);
            sent.add(message);
            if (message.getMetadata().getEntryType() == LogReplicationEntryType.SNAPSHOT_START) { return admission; }
            CompletableFuture<LogReplicationEntryMsg> reply = new CompletableFuture<>();
            replies.add(reply);
            return reply;
        });
        accept();
        while (!source.getDataSenderBufferManager().getPendingMessages().isFull()) {
            drive();
        }
        int whenFull = sent.size();
        drive();
        assertEquals(whenFull, sent.size(), "a full window sends nothing");
        TimeUnit.MILLISECONDS.sleep(200); // Let continuations of the earlier steps fire.
        CountDownLatch continued = nextContinuation();

        LogReplicationEntryMsg first = sent.get(1);
        replies.get(0).complete(first.toBuilder().setMetadata(first.getMetadata().toBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_REPLICATED)).build());

        // Well inside the status poll period, which is the only timer a waiting step has.
        assertTrue(continued.await(1, TimeUnit.SECONDS), "the reply did not wake the sender up");
        drive();
        assertEquals(whenFull + 1, sent.size(), "the freed slot is used by the next step");
    }

    @Test
    void everyReplyThatHasArrivedIsConsumedInOneStep() {
        accept(); // The data message and the end marker went out, and both replies are already there.
        drive();
        verify(fsm, times(1)).input(argThat(event -> event.getType() == LogReplicationEvent.LogReplicationEventType.SNAPSHOT_TRANSFER_COMPLETE));
        assertEquals(0, source.getDataSenderBufferManager().getPendingMessages().getSize());
    }

    // ---------------------------------------------------------------- giving an admitted transfer up

    /**
     * A stop (connection flap, forced sync, leadership loss) discards the attempt's identity with
     * the next reset, so nobody could cancel it later: the sink would stay protected until it
     * notices the silence, minutes later.
     */
    @Test
    void stoppingAnAdmittedTransferTellsTheSink() {
        accept();
        int count = sent.size();
        source.stop();
        assertEquals(count + 1, sent.size());
        LogReplicationEntryMetadataMsg cancel = sent.get(count).getMetadata();
        assertEquals(LogReplicationEntryType.SNAPSHOT_CANCEL, cancel.getEntryType());
        assertEquals(status.get().getAttemptId(), cancel.getSyncRequestId());
        assertEquals(status.get().getGeneration(), cancel.getAttemptGeneration());
        source.stop();
        assertEquals(count + 1, sent.size(), "once");
    }

    @Test
    void replacingAnAdmittedTransferTellsTheSinkBeforeItsIdentityIsDiscarded() {
        accept();
        int count = sent.size();
        UUID replaced = source.getWireAttemptId();
        source.reset();
        assertEquals(LogReplicationEntryType.SNAPSHOT_CANCEL, sent.get(count).getMetadata().getEntryType());
        assertEquals(org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg(replaced), sent.get(count).getMetadata().getSyncRequestId());
        assertNotEquals(replaced, source.getWireAttemptId());
    }

    /**
     * A cancellation usually goes out on the very connection whose loss is the reason for it. A sink
     * that never gets it keeps the attempt, closed to the next one and frozen, until its inactivity
     * limit: so the next run repeats it for as long as the sink's status still shows that attempt.
     */
    @Test
    void aCancellationTheSinkNeverGotIsRepeatedByTheNextRun() {
        accept();
        source.stop();
        admission = new CompletableFuture<>(); // The next proposal is not answered by the old acceptance.
        int count = sent.size();
        LogReplicationEntryMsg cancel = sent.get(count - 1);
        assertEquals(LogReplicationEntryType.SNAPSHOT_CANCEL, cancel.getMetadata().getEntryType());

        source.reset(); // The next run. The sink still shows the attempt of the previous one.
        drive();
        assertEquals(count, sent.size(), "not more often than the status is polled");
        nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(2));
        drive();
        assertEquals(count + 1, sent.size());
        assertEquals(cancel, sent.get(count), "the same cancellation, and no proposal while the sink is held");

        // The sink let go of it: nothing is owed any more, and the new run proposes.
        status.set(SnapshotSyncLease.seedIdle("sink", -1, 0));
        nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(2));
        drive();
        assertEquals(count + 2, sent.size());
        assertEquals(LogReplicationEntryType.SNAPSHOT_START, sent.get(count + 1).getMetadata().getEntryType());
        assertNotEquals(cancel.getMetadata().getSyncRequestId(), sent.get(count + 1).getMetadata().getSyncRequestId());
        nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(2));
        drive();
        assertEquals(count + 2, sent.size());
    }

    /** The end marker had made it after all: the sink applies, and that is followed, never cancelled. */
    @Test
    void aTransferThatTurnsOutToBeCompleteIsFollowedNotCancelledAgain() {
        accept();
        source.stop();
        status.set(SnapshotSyncLease.transferred(status.get()));
        source.reset();
        int count = sent.size();
        nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(2));
        drive();
        assertEquals(count, sent.size());
        verify(fsm).input(argThat(event -> event.getType() == LogReplicationEvent.LogReplicationEventType.SNAPSHOT_TRANSFER_COMPLETE));
    }

    @Test
    void anApplyThatRunsOnTheSinkIsNeverCancelledByAStop() {
        accept();
        drive(); // The end marker is acknowledged: the transfer is durable and the sink applies it alone.
        int count = sent.size();
        source.stop();
        assertEquals(count, sent.size());
    }

    /**
     * The sink reserves an attempt, and protects itself for it, when it takes the START: before this
     * sender can know. A sender that gives up in between must still say so, or the sink would hold
     * that reservation until its inactivity limit, closed to every other attempt.
     */
    @Test
    void aProposalTheSinkMayHaveReservedIsCancelledWithoutKnowingItsGeneration() {
        drive();
        LogReplicationEntryMetadataMsg start = sent.get(0).getMetadata();
        source.stop();
        assertEquals(2, sent.size());
        LogReplicationEntryMetadataMsg cancel = sent.get(1).getMetadata();
        assertEquals(LogReplicationEntryType.SNAPSHOT_CANCEL, cancel.getEntryType());
        assertEquals(start.getSyncRequestId(), cancel.getSyncRequestId());
        assertEquals(start.getSnapshotTimestamp(), cancel.getSnapshotTimestamp());
        assertEquals(0, cancel.getAttemptGeneration(), "the generation was never learnt");
        source.stop();
        assertEquals(2, sent.size(), "once");
    }

    @Test
    void replacingAProposalTheSinkMayHaveReservedCancelsItFirst() {
        drive();
        UUID replaced = source.getWireAttemptId();
        source.reset();
        assertEquals(LogReplicationEntryType.SNAPSHOT_CANCEL, sent.get(1).getMetadata().getEntryType());
        assertEquals(org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg(replaced), sent.get(1).getMetadata().getSyncRequestId());
    }

    @Test
    void aSenderThatNeverProposedHasNothingToCancel() {
        status.set(status.get().toBuilder().setPhase(SnapshotSyncLeaseRecord.Phase.RECOVERING).build());
        drive();
        source.stop();
        source.reset();
        assertTrue(sent.isEmpty());
    }

    /** The worker only records a failure in a Future nobody reads: a step must reschedule itself. */
    @Test
    void aStepThatDiesWithAnErrorIsStillRetried() throws Exception {
        when(reader.read(any())).thenThrow(new AssertionError("unexpected"));
        drive();
        LogReplicationEntryMsg start = sent.get(0);
        SnapshotSyncLeaseRecord reserved = SnapshotSyncLease.reserve(status.get(), start.getMetadata(), 1000, 10000, 20, "protection");
        status.set(SnapshotSyncLease.prepared(reserved));
        admission.complete(start.toBuilder().setMetadata(start.getMetadata().toBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_START_ACCEPTED).setAttemptGeneration(reserved.getGeneration())).build());
        TimeUnit.MILLISECONDS.sleep(200); // The acceptance asks for a step itself: let that one pass.
        CountDownLatch continued = nextContinuation();

        assertThrows(AssertionError.class, this::drive);

        assertTrue(continued.await(5, TimeUnit.SECONDS), "nothing would ever run this snapshot sync again");
    }

    @Test
    void resendTimerMeasuresElapsedTimeRatherThanPollCount() {
        AtomicLong now = new AtomicLong(100);
        LogReplicationPendingEntry pending = new LogReplicationPendingEntry(LogReplicationEntryMsg.getDefaultInstance(), now::get);
        for (int i = 0; i < 1000; i++) { assertFalse(pending.timeout(500)); }
        now.set(601);
        assertTrue(pending.timeout(500));
        pending.retry();
        assertFalse(pending.timeout(500));
        assertEquals(1, pending.getRetry());
    }
}
