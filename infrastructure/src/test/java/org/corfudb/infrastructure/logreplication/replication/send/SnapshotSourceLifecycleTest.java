package org.corfudb.infrastructure.logreplication.replication.send;

import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationAckReader;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReader;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReadMessage;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.*;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SnapshotSourceLifecycleTest {
    private CorfuRuntime runtime;
    private DataSender transport;
    private SnapshotSender source;
    private SnapshotReader reader;
    private LogReplicationFSM fsm;
    private final UUID eventId = UUID.randomUUID();
    private final AtomicReference<SnapshotSyncLeaseRecord> status = new AtomicReference<>(SnapshotSyncLease.initial("sink"));
    private final List<LogReplicationEntryMsg> sent = new ArrayList<>();
    private CompletableFuture<LogReplicationEntryMsg> admission;

    @BeforeEach
    void setup() {
        runtime = mock(CorfuRuntime.class);
        AddressSpaceView addressSpace = mock(AddressSpaceView.class);
        when(runtime.getAddressSpaceView()).thenReturn(addressSpace);
        when(addressSpace.getLogTail()).thenReturn(50L);
        fsm = mock(LogReplicationFSM.class);
        when(fsm.getAckReader()).thenReturn(mock(LogReplicationAckReader.class));
        reader = mock(SnapshotReader.class);
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
        source = new SnapshotSender(runtime, reader, transport, null, 1, fsm);
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
    void noStartDuringRecoveryAndNoBulkDataBeforeExplicitAcceptance() {
        status.set(status.get().toBuilder().setPhase(SnapshotSyncLeaseRecord.Phase.RECOVERING).build());
        drive();
        assertTrue(sent.isEmpty());
        verify(reader, never()).read(any());
        status.set(SnapshotSyncLease.initial("sink"));
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

    @Test
    void resendTimerMeasuresElapsedTimeRatherThanPollCount() {
        java.util.concurrent.atomic.AtomicLong now = new java.util.concurrent.atomic.AtomicLong(100);
        LogReplicationPendingEntry pending = new LogReplicationPendingEntry(LogReplicationEntryMsg.getDefaultInstance(), now::get);
        for (int i = 0; i < 1000; i++) { assertFalse(pending.timeout(500)); }
        now.set(601);
        assertTrue(pending.timeout(500));
        pending.retry();
        assertFalse(pending.timeout(500));
        assertEquals(1, pending.getRetry());
    }
}
