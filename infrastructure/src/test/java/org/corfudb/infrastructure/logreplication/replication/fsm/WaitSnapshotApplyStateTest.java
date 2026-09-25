package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationAckReader;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationEventMetadata;
import org.corfudb.infrastructure.logreplication.replication.send.SnapshotSender;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.protocols.CorfuProtocolCommon;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Outcome;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Phase;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * While the sink applies a transferred snapshot the source has no timer of its own. It follows the
 * sink's snapshot lease: the attempt completes, or the sink reports that it abandoned it (its
 * deadline, a failed apply, a change of owner or topology), and only then does the source cancel
 * and ask for a new attempt.
 */
@Slf4j
public class WaitSnapshotApplyStateTest {

    private LogReplicationFSM fsm;
    private InSnapshotSyncState inSnapshotSyncState;
    private WaitSnapshotApplyState state;
    private DataSender dataSender;

    private void setup() {
        fsm = mock(LogReplicationFSM.class);
        LogReplicationAckReader ackReader = mock(LogReplicationAckReader.class);
        dataSender = mock(DataSender.class);
        LogReplicationConfigManager tableManagerPlugin = mock(LogReplicationConfigManager.class);
        SnapshotSender snapshotSender = mock(SnapshotSender.class);

        inSnapshotSyncState = new InSnapshotSyncState(fsm, snapshotSender);
        state = new WaitSnapshotApplyState(fsm, dataSender, tableManagerPlugin);

        Map<LogReplicationStateType, LogReplicationState> states = new HashMap<>();
        states.put(LogReplicationStateType.IN_SNAPSHOT_SYNC, inSnapshotSyncState);
        states.put(LogReplicationStateType.IN_LOG_ENTRY_SYNC, mock(LogReplicationState.class));

        when(fsm.getAckReader()).thenReturn(ackReader);
        when(fsm.isValidTransition(any(), any())).thenReturn(true);
        when(fsm.getStates()).thenReturn(states);
        when(fsm.getLogReplicationFSMWorkers()).thenReturn(mock(ExecutorService.class));
    }

    @After
    public void cancelCallbacks() {
        if (state != null) {
            LogReplicationState stopped = mock(LogReplicationState.class);
            when(stopped.getType()).thenReturn(LogReplicationStateType.INITIALIZED);
            state.onExit(stopped);
        }
    }

    /** The lease of the attempt this source is waiting on, still being applied by the sink. */
    private SnapshotSyncLeaseRecord ownedStatus() {
        UUID wireId = new UUID(11, 22);
        when(inSnapshotSyncState.getSnapshotSender().getWireAttemptId()).thenReturn(wireId);
        when(inSnapshotSyncState.getSnapshotSender().getWireAttemptGeneration()).thenReturn(7L);
        state.setTransitionSyncId(UUID.randomUUID());
        state.setBaseSnapshotTimestamp(100);
        return SnapshotSyncLeaseRecord.newBuilder().setSchemaVersion(1)
                .setAttemptId(CorfuProtocolCommon.getUuidMsg(wireId)).setGeneration(7)
                .setSourceSnapshot(100).setPhase(Phase.APPLYING).build();
    }

    /**
     * Neither completion nor cancellation was reported. The next check, which the state schedules
     * for itself and which may already have come round, is not a conclusion.
     */
    private void nothingWasConcluded() {
        verify(fsm, never()).input(argThat(event ->
                event.getType() != LogReplicationEvent.LogReplicationEventType.SNAPSHOT_APPLY_IN_PROGRESS));
    }

    private void sinkReports(SnapshotSyncLeaseRecord lease) {
        when(dataSender.sendMetadataRequest()).thenReturn(CompletableFuture.completedFuture(
                LogReplicationMetadataResponseMsg.newBuilder().setSnapshotLease(lease).build()));
    }

    // ---------------------------------------------------------------- following the lease

    @Test
    public void anApplyInProgressIsFollowedForAsLongAsTheSinkRunsIt() {
        setup();
        sinkReports(ownedStatus());
        for (int poll = 0; poll < 5; poll++) {
            state.verifyStatusOfSnapshotSyncApply();
        }
        // No source-side bound exists: only the sink's deadline can end this attempt.
        nothingWasConcluded();
    }

    /** The first checks of an apply come sooner than the later ones: a small snapshot is applied in no time. */
    @Test
    public void theFirstChecksComeSoonerAndTheLaterOnesSettle() {
        setup();
        for (int check = 0; check < 8; check++) {
            Assert.assertEquals(250, state.nextVerificationDelayMs());
        }
        Assert.assertEquals(2000, state.nextVerificationDelayMs());
        Assert.assertEquals(2000, state.nextVerificationDelayMs());
        // Entering the state for another attempt starts over.
        state.onExit(inSnapshotSyncState);
        state.onEntry(inSnapshotSyncState);
        Assert.assertEquals(250, state.nextVerificationDelayMs());
    }

    @Test
    public void completionSurvivesTheSinksCleanupAndRecovery() {
        setup();
        sinkReports(ownedStatus().toBuilder().setPhase(Phase.RECOVERING).setOutcome(Outcome.COMPLETED).build());
        state.verifyStatusOfSnapshotSyncApply();
        verify(fsm).input(argThat(event ->
                event.getType() == LogReplicationEvent.LogReplicationEventType.SNAPSHOT_APPLY_COMPLETE
                        && new UUID(11, 22).equals(event.getMetadata().getSnapshotAttemptId())
                        && event.getMetadata().getSnapshotAttemptGeneration() == 7));
    }

    @Test
    public void fullUuidMismatchAndAbandonmentCancelEvenWhenTimestampsMatch() {
        setup();
        sinkReports(ownedStatus().toBuilder().setAttemptId(CorfuProtocolCommon.getUuidMsg(new UUID(11, 23))).build());
        state.verifyStatusOfSnapshotSyncApply();
        verify(fsm).input(argThat(event -> event.getType() == LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL));

        clearInvocations(fsm);
        sinkReports(ownedStatus().toBuilder().setGeneration(8).build());
        state.verifyStatusOfSnapshotSyncApply();
        verify(fsm).input(argThat(event -> event.getType() == LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL));

        clearInvocations(fsm);
        sinkReports(ownedStatus().toBuilder().setPhase(Phase.RECOVERING).setOutcome(Outcome.ABORTED).build());
        state.verifyStatusOfSnapshotSyncApply();
        verify(fsm).input(argThat(event -> event.getType() == LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL));
    }

    @Test
    public void anUninitializedSinkAndATransportFailureConcludeNothing() {
        setup();
        ownedStatus();
        when(dataSender.sendMetadataRequest()).thenReturn(CompletableFuture.failedFuture(new IllegalStateException("disconnected")));
        state.verifyStatusOfSnapshotSyncApply();
        // A sink whose new leader has not initialized its lease yet publishes a placeholder.
        sinkReports(SnapshotSyncLeaseRecord.getDefaultInstance());
        state.verifyStatusOfSnapshotSyncApply();
        nothingWasConcluded();
    }

    @Test
    public void aSinkWithoutALeaseIsNeverMistakenForACompletedOrFailedApply() {
        setup();
        ownedStatus();
        // Matching timestamps from a sink that predates the lease prove nothing about this attempt.
        when(dataSender.sendMetadataRequest()).thenReturn(CompletableFuture.completedFuture(
                LogReplicationMetadataResponseMsg.newBuilder().setSnapshotStart(100).setSnapshotTransferred(100)
                        .setSnapshotApplied(100).setLastLogEntryTimestamp(100).build()));
        state.verifyStatusOfSnapshotSyncApply();
        state.verifyStatusOfSnapshotSyncApply();
        nothingWasConcluded();
    }

    @Test
    public void replyAfterExitCannotCompleteAStoppedAttempt() throws Exception {
        setup();
        SnapshotSyncLeaseRecord lease = ownedStatus().toBuilder().setOutcome(Outcome.COMPLETED).build();
        CompletableFuture<LogReplicationMetadataResponseMsg> reply = new CompletableFuture<>();
        CountDownLatch queried = new CountDownLatch(1);
        when(dataSender.sendMetadataRequest()).thenAnswer(call -> { queried.countDown(); return reply; });
        CompletableFuture<Void> verifying = CompletableFuture.runAsync(state::verifyStatusOfSnapshotSyncApply);
        Assert.assertTrue(queried.await(2, TimeUnit.SECONDS));
        cancelCallbacks();
        reply.complete(LogReplicationMetadataResponseMsg.newBuilder().setSnapshotLease(lease).build());
        verifying.get(2, TimeUnit.SECONDS);
        verify(fsm, never()).input(any());
    }

    // ---------------------------------------------------------------- events

    @Test
    public void delayedCompletionCannotFinishAnotherAttemptWithTheSameForcedRequestId() throws Exception {
        setup();
        ownedStatus();
        LogReplicationEventMetadata oldAttempt = new LogReplicationEventMetadata(state.getTransitionSyncId(), 100, 100, true)
                .setSnapshotAttempt(new UUID(11, 23), 7);
        Assert.assertSame(state, state.processEvent(new LogReplicationEvent(
                LogReplicationEvent.LogReplicationEventType.SNAPSHOT_APPLY_COMPLETE, oldAttempt)));
        oldAttempt.setSnapshotAttempt(new UUID(11, 22), 6);
        Assert.assertSame(state, state.processEvent(new LogReplicationEvent(
                LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL, oldAttempt)));
        verify(fsm.getAckReader(), never()).markLogEntrySyncOngoing(anyBoolean());
        Assert.assertEquals(0, inSnapshotSyncState.consecutiveCancellations);
    }

    @Test
    public void aQueuedTransferContinuationIsHarmless() throws IllegalTransitionException {
        setup();
        state.setTransitionSyncId(UUID.randomUUID());
        Assert.assertSame(state, state.processEvent(new LogReplicationEvent(
                LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_CONTINUE,
                new LogReplicationEventMetadata(state.getTransitionSyncId()))));
    }

    @Test
    public void syncCancelRestartsAndCountsTheCancellation() throws IllegalTransitionException {
        setup();
        UUID syncId = UUID.randomUUID();
        state.setTransitionSyncId(syncId);

        LogReplicationState next = state.processEvent(new LogReplicationEvent(
                LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL, new LogReplicationEventMetadata(syncId)));

        Assert.assertSame(inSnapshotSyncState, next);
        Assert.assertEquals(1, inSnapshotSyncState.consecutiveCancellations);
        Assert.assertNotEquals("a cancelled default sync restarts under a new request id",
                syncId, inSnapshotSyncState.getTransitionSyncId());
    }

    @Test
    public void aCancelledForcedSyncKeepsItsRequestId() throws IllegalTransitionException {
        setup();
        UUID syncId = UUID.randomUUID();
        state.setTransitionSyncId(syncId);

        state.processEvent(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL,
                new LogReplicationEventMetadata(syncId, true)));

        Assert.assertEquals(syncId, inSnapshotSyncState.getTransitionSyncId());
    }

    @Test
    public void aSnapshotSyncRequestWhileWaitingStartsANewRunOfAttempts() throws IllegalTransitionException {
        setup();
        state.setTransitionSyncId(UUID.randomUUID());
        inSnapshotSyncState.registerCancellation();
        UUID requested = UUID.randomUUID();

        LogReplicationState next = state.processEvent(new LogReplicationEvent(
                LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST, new LogReplicationEventMetadata(requested)));

        Assert.assertSame(inSnapshotSyncState, next);
        Assert.assertEquals(requested, inSnapshotSyncState.getTransitionSyncId());
        Assert.assertEquals(0, inSnapshotSyncState.consecutiveCancellations);
    }

    @Test
    public void stopShutdownAndCompletionEndTheRunOfAttempts() throws IllegalTransitionException {
        setup();
        UUID syncId = UUID.randomUUID();
        state.setTransitionSyncId(syncId);
        for (LogReplicationEvent.LogReplicationEventType boundary : new LogReplicationEvent.LogReplicationEventType[]{
                LogReplicationEvent.LogReplicationEventType.REPLICATION_STOP,
                LogReplicationEvent.LogReplicationEventType.REPLICATION_SHUTDOWN,
                LogReplicationEvent.LogReplicationEventType.SNAPSHOT_APPLY_COMPLETE}) {
            inSnapshotSyncState.registerCancellation();
            inSnapshotSyncState.registerCancellation();
            state.processEvent(new LogReplicationEvent(boundary, new LogReplicationEventMetadata(syncId, 0L, 0L, false)));
            Assert.assertEquals(boundary + " is a clean boundary; a later session must not inherit this count",
                    0, inSnapshotSyncState.consecutiveCancellations);
        }
    }
}
