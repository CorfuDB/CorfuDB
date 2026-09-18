package org.corfudb.infrastructure.logreplication.runtime.fsm;

import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientRouter;
import org.corfudb.infrastructure.logreplication.runtime.fsm.LogReplicationRuntimeEvent.LogReplicationRuntimeEventType;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Outcome;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Phase;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.SnapshotSyncLease;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Snapshot sync is governed by the sink's lease and there is no other protocol. The sink cluster is
 * upgraded first, so the only mixed pairing is a source that does not know the lease talking to a
 * sink that requires it, which the sink rejects. A source of this version that meets a sink without
 * a lease (a rollback, or a wrong pairing) must not replicate either, and must not retry in a loop.
 */
class SnapshotNegotiatingStateTest {
    private CorfuLogReplicationRuntime fsm;
    private LogReplicationClientRouter router;
    private NegotiatingState state;
    private final LogReplicationMetadataResponseMsg.Builder response = LogReplicationMetadataResponseMsg.newBuilder()
            .setTopologyConfigID(1).setSnapshotStart(50).setSnapshotTransferred(50)
            .setSnapshotApplied(50).setLastLogEntryTimestamp(60);

    @BeforeEach
    void setup() {
        fsm = mock(CorfuLogReplicationRuntime.class, RETURNS_DEEP_STUBS);
        when(fsm.getRemoteLeaderNodeId()).thenReturn(Optional.of("sink"));
        router = mock(LogReplicationClientRouter.class);
        when(router.getRemoteLeaderConnectionFuture()).thenReturn(new CompletableFuture<>());
        when(router.sendRequestAndGetCompletable(any(), eq("sink"))).thenAnswer(call -> {
            RequestPayloadMsg request = call.getArgument(0);
            assertTrue(request.getLrMetadataRequest().getSupportsSnapshotLifecycle());
            return CompletableFuture.completedFuture(response.build());
        });
        LogReplicationMetadataManager metadata = mock(LogReplicationMetadataManager.class);
        when(metadata.getTopologyConfigId()).thenReturn(1L);
        when(metadata.getLogHead()).thenReturn(10L);
        ThreadPoolExecutor worker = mock(ThreadPoolExecutor.class);
        when(worker.getQueue()).thenReturn(new LinkedBlockingQueue<>());
        doAnswer(call -> { ((Runnable) call.getArgument(0)).run(); return null; }).when(worker).submit(any(Runnable.class));
        state = new NegotiatingState(fsm, worker, router, metadata,
                mock(LogReplicationConfigManager.class, RETURNS_DEEP_STUBS));
    }

    private void verifyNegotiated(LogReplicationEventType expected) {
        verify(fsm).input(argThat(event -> event.getType() == LogReplicationRuntimeEventType.NEGOTIATION_COMPLETE
                && event.getNegotiationResult().getType() == expected));
    }

    private void verifyRejected() {
        verify(fsm).input(argThat(event -> event.getType() == LogReplicationRuntimeEventType.NEGOTIATION_FAILED));
        verify(fsm, never()).input(argThat(event -> event.getType() == LogReplicationRuntimeEventType.NEGOTIATION_COMPLETE));
        assertFalse(router.getRemoteLeaderConnectionFuture().isDone(), "a rejected negotiation must not open the channel");
    }

    @Test
    void anUnfinishedLeaseOverridesTheTimestampsAndUsesTheAdmissionWorkflow() {
        // The timestamps above describe a completed snapshot; only the lease knows that an attempt
        // is in flight, was abandoned, or that the sink still owes a recovery.
        for (Phase phase : new Phase[]{Phase.READY, Phase.PREPARING, Phase.TRANSFERRING, Phase.APPLYING,
                Phase.ABORTING, Phase.RELEASING, Phase.RECOVERING, Phase.FAULTED}) {
            for (Outcome outcome : new Outcome[]{Outcome.NONE, Outcome.ABORTED}) {
                response.setSnapshotLease(SnapshotSyncLeaseRecord.newBuilder().setSchemaVersion(1)
                        .setPhase(phase).setOutcome(outcome));
                state.onEntry(state);
                verifyNegotiated(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST);
                clearInvocations(fsm);
            }
        }
    }

    @Test
    void aCompletedLeaseCanResumeIncrementalWhileCleanupRemainsPending() {
        response.setSnapshotLease(SnapshotSyncLeaseRecord.newBuilder().setSchemaVersion(1)
                .setPhase(Phase.RECOVERING).setOutcome(Outcome.COMPLETED));
        state.onEntry(state);
        verifyNegotiated(LogReplicationEventType.LOG_ENTRY_SYNC_REQUEST);
        assertTrue(router.getRemoteLeaderConnectionFuture().isDone());
    }

    /** What an upgraded sink seeds when its last legacy snapshot had completed: replication continues. */
    @Test
    void aSinkSeededIdleAtUpgradeResumesIncrementalSync() {
        response.setSnapshotLease(SnapshotSyncLease.seedIdle("sink", 50, 1));
        state.onEntry(state);
        verifyNegotiated(LogReplicationEventType.LOG_ENTRY_SYNC_REQUEST);
    }

    /** What an upgraded sink seeds when a legacy snapshot was pending: it is restarted under the lease. */
    @Test
    void aSinkSeededRecoveringAtUpgradeRestartsTheSnapshot() {
        response.setSnapshotLease(SnapshotSyncLease.seedRecovering("sink", 1_000, 40, 50, "Pre-lease snapshot superseded"));
        state.onEntry(state);
        verifyNegotiated(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST);
    }

    @Test
    void aNeverSyncedSinkStillStartsWithASnapshot() {
        response.setSnapshotStart(-1).setSnapshotTransferred(-1).setSnapshotApplied(-1).setLastLogEntryTimestamp(-1)
                .setSnapshotLease(SnapshotSyncLease.seedIdle("sink", -1, 1));
        state.onEntry(state);
        verifyNegotiated(LogReplicationEventType.SNAPSHOT_SYNC_REQUEST);
    }

    @Test
    void aSinkWithoutALeaseIsRejectedAndNotRetriedInATightLoop() {
        long start = System.nanoTime();
        state.onEntry(state);
        verifyRejected();
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) >= 900,
                "the rejection is paced: a failed negotiation is retried immediately by the runtime FSM");
    }

    @Test
    void aSinkThatHasNotInitializedItsLeaseIsRetriedInsteadOfBeingForcedIntoASnapshot() {
        response.setSnapshotLease(SnapshotSyncLeaseRecord.getDefaultInstance());
        state.onEntry(state);
        verifyRejected();
        clearInvocations(fsm);

        response.setSnapshotLease(SnapshotSyncLeaseRecord.newBuilder().setSchemaVersion(1).setPhase(Phase.NOT_READY));
        state.onEntry(state);
        verifyRejected();
    }

    @Test
    void theLeaseDoesNotBypassTopologyValidation() {
        for (long sinkTopology : new long[]{0, 2}) {
            response.setTopologyConfigID(sinkTopology).setSnapshotLease(SnapshotSyncLease.seedIdle("sink", 50, sinkTopology));
            state.onEntry(state);
            verifyRejected();
            clearInvocations(fsm);
        }
    }
}
