package org.corfudb.infrastructure.logreplication.runtime.fsm;

import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationEvent.LogReplicationEventType;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.runtime.CorfuLogReplicationRuntime;
import org.corfudb.infrastructure.logreplication.runtime.LogReplicationClientRouter;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.corfudb.runtime.LogReplication.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.LogReplication.SnapshotSyncLeaseRecord.Outcome;
import org.corfudb.runtime.LogReplication.SnapshotSyncLeaseRecord.Phase;
import org.corfudb.runtime.proto.service.CorfuMessage.RequestPayloadMsg;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

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

    @Test
    void unfinishedLeaseOverridesLegacyCompletionAndUsesAdmissionWorkflow() {
        for (Phase phase : new Phase[]{Phase.NOT_READY, Phase.READY, Phase.TRANSFERRING, Phase.APPLYING,
                Phase.ABORTING, Phase.RELEASING, Phase.RECOVERING, Phase.FAULTED}) {
            response.setSnapshotLease(SnapshotSyncLeaseRecord.newBuilder().setSchemaVersion(1).setPhase(phase));
            state.onEntry(state);
            verify(fsm).input(argThat(event -> event.getType()
                    == LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_COMPLETE
                    && event.getNegotiationResult().getType() == LogReplicationEventType.SNAPSHOT_SYNC_REQUEST));
            clearInvocations(fsm);
        }
    }

    @Test
    void completedLeaseCanResumeIncrementalWhileCleanupRemainsPending() {
        response.setSnapshotLease(SnapshotSyncLeaseRecord.newBuilder().setSchemaVersion(1)
                .setPhase(Phase.RECOVERING).setOutcome(Outcome.COMPLETED));
        state.onEntry(state);
        verify(fsm).input(argThat(event -> event.getNegotiationResult().getType() == LogReplicationEventType.LOG_ENTRY_SYNC_REQUEST));
        assertTrue(router.getRemoteLeaderConnectionFuture().isDone());
    }

    @Test
    void legacySinkRetainsTimestampNegotiation() {
        state.onEntry(state);
        verify(fsm).input(argThat(event -> event.getNegotiationResult().getType() == LogReplicationEventType.LOG_ENTRY_SYNC_REQUEST));
    }

    @Test
    void leaseDoesNotBypassTopologyValidation() {
        response.setTopologyConfigID(2).setSnapshotLease(SnapshotSyncLeaseRecord.newBuilder().setSchemaVersion(1));
        state.onEntry(state);
        verify(fsm).input(argThat(event -> event.getType()
                == LogReplicationRuntimeEvent.LogReplicationRuntimeEventType.NEGOTIATION_FAILED));
    }
}
