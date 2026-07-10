package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationAckReader;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationEventMetadata;
import org.corfudb.infrastructure.logreplication.replication.send.SnapshotSender;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that a SYNC_CANCEL arriving while waiting for snapshot apply to complete (i.e. transfer
 * already finished, only the sink's apply confirmation is pending) routes through the same backoff
 * accounting InSnapshotSyncState applies to its own SYNC_CANCEL, instead of restarting the
 * snapshot sync immediately. Before this fix, a cancellation reaching this state bypassed the
 * backoff entirely, reproducing the cancel-and-immediately-retry livelock via this path.
 *
 * Also verifies that the backoff/cancellation counters are only cleared once the snapshot sync
 * fully completes (SNAPSHOT_APPLY_COMPLETE), not merely when a transfer attempt succeeds. Clearing
 * on transfer success alone (the previous behavior) meant a failure localized to the apply phase --
 * transfer keeps succeeding, only apply keeps failing/getting canceled -- would zero the counter
 * every cycle right before the next apply-side cancellation could grow it, pinning the backoff at
 * its initial value forever instead of escalating for a persistently repeating failure.
 *
 * See InSnapshotSyncStateTest for the primary backoff coverage.
 */
@Slf4j
public class WaitSnapshotApplyStateTest {

    private LogReplicationFSM fsm;
    private InSnapshotSyncState inSnapshotSyncState;
    private WaitSnapshotApplyState state;

    private void setup() {
        fsm = mock(LogReplicationFSM.class);
        LogReplicationAckReader ackReader = mock(LogReplicationAckReader.class);
        DataSender dataSender = mock(DataSender.class);
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
    }

    @Test
    public void syncCancelAppliesBackoffBeforeRestarting() throws IllegalTransitionException {
        setup();
        UUID syncId = UUID.randomUUID();
        state.setTransitionSyncId(syncId);

        LogReplicationEvent cancel = new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL,
                new LogReplicationEventMetadata(syncId));

        LogReplicationState next = state.processEvent(cancel);

        Assert.assertSame(inSnapshotSyncState, next);
        Assert.assertEquals(1, inSnapshotSyncState.consecutiveCancellations);
        Assert.assertEquals(InSnapshotSyncState.INITIAL_RETRY_BACKOFF_MS, inSnapshotSyncState.retryBackoffMs);
    }

    @Test
    public void repeatedTransferSuccessWithApplyCancelEscalatesBackoff() throws IllegalTransitionException {
        setup();
        LogReplicationEvent cancel = new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL,
                new LogReplicationEventMetadata(UUID.randomUUID()));

        // Cycle 1: transfer succeeds (InSnapshotSyncState exits to WAIT_SNAPSHOT_APPLY), then apply
        // is canceled.
        inSnapshotSyncState.onExit(state);
        state.setTransitionSyncId(UUID.randomUUID());
        state.processEvent(cancel);
        Assert.assertEquals(1, inSnapshotSyncState.consecutiveCancellations);
        Assert.assertEquals(InSnapshotSyncState.INITIAL_RETRY_BACKOFF_MS, inSnapshotSyncState.retryBackoffMs);

        // Cycle 2: the retry's transfer succeeds again, then apply is canceled again. The backoff
        // must keep growing, not reset back to the initial value merely because transfer succeeded.
        inSnapshotSyncState.onExit(state);
        state.setTransitionSyncId(UUID.randomUUID());
        state.processEvent(cancel);
        Assert.assertEquals(2, inSnapshotSyncState.consecutiveCancellations);
        Assert.assertEquals(InSnapshotSyncState.INITIAL_RETRY_BACKOFF_MS * 2, inSnapshotSyncState.retryBackoffMs);
    }

    @Test
    public void snapshotApplyCompleteResetsBackoff() throws IllegalTransitionException {
        setup();
        // Simulate a couple of prior retries before this attempt finally succeeds end to end.
        inSnapshotSyncState.registerCancellationAndComputeBackoff();
        inSnapshotSyncState.registerCancellationAndComputeBackoff();
        Assert.assertTrue(inSnapshotSyncState.consecutiveCancellations > 0);
        Assert.assertTrue(inSnapshotSyncState.retryBackoffMs > 0);

        UUID syncId = UUID.randomUUID();
        state.setTransitionSyncId(syncId);
        LogReplicationEvent applyComplete = new LogReplicationEvent(
                LogReplicationEvent.LogReplicationEventType.SNAPSHOT_APPLY_COMPLETE,
                new LogReplicationEventMetadata(syncId, 0L, 0L, false));

        state.processEvent(applyComplete);

        Assert.assertEquals(0, inSnapshotSyncState.consecutiveCancellations);
        Assert.assertEquals(0, inSnapshotSyncState.retryBackoffMs);
    }
}
