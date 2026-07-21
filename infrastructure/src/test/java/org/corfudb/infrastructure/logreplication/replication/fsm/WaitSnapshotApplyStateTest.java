package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
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
 * Verifies that a SYNC_CANCEL or SNAPSHOT_SYNC_REQUEST arriving while waiting for snapshot apply to
 * complete (i.e. transfer already finished, only the sink's apply confirmation is pending) routes
 * through the same backoff accounting InSnapshotSyncState applies to its own SYNC_CANCEL, instead of
 * restarting the snapshot sync unthrottled. Before this fix (this is a fresh implementation, not a
 * regression against prior behavior, since master has no backoff at all), a cancellation reaching
 * this state would bypass any backoff, making this path another avenue for a restart storm.
 *
 * Also verifies the backoff is cleared on a full completion (SNAPSHOT_APPLY_COMPLETE) and on a clean
 * stop/shutdown boundary, so a later, unrelated session doesn't inherit it.
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
        Assert.assertEquals(LogReplicationConfig.INITIAL_RETRY_BACKOFF_MS, inSnapshotSyncState.retryBackoffMs);
    }

    @Test
    public void snapshotSyncRequestWhileWaitingForApplyAppliesBackoff() throws IllegalTransitionException {
        setup();
        UUID syncId = UUID.randomUUID();
        state.setTransitionSyncId(syncId);

        LogReplicationEvent request = new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST,
                new LogReplicationEventMetadata(UUID.randomUUID()));

        LogReplicationState next = state.processEvent(request);

        Assert.assertSame(inSnapshotSyncState, next);
        Assert.assertEquals("a request arriving during apply-wait abandons and restarts the same way " +
                        "a SYNC_CANCEL does, and must not bypass the backoff accounting",
                1, inSnapshotSyncState.consecutiveCancellations);
        Assert.assertEquals(LogReplicationConfig.INITIAL_RETRY_BACKOFF_MS, inSnapshotSyncState.retryBackoffMs);
    }

    @Test
    public void replicationStopResetsBackoff() throws IllegalTransitionException {
        setup();
        inSnapshotSyncState.registerCancellationAndComputeBackoff();
        inSnapshotSyncState.registerCancellationAndComputeBackoff();
        Assert.assertTrue(inSnapshotSyncState.consecutiveCancellations > 0);

        UUID syncId = UUID.randomUUID();
        state.setTransitionSyncId(syncId);
        LogReplicationEvent stop = new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.REPLICATION_STOP,
                new LogReplicationEventMetadata(syncId));

        state.processEvent(stop);

        Assert.assertEquals("a stop is a clean boundary; a later, unrelated session must not inherit this backoff",
                0, inSnapshotSyncState.consecutiveCancellations);
        Assert.assertEquals(0, inSnapshotSyncState.retryBackoffMs);
    }

    @Test
    public void replicationShutdownResetsBackoff() throws IllegalTransitionException {
        setup();
        inSnapshotSyncState.registerCancellationAndComputeBackoff();
        Assert.assertTrue(inSnapshotSyncState.consecutiveCancellations > 0);

        UUID syncId = UUID.randomUUID();
        state.setTransitionSyncId(syncId);
        LogReplicationEvent shutdown = new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.REPLICATION_SHUTDOWN,
                new LogReplicationEventMetadata(syncId));

        state.processEvent(shutdown);

        Assert.assertEquals(0, inSnapshotSyncState.consecutiveCancellations);
        Assert.assertEquals(0, inSnapshotSyncState.retryBackoffMs);
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
