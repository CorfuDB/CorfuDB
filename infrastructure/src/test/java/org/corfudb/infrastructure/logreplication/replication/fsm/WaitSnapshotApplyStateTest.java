package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationAckReader;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationEventMetadata;
import org.corfudb.infrastructure.logreplication.replication.send.SnapshotSender;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
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
 * See InSnapshotSyncStateTest for the primary backoff coverage.
 */
@Slf4j
public class WaitSnapshotApplyStateTest {

    @Test
    public void syncCancelAppliesBackoffBeforeRestarting() throws IllegalTransitionException {
        LogReplicationFSM fsm = mock(LogReplicationFSM.class);
        LogReplicationAckReader ackReader = mock(LogReplicationAckReader.class);
        DataSender dataSender = mock(DataSender.class);
        LogReplicationConfigManager tableManagerPlugin = mock(LogReplicationConfigManager.class);
        SnapshotSender snapshotSender = mock(SnapshotSender.class);

        InSnapshotSyncState inSnapshotSyncState = new InSnapshotSyncState(fsm, snapshotSender);
        WaitSnapshotApplyState state = new WaitSnapshotApplyState(fsm, dataSender, tableManagerPlugin);

        when(fsm.getAckReader()).thenReturn(ackReader);
        when(fsm.isValidTransition(any(), any())).thenReturn(true);
        when(fsm.getStates()).thenReturn(Collections.singletonMap(LogReplicationStateType.IN_SNAPSHOT_SYNC, inSnapshotSyncState));

        UUID syncId = UUID.randomUUID();
        state.setTransitionSyncId(syncId);

        LogReplicationEvent cancel = new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL,
                new LogReplicationEventMetadata(syncId));

        LogReplicationState next = state.processEvent(cancel);

        Assert.assertSame(inSnapshotSyncState, next);
        Assert.assertEquals(1, inSnapshotSyncState.consecutiveCancellations);
        Assert.assertEquals(InSnapshotSyncState.INITIAL_RETRY_BACKOFF_MS, inSnapshotSyncState.retryBackoffMs);
    }
}
