package org.corfudb.infrastructure.logreplication.replication.receive;

import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.view.Address;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.UUID;

import static org.corfudb.protocols.CorfuProtocolCommon.getUUID;
import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for SnapshotSinkBufferManager, in particular the activeSyncRequestId identity check
 * (verifyMessageType()) added to close the cross-attempt data-mixing gap: since every snapshot-
 * sync attempt's sequence numbers restart from Address.NON_ADDRESS (SenderBufferManager.reset()),
 * a straggler message from a prior, already-cancelled attempt (still in flight when cancelled --
 * cancellation can't retract an in-flight RPC) could otherwise coincidentally match a currently-
 * buffered seqNum slot and get applied into a newer attempt's shadow streams as if it were current
 * data. This had no dedicated unit coverage before -- only indirectly through LogReplicationIT.
 */
public class SnapshotSinkBufferManagerTest {

    private static final int ACK_CYCLE_TIME_MS = 1000;
    private static final int ACK_CYCLE_CNT = 1;
    private static final int BUFFER_SIZE = 10;

    private final UUID activeSyncId = UUID.randomUUID();
    private LogReplicationSinkManager sinkManager;
    private SnapshotSinkBufferManager buffer;

    @Before
    public void setup() {
        sinkManager = mock(LogReplicationSinkManager.class);
        doReturn(true).when(sinkManager).processMessage(any());
        buffer = new SnapshotSinkBufferManager(ACK_CYCLE_TIME_MS, ACK_CYCLE_CNT, BUFFER_SIZE,
                Address.NON_ADDRESS, activeSyncId, sinkManager);
    }

    private LogReplicationEntryMsg msg(UUID syncId, LogReplicationEntryType type, long seqNum) {
        LogReplicationEntryMetadataMsg metadata = LogReplicationEntryMetadataMsg.newBuilder()
                .setEntryType(type)
                .setSyncRequestId(getUuidMsg(syncId))
                .setSnapshotSyncSeqNum(seqNum)
                .build();
        return LogReplicationEntryMsg.newBuilder().setMetadata(metadata).build();
    }

    @Test
    public void rejectsMessageFromADifferentAttemptOutright() {
        LogReplicationEntryMsg stale = msg(UUID.randomUUID(), LogReplicationEntryType.SNAPSHOT_MESSAGE, 0);

        LogReplicationEntryMsg ack = buffer.processMsgAndBuffer(stale);

        Assert.assertNull(ack);
        verify(sinkManager, never()).processMessage(any());
        Assert.assertEquals(Address.NON_ADDRESS, buffer.lastProcessedSeq);
    }

    @Test
    public void acceptsInOrderMessageFromTheActiveAttempt() {
        LogReplicationEntryMsg m = msg(activeSyncId, LogReplicationEntryType.SNAPSHOT_MESSAGE, 0);

        buffer.processMsgAndBuffer(m);

        verify(sinkManager, times(1)).processMessage(m);
        Assert.assertEquals(0, buffer.lastProcessedSeq);
    }

    /**
     * Reproduces the exact scenario from the review finding this fix addresses: attempt N is
     * cancelled after seqNum 9 is already in flight; attempt N+1 (a fresh SnapshotSinkBufferManager
     * instance, since LogReplicationSinkManager.processSnapshotStart() constructs a new one per
     * attempt) starts from scratch and is only partway through its own, unrelated seqNum sequence
     * when N's straggler for seqNum 10 arrives. Without the identity check, the straggler would
     * satisfy "currentTs > lastProcessedSeq" purely by number and get buffered at key 9 (preTs);
     * when N+1 legitimately reaches seqNum 9, processBuffer() would look up that same key and
     * apply the straggler's stale data as if it were N+1's own seqNum 9.
     */
    @Test
    public void staleStragglerFromACancelledAttemptCannotBeMisappliedIntoTheNewAttempt() {
        for (long seq = 0; seq <= 8; seq++) {
            buffer.processMsgAndBuffer(msg(activeSyncId, LogReplicationEntryType.SNAPSHOT_MESSAGE, seq));
        }
        Assert.assertEquals(8, buffer.lastProcessedSeq);

        UUID cancelledAttemptSyncId = UUID.randomUUID();
        LogReplicationEntryMsg straggler =
                msg(cancelledAttemptSyncId, LogReplicationEntryType.SNAPSHOT_MESSAGE, 10);
        LogReplicationEntryMsg strayAck = buffer.processMsgAndBuffer(straggler);
        Assert.assertNull("the straggler must be rejected outright, not buffered", strayAck);

        // Legitimately advance the *active* attempt through and past seqNum 9/10.
        buffer.processMsgAndBuffer(msg(activeSyncId, LogReplicationEntryType.SNAPSHOT_MESSAGE, 9));
        buffer.processMsgAndBuffer(msg(activeSyncId, LogReplicationEntryType.SNAPSHOT_MESSAGE, 10));

        Assert.assertEquals(10, buffer.lastProcessedSeq);
        ArgumentCaptor<LogReplicationEntryMsg> applied = ArgumentCaptor.forClass(LogReplicationEntryMsg.class);
        verify(sinkManager, times(11)).processMessage(applied.capture());
        for (LogReplicationEntryMsg appliedMsg : applied.getAllValues()) {
            Assert.assertEquals("no message from the cancelled attempt must ever reach processMessage()",
                    activeSyncId, getUUID(appliedMsg.getMetadata().getSyncRequestId()));
        }
    }

    @Test
    public void snapshotEndTransitionsAckToTransferComplete() {
        LogReplicationEntryMsg end = msg(activeSyncId, LogReplicationEntryType.SNAPSHOT_END, 0);

        LogReplicationEntryMsg ack = buffer.processMsgAndBuffer(end);

        Assert.assertNotNull(ack);
        Assert.assertEquals(LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE, ack.getMetadata().getEntryType());
    }

    @Test
    public void ackAlwaysStatesExpectedSeqNum() {
        // expectedSeqNum must be set on every ack (not just when a gap is detected) -- see its
        // Javadoc in the .proto for why the source depends on that.
        LogReplicationEntryMsg m = msg(activeSyncId, LogReplicationEntryType.SNAPSHOT_MESSAGE, 0);

        LogReplicationEntryMsg ack = buffer.processMsgAndBuffer(m);

        Assert.assertNotNull(ack);
        Assert.assertTrue(ack.getMetadata().hasExpectedSeqNum());
        Assert.assertEquals(1L, ack.getMetadata().getExpectedSeqNum());
    }

    @Test
    public void bufferedOutOfOrderMessageIsAppliedOnceTheGapCloses() {
        // seqNum 1 arrives before 0: not in order yet (preTs=0 > lastProcessedSeq=-1), gets
        // buffered rather than dropped, since it's within maxSize and from the active attempt.
        LogReplicationEntryMsg second = msg(activeSyncId, LogReplicationEntryType.SNAPSHOT_MESSAGE, 1);
        buffer.processMsgAndBuffer(second);
        verify(sinkManager, never()).processMessage(any());

        LogReplicationEntryMsg first = msg(activeSyncId, LogReplicationEntryType.SNAPSHOT_MESSAGE, 0);
        buffer.processMsgAndBuffer(first);

        // Both messages should now have been applied, in order, via processBuffer().
        List<Long> order = List.of(0L, 1L);
        ArgumentCaptor<LogReplicationEntryMsg> applied = ArgumentCaptor.forClass(LogReplicationEntryMsg.class);
        verify(sinkManager, times(2)).processMessage(applied.capture());
        for (int i = 0; i < applied.getAllValues().size(); i++) {
            Assert.assertEquals((long) order.get(i),
                    applied.getAllValues().get(i).getMetadata().getSnapshotSyncSeqNum());
        }
        Assert.assertEquals(1, buffer.lastProcessedSeq);
    }
}
