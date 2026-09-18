package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationAckReader;
import org.corfudb.infrastructure.logreplication.replication.send.LogReplicationEventMetadata;
import org.corfudb.infrastructure.logreplication.replication.send.SenderBufferManager;
import org.corfudb.infrastructure.logreplication.replication.send.SenderPendingMessageQueue;
import org.corfudb.infrastructure.logreplication.replication.send.SnapshotSender;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The source does not pace its own restarts: the sink does, by not admitting a new attempt until it
 * has cleaned up the previous one and its checkpointer has caught up. What this state still owns is
 * verified here: a restart is applied on the worker behind whatever the cancelled task was doing,
 * the run of cancellations is reported, and events of another attempt are ignored.
 */
@Slf4j
public class InSnapshotSyncStateTest {

    private SnapshotSender snapshotSender;
    private InSnapshotSyncState state;
    private ExecutorService workers;
    private LogReplicationAckReader ackReader;

    @Before
    public void setup() {
        LogReplicationFSM fsm = mock(LogReplicationFSM.class);
        snapshotSender = mock(SnapshotSender.class);
        ackReader = mock(LogReplicationAckReader.class);
        SenderBufferManager bufferManager = mock(SenderBufferManager.class);
        SenderPendingMessageQueue pendingMessages = new SenderPendingMessageQueue(10);

        when(fsm.getAckReader()).thenReturn(ackReader);
        when(fsm.isValidTransition(any(), any())).thenReturn(true);
        when(snapshotSender.getDataSenderBufferManager()).thenReturn(bufferManager);
        when(bufferManager.getPendingMessages()).thenReturn(pendingMessages);
        when(snapshotSender.getStopSnapshotSync()).thenReturn(new AtomicBoolean(false));

        workers = Executors.newSingleThreadExecutor();
        when(fsm.getLogReplicationFSMWorkers()).thenReturn(workers);

        state = new InSnapshotSyncState(fsm, snapshotSender);
        when(fsm.getStates()).thenReturn(Collections.singletonMap(LogReplicationStateType.IN_SNAPSHOT_SYNC, state));

        // Establish transmitFuture (read by cancelSnapshotSync) via a normal entry.
        LogReplicationState initialized = mock(LogReplicationState.class);
        when(initialized.getType()).thenReturn(LogReplicationStateType.INITIALIZED);
        state.setTransitionSyncId(UUID.randomUUID());
        state.onEntry(initialized);
        // Let the (mocked, effectively instant) transmit() call settle before driving events.
        verify(snapshotSender, timeout(2000)).transmit(any(), anyBoolean());
        verify(snapshotSender, times(1)).reset();
    }

    @After
    public void tearDown() throws IllegalTransitionException {
        state.processEvent(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.REPLICATION_STOP,
                new LogReplicationEventMetadata(state.getTransitionSyncId())));
        workers.shutdownNow();
    }

    private LogReplicationEvent cancelEvent() {
        return new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL,
                new LogReplicationEventMetadata(state.getTransitionSyncId()));
    }

    @Test
    public void aRestartIsAppliedOnceOnTheWorkerBehindTheCancelledTask() throws Exception {
        CountDownLatch occupied = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        workers.submit(() -> { occupied.countDown(); release.await(); return null; });
        Assert.assertTrue(occupied.await(2, TimeUnit.SECONDS));
        clearInvocations(snapshotSender);

        state.processEvent(cancelEvent());
        state.onEntry(state);
        state.onEntry(state); // A superseded queued continuation must not consume the pending reset.
        verify(snapshotSender, after(100).never()).transmit(any(), anyBoolean());

        release.countDown();
        verify(snapshotSender, timeout(2000)).transmit(any(), anyBoolean());
        verify(snapshotSender, times(1)).reset();
    }

    @Test
    public void aRestartIsNotDelayedByTheSource() throws Exception {
        long start = System.nanoTime();
        state.processEvent(cancelEvent());
        state.onEntry(state);
        verify(snapshotSender, timeout(1000).times(2)).transmit(any(), anyBoolean());
        Assert.assertTrue("the sink paces admission, the source does not back off",
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 1000);
        verify(snapshotSender, times(2)).reset();
    }

    @Test
    public void aContinuationDoesNotResetTheSender() throws Exception {
        state.onEntry(state);
        verify(snapshotSender, timeout(1000).times(2)).transmit(any(), anyBoolean());
        verify(snapshotSender, times(1)).reset();
    }

    @Test
    public void consecutiveCancellationsAreSurfacedToTheReplicationStatusTable() throws IllegalTransitionException {
        // status alone stays ONGOING throughout a restart loop, indistinguishable from one long
        // healthy transfer -- consecutiveFailures on SnapshotSyncInfo is what lets an external caller
        // (dashboard, alerting) tell the two apart.
        state.processEvent(cancelEvent());
        verify(ackReader).markSnapshotSyncInfoOngoing(anyBoolean(), any(), eq(1));

        state.processEvent(cancelEvent());
        verify(ackReader).markSnapshotSyncInfoOngoing(anyBoolean(), any(), eq(2));
        Assert.assertEquals(2, state.consecutiveCancellations);
    }

    @Test
    public void aFreshExternalRequestStartsANewRunOfAttempts() throws IllegalTransitionException {
        state.processEvent(cancelEvent());
        state.processEvent(cancelEvent());
        Assert.assertEquals(2, state.consecutiveCancellations);

        UUID requested = UUID.randomUUID();
        state.processEvent(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST,
                new LogReplicationEventMetadata(requested)));

        Assert.assertEquals(0, state.consecutiveCancellations);
        Assert.assertEquals(requested, state.getTransitionSyncId());
        verify(ackReader).markSnapshotSyncInfoOngoing(anyBoolean(), eq(requested), eq(0));
        verify(snapshotSender, times(3)).stop();
    }

    @Test
    public void stopAndShutdownEndTheRunOfAttempts() throws IllegalTransitionException {
        state.processEvent(cancelEvent());
        state.processEvent(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.REPLICATION_STOP,
                new LogReplicationEventMetadata(state.getTransitionSyncId())));
        Assert.assertEquals("a stop is a clean boundary; a later, unrelated session must not inherit this count",
                0, state.consecutiveCancellations);

        state.processEvent(cancelEvent());
        state.processEvent(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.REPLICATION_SHUTDOWN,
                new LogReplicationEventMetadata(state.getTransitionSyncId())));
        Assert.assertEquals(0, state.consecutiveCancellations);
    }

    @Test
    public void theCounterSaturatesInsteadOfOverflowing() {
        state.consecutiveCancellations = Integer.MAX_VALUE - 1;
        state.registerCancellation();
        state.registerCancellation();
        Assert.assertEquals(Integer.MAX_VALUE, state.consecutiveCancellations);
    }

    @Test
    public void anEventOfAnotherAttemptIsIgnored() throws IllegalTransitionException {
        // The sender's current attempt identity is null on this mock, so any bound identity differs.
        clearInvocations(snapshotSender, ackReader);
        LogReplicationEvent stale = new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL,
                new LogReplicationEventMetadata(state.getTransitionSyncId()).setSnapshotAttempt(UUID.randomUUID(), 4));

        Assert.assertSame(state, state.processEvent(stale));

        Assert.assertEquals(0, state.consecutiveCancellations);
        verify(snapshotSender, never()).stop();
        verify(ackReader, never()).markSnapshotSyncInfoOngoing(anyBoolean(), any(), anyInt());
    }

    @Test
    public void stopCancelsTheQueuedTransmitWithoutOccupyingTheWorker() throws Exception {
        CountDownLatch occupied = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        workers.submit(() -> { occupied.countDown(); release.await(); return null; });
        Assert.assertTrue(occupied.await(2, TimeUnit.SECONDS));
        clearInvocations(snapshotSender);

        state.onEntry(state);
        state.processEvent(new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.REPLICATION_STOP,
                new LogReplicationEventMetadata(state.getTransitionSyncId())));
        release.countDown();
        workers.submit(() -> { }).get(2, TimeUnit.SECONDS);

        verify(snapshotSender, never()).transmit(any(), anyBoolean());
        verify(snapshotSender).stop();
    }
}
