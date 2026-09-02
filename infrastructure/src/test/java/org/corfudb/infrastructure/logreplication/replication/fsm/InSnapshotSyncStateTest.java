package org.corfudb.infrastructure.logreplication.replication.fsm;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the backoff introduced to break the snapshot-sync ack-timeout livelock: repeated
 * cancel-and-immediate-retry cycles gave a struggling sink no window to catch up. InSnapshotSyncState
 * increases the delay between a cancellation and the next retry attempt, and resets it on a fresh
 * externally-requested sync or a clean stop/shutdown boundary.
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
        when(fsm.isValidTransition(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any())).thenReturn(true);
        when(snapshotSender.getDataSenderBufferManager()).thenReturn(bufferManager);
        when(bufferManager.getPendingMessages()).thenReturn(pendingMessages);
        when(snapshotSender.getStopSnapshotSync()).thenReturn(new AtomicBoolean(false));

        workers = Executors.newSingleThreadExecutor();
        when(fsm.getLogReplicationFSMWorkers()).thenReturn(workers);

        state = new InSnapshotSyncState(fsm, snapshotSender);
        when(fsm.getStates()).thenReturn(Collections.singletonMap(LogReplicationStateType.IN_SNAPSHOT_SYNC, state));

        // Establish transmitFuture (read by cancelSnapshotSync) via a normal, non-backoff entry.
        LogReplicationState initialized = mock(LogReplicationState.class);
        when(initialized.getType()).thenReturn(LogReplicationStateType.INITIALIZED);
        state.setTransitionSyncId(UUID.randomUUID());
        state.onEntry(initialized);
        // Let the (mocked, effectively instant) transmit() call settle before driving events.
        verify(snapshotSender, timeout(2000)).transmit(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyBoolean());
    }

    @After
    public void tearDown() {
        workers.shutdownNow();
    }

    private LogReplicationEvent cancelEvent() {
        return new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.SYNC_CANCEL,
                new LogReplicationEventMetadata(state.getTransitionSyncId()));
    }

    @Test
    public void backoffGrowsExponentiallyAndCapsAtMax() throws IllegalTransitionException {
        long expected = LogReplicationConfig.INITIAL_RETRY_BACKOFF_MS;
        for (int i = 0; i < 8; i++) {
            state.processEvent(cancelEvent());
            Assert.assertEquals("unexpected backoff after " + (i + 1) + " consecutive cancellations",
                    expected, state.retryBackoffMs);
            expected = Math.min(expected * 2, LogReplicationConfig.MAX_RETRY_BACKOFF_MS);
        }
        Assert.assertEquals(LogReplicationConfig.MAX_RETRY_BACKOFF_MS, state.retryBackoffMs);
    }

    @Test
    public void minBackoffFloorOverridesNormalBackoffAndIsNotCappedByMaxRetryBackoff() throws IllegalTransitionException {
        // checkpointerGracePeriodMs (see LogReplicationSinkManager) can legitimately exceed
        // MAX_RETRY_BACKOFF_MS for a large-dataset deployment -- the floor exists for a different
        // purpose (checkpointer breathing room) than the general restart-storm throttling
        // MAX_RETRY_BACKOFF_MS is tuned for, so it deliberately is not capped by it.
        long largeFloor = LogReplicationConfig.MAX_RETRY_BACKOFF_MS * 10;

        state.registerCancellationAndComputeBackoff(largeFloor);

        Assert.assertEquals(largeFloor, state.retryBackoffMs);
        Assert.assertEquals(1, state.consecutiveCancellations);
    }

    @Test
    public void minBackoffFloorIsANoOpWhenSmallerThanTheNormalComputedBackoff() throws IllegalTransitionException {
        state.registerCancellationAndComputeBackoff(1);
        state.registerCancellationAndComputeBackoff(1);

        // Normal doubling (INITIAL_RETRY_BACKOFF_MS * 2) already exceeds the floor of 1, which must
        // not override or shrink it.
        Assert.assertEquals(LogReplicationConfig.INITIAL_RETRY_BACKOFF_MS * 2, state.retryBackoffMs);
    }

    @Test
    public void zeroMinBackoffBehavesExactlyLikeTheNoArgOverload() throws IllegalTransitionException {
        state.registerCancellationAndComputeBackoff(0);

        Assert.assertEquals(LogReplicationConfig.INITIAL_RETRY_BACKOFF_MS, state.retryBackoffMs);
        Assert.assertEquals(1, state.consecutiveCancellations);
    }

    @Test
    public void consecutiveCancellationsIsSurfacedToTheReplicationStatusTable() throws IllegalTransitionException {
        // status alone stays ONGOING throughout a restart loop, indistinguishable from one long
        // healthy transfer -- consecutiveFailures on SnapshotSyncInfo is what actually lets an
        // external caller (dashboard, alerting) tell the two apart. Verifies InSnapshotSyncState
        // passes its own up-to-date counter on every markSnapshotSyncInfoOngoing() call site, not
        // just some of them.
        state.processEvent(cancelEvent());
        verify(ackReader).markSnapshotSyncInfoOngoing(org.mockito.ArgumentMatchers.anyBoolean(),
                org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq(1));

        state.processEvent(cancelEvent());
        verify(ackReader).markSnapshotSyncInfoOngoing(org.mockito.ArgumentMatchers.anyBoolean(),
                org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq(2));
    }

    @Test
    public void backoffResetsOnFreshExternalRequest() throws IllegalTransitionException {
        state.processEvent(cancelEvent());
        state.processEvent(cancelEvent());
        Assert.assertTrue(state.retryBackoffMs > 0);
        Assert.assertTrue(state.consecutiveCancellations > 0);

        // Simulate that the current attempt has been running for a while (as it would in production,
        // where onEntry() re-stamps lastEntryTimeMs on every genuine transition) so this genuinely
        // reads as a fresh, externally requested sync rather than part of the same restart storm.
        state.lastEntryTimeMs = System.currentTimeMillis() - InSnapshotSyncState.MIN_ATTEMPT_AGE_FOR_UNTHROTTLED_RESTART_MS - 1;

        LogReplicationEvent freshRequest = new LogReplicationEvent(
                LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST,
                new LogReplicationEventMetadata(UUID.randomUUID()));
        state.processEvent(freshRequest);

        Assert.assertEquals(0, state.retryBackoffMs);
        Assert.assertEquals(0, state.consecutiveCancellations);
    }

    @Test
    public void backoffAppliesWhenRequestArrivesRightAfterEntry() throws IllegalTransitionException {
        // setup() stamped lastEntryTimeMs via onEntry() moments ago, so a SNAPSHOT_SYNC_REQUEST
        // arriving now looks like part of a restart storm rather than a deliberate new request, and
        // should back off instead of restarting unthrottled.
        LogReplicationEvent request = new LogReplicationEvent(
                LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST,
                new LogReplicationEventMetadata(UUID.randomUUID()));
        state.processEvent(request);

        Assert.assertTrue("expected backoff to apply instead of resetting to zero", state.retryBackoffMs > 0);
        Assert.assertEquals(1, state.consecutiveCancellations);
    }

    @Test
    public void selfLoopEntryDoesNotRestampLastEntryTime() {
        // setup() already stamped lastEntryTimeMs via a genuine (from != this) entry.
        long stampedAtSetup = state.lastEntryTimeMs;
        // Simulates the SNAPSHOT_SYNC_CONTINUE self-loop, which re-enters via onEntry(this) every
        // ~maxNumSnapshotMsgPerBatch messages during an active, healthy transfer.
        state.onEntry(state);
        Assert.assertEquals("self-loop re-entry must not re-stamp lastEntryTimeMs, or an active " +
                        "transfer would always look like it just started",
                stampedAtSetup, state.lastEntryTimeMs);
    }

    @Test
    public void replicationStopResetsBackoff() throws IllegalTransitionException {
        state.processEvent(cancelEvent());
        state.processEvent(cancelEvent());
        Assert.assertTrue(state.consecutiveCancellations > 0);
        Assert.assertTrue(state.retryBackoffMs > 0);

        LogReplicationEvent stop = new LogReplicationEvent(LogReplicationEvent.LogReplicationEventType.REPLICATION_STOP,
                new LogReplicationEventMetadata(state.getTransitionSyncId()));
        state.processEvent(stop);

        Assert.assertEquals("a stop is a clean boundary; a later, unrelated session must not inherit this backoff",
                0, state.consecutiveCancellations);
        Assert.assertEquals(0, state.retryBackoffMs);
    }

    @Test
    public void onEntryDelaysTransmitByThePendingBackoff() {
        state.retryBackoffMs = 500;
        LogReplicationState self = state;
        long start = System.currentTimeMillis();

        state.onEntry(self); // from == this: re-entry after a cancellation, consumes the backoff

        verify(snapshotSender, timeout(3000).times(2))
                .transmit(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyBoolean());
        long elapsed = System.currentTimeMillis() - start;
        Assert.assertTrue("expected onEntry to delay by ~500ms before transmitting, elapsed=" + elapsed,
                elapsed >= 450);
    }

    @Test
    public void backoffWaitIsInterruptedByStop() throws InterruptedException {
        AtomicBoolean stop = new AtomicBoolean(false);
        when(snapshotSender.getStopSnapshotSync()).thenReturn(stop);
        state.retryBackoffMs = LogReplicationConfig.MAX_RETRY_BACKOFF_MS; // 60s, would time out the test if not interrupted

        long start = System.currentTimeMillis();
        state.onEntry(state);
        // Flip the stop flag shortly after entry starts backing off, on a thread of its own -- workers
        // is single-threaded and already busy running the backoff wait itself.
        Thread stopSetter = new Thread(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(300);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            stop.set(true);
        });
        stopSetter.start();

        verify(snapshotSender, timeout(3000).times(2))
                .transmit(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyBoolean());
        long elapsed = System.currentTimeMillis() - start;
        Assert.assertTrue("expected the 60s backoff to be cut short by stopSnapshotSync, elapsed=" + elapsed,
                elapsed < 3000);
        stopSetter.join();
    }
}
