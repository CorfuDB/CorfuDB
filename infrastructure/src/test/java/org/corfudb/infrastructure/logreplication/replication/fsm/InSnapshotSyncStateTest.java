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
 * cancel-and-immediate-retry cycles gave a struggling sink no window to catch up (e.g. to resume
 * local checkpointing paused for the duration of an active snapshot sync). InSnapshotSyncState
 * now increases the delay between a SYNC_CANCEL and the next retry attempt, and resets it on a
 * fresh externally-requested sync.
 */
@Slf4j
public class InSnapshotSyncStateTest {

    private SnapshotSender snapshotSender;
    private InSnapshotSyncState state;
    private ExecutorService workers;

    @Before
    public void setup() {
        LogReplicationFSM fsm = mock(LogReplicationFSM.class);
        snapshotSender = mock(SnapshotSender.class);
        LogReplicationAckReader ackReader = mock(LogReplicationAckReader.class);
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
        long expected = InSnapshotSyncState.INITIAL_RETRY_BACKOFF_MS;
        for (int i = 0; i < 8; i++) {
            state.processEvent(cancelEvent());
            Assert.assertEquals("unexpected backoff after " + (i + 1) + " consecutive cancellations",
                    expected, state.retryBackoffMs);
            expected = Math.min(expected * 2, InSnapshotSyncState.MAX_RETRY_BACKOFF_MS);
        }
        Assert.assertEquals(InSnapshotSyncState.MAX_RETRY_BACKOFF_MS, state.retryBackoffMs);
    }

    @Test
    public void backoffResetsOnFreshExternalRequest() throws IllegalTransitionException {
        state.processEvent(cancelEvent());
        state.processEvent(cancelEvent());
        Assert.assertTrue(state.retryBackoffMs > 0);
        Assert.assertTrue(state.consecutiveCancellations > 0);

        // Simulate that the current attempt has been running for a while (as it would in
        // production, where onEntry() re-stamps lastEntryTimeMs on every real transition) so this
        // genuinely reads as a fresh, externally requested sync rather than part of the same
        // restart storm -- see backoffAppliesWhenRequestArrivesRightAfterEntry for the other case.
        state.lastEntryTimeMs = System.currentTimeMillis() - InSnapshotSyncState.MIN_ATTEMPT_AGE_FOR_UNTHROTTLED_RESTART_MS;

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
        // arriving now looks like part of a restart storm (e.g. a caller repeatedly invoking
        // enforceSnapshotSync()) rather than a deliberate new request, and should back off instead
        // of restarting unthrottled.
        LogReplicationEvent request = new LogReplicationEvent(
                LogReplicationEvent.LogReplicationEventType.SNAPSHOT_SYNC_REQUEST,
                new LogReplicationEventMetadata(UUID.randomUUID()));
        state.processEvent(request);

        Assert.assertTrue("expected backoff to apply instead of resetting to zero", state.retryBackoffMs > 0);
        Assert.assertEquals(1, state.consecutiveCancellations);
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
        state.retryBackoffMs = InSnapshotSyncState.MAX_RETRY_BACKOFF_MS; // 60s, would time out the test if not interrupted

        long start = System.currentTimeMillis();
        state.onEntry(state);
        // Flip the stop flag shortly after entry starts backing off (on a thread of its own --
        // `workers` is single-threaded and is already busy running the backoff wait itself, so
        // queuing this on `workers` would never get a turn to run).
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
