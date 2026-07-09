package org.corfudb.infrastructure.logreplication.replication.send;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationAckReader;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.ReadProcessor;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReader;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Verifies that {@link SnapshotSender} tolerates a slow/busy sink by retrying the outstanding
 * snapshot-sync ack wait instead of canceling on the very first timeout (fix for the ack-timeout
 * livelock: repeated single-timeout cancellations never gave the sink a window to catch up).
 *
 * These tests reach the package-private {@code waitForSnapshotSyncAck} method and the
 * package-private {@code maxSnapshotAckWaitRetries}/{@code snapshotSyncAck} fields directly
 * (they are package-private, not private, specifically to allow this) to exercise the retry
 * logic in isolation, without needing to drive it through the full read/send loop or a real
 * Corfu server.
 */
@Slf4j
public class SnapshotSenderTest {

    private SnapshotSender snapshotSender;
    private ScheduledExecutorService delayedCompletionExecutor;

    @Before
    public void setup() {
        CorfuRuntime runtime = mock(CorfuRuntime.class);
        SnapshotReader snapshotReader = mock(SnapshotReader.class);
        DataSender dataSender = mock(DataSender.class);
        ReadProcessor readProcessor = mock(ReadProcessor.class);
        LogReplicationFSM fsm = mock(LogReplicationFSM.class);
        LogReplicationAckReader ackReader = mock(LogReplicationAckReader.class);
        when(fsm.getAckReader()).thenReturn(ackReader);

        snapshotSender = new SnapshotSender(runtime, snapshotReader, dataSender, readProcessor, 10, fsm);
        delayedCompletionExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void tearDown() {
        delayedCompletionExecutor.shutdownNow();
    }

    @Test
    public void survivesAckArrivingAfterOneTimeoutInsteadOfCancelingImmediately() throws Exception {
        // 2 retries => up to 2 * DEFAULT_TIMEOUT_MS (10s) of patience. The ack is completed after
        // one timeout has already elapsed (~5.5s in), which the old (no-retry) behavior would have
        // treated as an outright failure -- canceling and restarting the whole snapshot sync --
        // but which the fix should absorb transparently.
        snapshotSender.maxSnapshotAckWaitRetries = 2;
        CompletableFuture<LogReplicationEntryMsg> ack = new CompletableFuture<>();
        snapshotSender.snapshotSyncAck = ack;

        LogReplicationEntryMsg expectedAck = LogReplicationEntryMsg.newBuilder().build();
        delayedCompletionExecutor.schedule(() -> ack.complete(expectedAck), 5500, TimeUnit.MILLISECONDS);

        long start = System.currentTimeMillis();
        LogReplicationEntryMsg result = snapshotSender.waitForSnapshotSyncAck(UUID.randomUUID());
        long elapsed = System.currentTimeMillis() - start;

        Assert.assertEquals(expectedAck, result);
        Assert.assertTrue("expected to wait through at least one 5s timeout, elapsed=" + elapsed,
                elapsed >= 5000);
    }

    @Test
    public void cancelsOnlyAfterExhaustingAllRetries() throws Exception {
        // 2 retries, ack never arrives => should take ~2 * DEFAULT_TIMEOUT_MS (10s) and then throw,
        // instead of throwing after the first 5s timeout.
        snapshotSender.maxSnapshotAckWaitRetries = 2;
        snapshotSender.snapshotSyncAck = new CompletableFuture<>();

        long start = System.currentTimeMillis();
        try {
            snapshotSender.waitForSnapshotSyncAck(UUID.randomUUID());
            Assert.fail("Expected a TimeoutException after exhausting retries");
        } catch (TimeoutException expected) {
            long elapsed = System.currentTimeMillis() - start;
            Assert.assertTrue("expected to exhaust 2 retries (~10s), elapsed=" + elapsed, elapsed >= 9000);
        }
    }
}
