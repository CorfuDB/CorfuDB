package org.corfudb.infrastructure.logreplication.replication.send;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.logreplication.DataSender;
import org.corfudb.infrastructure.logreplication.replication.LogReplicationAckReader;
import org.corfudb.infrastructure.logreplication.replication.fsm.LogReplicationFSM;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.ReadProcessor;
import org.corfudb.infrastructure.logreplication.replication.send.logreader.SnapshotReader;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.LogReplication.LogReplicationMetadataResponseMsg;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.DEFAULT_TIMEOUT_MS;
import static org.corfudb.infrastructure.logreplication.LogReplicationConfig.SNAPSHOT_SYNC_ACK_MAX_RETRIES;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Verifies the busy-signal-aware ack wait introduced to break the snapshot-sync ack-timeout
 * livelock: a source that receives no ack for the final snapshot-sync message no longer gives up
 * immediately -- it polls the sink directly, and only treats the sink as stalled once it has been
 * silent (no ack, and no "still processing" response to the poll) for
 * SNAPSHOT_SYNC_ACK_MAX_RETRIES consecutive attempts.
 */
@Slf4j
public class SnapshotSenderTest {

    private DataSender dataSender;
    private SnapshotSender snapshotSender;

    @Before
    public void setup() {
        LogReplicationFSM fsm = mock(LogReplicationFSM.class);
        LogReplicationAckReader ackReader = mock(LogReplicationAckReader.class);
        when(fsm.getAckReader()).thenReturn(ackReader);

        dataSender = mock(DataSender.class);
        CorfuRuntime runtime = mock(CorfuRuntime.class);
        SnapshotReader snapshotReader = mock(SnapshotReader.class);
        ReadProcessor readProcessor = mock(ReadProcessor.class);

        snapshotSender = new SnapshotSender(runtime, snapshotReader, dataSender, readProcessor, 5, fsm);
    }

    private LogReplicationEntryMsg transferCompleteAck() {
        LogReplicationEntryMetadataMsg metadata = LogReplicationEntryMetadataMsg.newBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE)
                .build();
        return LogReplicationEntryMsg.newBuilder().setMetadata(metadata).build();
    }

    private LogReplicationMetadataResponseMsg metadataResponse(boolean isProcessing) {
        return LogReplicationMetadataResponseMsg.newBuilder().setIsProcessing(isProcessing).build();
    }

    private LogReplicationMetadataResponseMsg metadataResponseForAttempt(boolean isProcessing,
                                                                          long processingSnapshotTimestamp) {
        return LogReplicationMetadataResponseMsg.newBuilder()
                .setIsProcessing(isProcessing)
                .setProcessingSnapshotTimestamp(processingSnapshotTimestamp)
                .build();
    }

    private LogReplicationEntryMsg snapshotDataMessage() {
        LogReplicationEntryMetadataMsg metadata = LogReplicationEntryMetadataMsg.newBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_MESSAGE)
                .build();
        return LogReplicationEntryMsg.newBuilder().setMetadata(metadata).build();
    }

    private LogReplicationEntryMsg ackMessage(long snapshotSyncSeqNum, long expectedSeqNum) {
        LogReplicationEntryMetadataMsg metadata = LogReplicationEntryMetadataMsg.newBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_REPLICATED)
                .setSnapshotSyncSeqNum(snapshotSyncSeqNum)
                .setExpectedSeqNum(expectedSeqNum)
                .build();
        return LogReplicationEntryMsg.newBuilder().setMetadata(metadata).build();
    }

    private LogReplicationEntryMsg ackMessageWithoutExpectedSeqNum(long snapshotSyncSeqNum) {
        // Simulates an old peer whose acks never populate expectedSeqNum at all.
        LogReplicationEntryMetadataMsg metadata = LogReplicationEntryMetadataMsg.newBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_REPLICATED)
                .setSnapshotSyncSeqNum(snapshotSyncSeqNum)
                .build();
        return LogReplicationEntryMsg.newBuilder().setMetadata(metadata).build();
    }

    /**
     * Puts one entry in the pending (unacked) buffer and backdates its "last ack advanced" clock,
     * simulating a batch that's been sitting unacked for the given duration -- without an actual
     * real-time wait.
     */
    private void makeBufferStalledFor(long stalledForMs) {
        snapshotSender.getDataSenderBufferManager().sendWithBuffering(snapshotDataMessage());
        snapshotSender.getDataSenderBufferManager().lastAckAdvancedTimeMs = System.currentTimeMillis() - stalledForMs;
    }

    @Test
    public void returnsImmediatelyWithoutPollingWhenAckArrivesInTime() throws Exception {
        CompletableFuture<LogReplicationEntryMsg> ackFuture = CompletableFuture.completedFuture(transferCompleteAck());

        LogReplicationEntryMsg ack = snapshotSender.waitForSnapshotSyncAck(UUID.randomUUID(), ackFuture);

        Assert.assertEquals(LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE, ack.getMetadata().getEntryType());
        verify(dataSender, times(0)).sendMetadataRequest();
    }

    @Test
    public void busySinkExtendsWaitInsteadOfFailing() throws Exception {
        // The real ack never arrives on its own within a single DEFAULT_TIMEOUT_MS window; only the
        // busy poll tells the source the sink is still alive and working.
        CompletableFuture<LogReplicationEntryMsg> ackFuture = new CompletableFuture<>();
        when(dataSender.sendMetadataRequest())
                .thenReturn(CompletableFuture.completedFuture(metadataResponse(true)));

        // Resolve the real ack shortly after the first ack-wait window would have elapsed, from a
        // separate thread, proving the wait was extended past one DEFAULT_TIMEOUT_MS instead of the
        // caller giving up on the first timeout.
        ScheduledExecutorService delayedCompletion = Executors.newSingleThreadScheduledExecutor();
        try {
            delayedCompletion.schedule(() -> ackFuture.complete(transferCompleteAck()),
                    DEFAULT_TIMEOUT_MS + 500, TimeUnit.MILLISECONDS);

            LogReplicationEntryMsg ack = snapshotSender.waitForSnapshotSyncAck(UUID.randomUUID(), ackFuture);

            Assert.assertEquals(LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE, ack.getMetadata().getEntryType());
            verify(dataSender, times(1)).sendMetadataRequest();
        } finally {
            delayedCompletion.shutdownNow();
        }
    }

    @Test
    public void singleGenuineTimeoutDoesNotFailImmediately() throws Exception {
        // Sink reports no activity at all on the poll, but SNAPSHOT_SYNC_ACK_MAX_RETRIES (6) > 1, so
        // a single silent timeout must not be treated as exhausted yet: the caller keeps waiting and
        // eventually gets the real ack instead of failing after just one timeout.
        CompletableFuture<LogReplicationEntryMsg> ackFuture = new CompletableFuture<>();
        when(dataSender.sendMetadataRequest())
                .thenReturn(CompletableFuture.completedFuture(metadataResponse(false)));

        ScheduledExecutorService delayedCompletion = Executors.newSingleThreadScheduledExecutor();
        try {
            delayedCompletion.schedule(() -> ackFuture.complete(transferCompleteAck()),
                    DEFAULT_TIMEOUT_MS + 500, TimeUnit.MILLISECONDS);

            LogReplicationEntryMsg ack = snapshotSender.waitForSnapshotSyncAck(UUID.randomUUID(), ackFuture);

            Assert.assertEquals(LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE, ack.getMetadata().getEntryType());
        } finally {
            delayedCompletion.shutdownNow();
        }
    }

    // ------------------------------------------------------------------------------------------
    // checkForGenuineMidTransferStall(): covers the mid-transfer livelock gap the above tests don't
    // -- a sink that stops acking BEFORE the source finishes reading all data never reaches
    // waitForSnapshotSyncAck at all (snapshotCompleted never becomes true), so without this check
    // SenderBufferManager.resend() (which has no retry cap of its own) would retry forever with no
    // path to cancellation.
    // ------------------------------------------------------------------------------------------

    @Test
    public void noStallWhenNothingPending() {
        boolean canceled = snapshotSender.checkForGenuineMidTransferStall(UUID.randomUUID(), false);

        Assert.assertFalse(canceled);
        verify(dataSender, times(0)).sendMetadataRequest();
    }

    @Test
    public void noStallWhenAckHasRecentlyAdvanced() {
        // Buffer has a pending entry, but it's only been unacked for a fraction of the bound.
        makeBufferStalledFor(1000);

        boolean canceled = snapshotSender.checkForGenuineMidTransferStall(UUID.randomUUID(), false);

        Assert.assertFalse(canceled);
        Assert.assertEquals(0, snapshotSender.consecutiveGenuineStallChecks);
        verify(dataSender, times(0)).sendMetadataRequest();
    }

    @Test
    public void busySinkDuringMidTransferStallExtendsPatienceInsteadOfCanceling() {
        makeBufferStalledFor(SNAPSHOT_SYNC_ACK_MAX_RETRIES * DEFAULT_TIMEOUT_MS + 1000);
        when(dataSender.sendMetadataRequest())
                .thenReturn(CompletableFuture.completedFuture(metadataResponse(true)));

        boolean canceled = snapshotSender.checkForGenuineMidTransferStall(UUID.randomUUID(), false);

        Assert.assertFalse(canceled);
        Assert.assertEquals(0, snapshotSender.consecutiveGenuineStallChecks);
        verify(dataSender, times(1)).sendMetadataRequest();
    }

    @Test
    public void singleGenuineMidTransferStallDoesNotCancelImmediately() {
        makeBufferStalledFor(SNAPSHOT_SYNC_ACK_MAX_RETRIES * DEFAULT_TIMEOUT_MS + 1000);
        when(dataSender.sendMetadataRequest())
                .thenReturn(CompletableFuture.completedFuture(metadataResponse(false)));

        boolean canceled = snapshotSender.checkForGenuineMidTransferStall(UUID.randomUUID(), false);

        Assert.assertFalse("a single genuine stall check must not cancel yet (max retries > 1)", canceled);
        Assert.assertEquals(1, snapshotSender.consecutiveGenuineStallChecks);
    }

    @Test
    public void midTransferStallCancelsAfterMaxRetries() {
        makeBufferStalledFor(SNAPSHOT_SYNC_ACK_MAX_RETRIES * DEFAULT_TIMEOUT_MS + 1000);
        when(dataSender.sendMetadataRequest())
                .thenReturn(CompletableFuture.completedFuture(metadataResponse(false)));
        // Simulate the prior (SNAPSHOT_SYNC_ACK_MAX_RETRIES - 1) genuine checks having already
        // happened, and backdate the pacing clock so this call isn't throttled.
        snapshotSender.consecutiveGenuineStallChecks = SNAPSHOT_SYNC_ACK_MAX_RETRIES - 1;
        snapshotSender.lastStallCheckTimeMs = 0;

        boolean canceled = snapshotSender.checkForGenuineMidTransferStall(UUID.randomUUID(), false);

        Assert.assertTrue("expected cancellation once genuine stall checks reach the max", canceled);
        Assert.assertEquals(0, snapshotSender.consecutiveGenuineStallChecks);
        verify(dataSender, times(1)).onError(any());
    }

    @Test
    public void midTransferStallCheckIsPacedAndDoesNotHammerTheSink() {
        makeBufferStalledFor(SNAPSHOT_SYNC_ACK_MAX_RETRIES * DEFAULT_TIMEOUT_MS + 1000);
        when(dataSender.sendMetadataRequest())
                .thenReturn(CompletableFuture.completedFuture(metadataResponse(false)));

        // Two calls back-to-back, as a tight SNAPSHOT_SYNC_CONTINUE self-loop (pending buffer
        // permanently full) would produce with no other pacing in between.
        snapshotSender.checkForGenuineMidTransferStall(UUID.randomUUID(), false);
        snapshotSender.checkForGenuineMidTransferStall(UUID.randomUUID(), false);

        Assert.assertEquals("the second call landed within the pacing window and must not have " +
                        "incremented the counter again", 1, snapshotSender.consecutiveGenuineStallChecks);
        verify(dataSender, times(1)).sendMetadataRequest();
    }

    // ------------------------------------------------------------------------------------------
    // isSinkStillProcessing()'s attempt-scoping: isProcessing alone is sink-wide, not scoped to
    // the specific attempt this source is asking about (e.g. the sink could be busy resuming an
    // old, already-abandoned apply after its own restart while this source has since moved on to a
    // fresh attempt). Trusting it blindly would let a stale-attempt busy signal extend patience
    // for an attempt it says nothing about, indefinitely. A sink new enough to report
    // processingSnapshotTimestamp lets the source tell the two apart.
    // ------------------------------------------------------------------------------------------

    @Test
    public void busySinkOnADifferentAttemptDoesNotExtendPatienceForOurs() {
        makeBufferStalledFor(SNAPSHOT_SYNC_ACK_MAX_RETRIES * DEFAULT_TIMEOUT_MS + 1000);
        // Busy, but on some other attempt's baseSnapshotTimestamp -- e.g. still resuming an old,
        // already-abandoned apply. snapshotSender's own baseSnapshotTimestamp defaults to 0 here
        // (reset() was never called in this unit-test fixture), so 999 is guaranteed to differ.
        when(dataSender.sendMetadataRequest())
                .thenReturn(CompletableFuture.completedFuture(metadataResponseForAttempt(true, 999L)));
        // Simulate the prior (SNAPSHOT_SYNC_ACK_MAX_RETRIES - 1) genuine checks having already
        // happened, mirroring midTransferStallCancelsAfterMaxRetries() above.
        snapshotSender.consecutiveGenuineStallChecks = SNAPSHOT_SYNC_ACK_MAX_RETRIES - 1;
        snapshotSender.lastStallCheckTimeMs = 0;

        boolean canceled = snapshotSender.checkForGenuineMidTransferStall(UUID.randomUUID(), false);

        Assert.assertTrue("a busy signal for a different attempt must not be trusted as busy on ours",
                canceled);
    }

    @Test
    public void busySinkOnOurOwnAttemptStillExtendsPatience() {
        makeBufferStalledFor(SNAPSHOT_SYNC_ACK_MAX_RETRIES * DEFAULT_TIMEOUT_MS + 1000);
        when(dataSender.sendMetadataRequest()).thenReturn(CompletableFuture.completedFuture(
                metadataResponseForAttempt(true, snapshotSender.getBaseSnapshotTimestamp())));

        boolean canceled = snapshotSender.checkForGenuineMidTransferStall(UUID.randomUUID(), false);

        Assert.assertFalse(canceled);
        Assert.assertEquals(0, snapshotSender.consecutiveGenuineStallChecks);
    }

    @Test
    public void oldSinkWithoutProcessingTimestampIsStillTrustedAtFaceValue() {
        // Rolling-upgrade safety: an old sink's response never sets processingSnapshotTimestamp at
        // all (hasProcessingSnapshotTimestamp() == false) -- must fall back to trusting isProcessing
        // exactly as before this attempt-scoping existed, not be misread as "different attempt".
        makeBufferStalledFor(SNAPSHOT_SYNC_ACK_MAX_RETRIES * DEFAULT_TIMEOUT_MS + 1000);
        when(dataSender.sendMetadataRequest())
                .thenReturn(CompletableFuture.completedFuture(metadataResponse(true)));

        boolean canceled = snapshotSender.checkForGenuineMidTransferStall(UUID.randomUUID(), false);

        Assert.assertFalse(canceled);
    }

    // ------------------------------------------------------------------------------------------
    // expectedSeqNum-driven expedited resend: the sink can now explicitly confirm what it's still
    // waiting for, turning the source's blind, cadence-paced resend into a receiver-directed one.
    // ------------------------------------------------------------------------------------------

    @Test
    public void repeatedExpectedSeqNumAcrossAcksExpeditesResendOfPendingEntriesAtOrAfterIt() {
        // sendWithBuffering() overrides whatever snapshotSyncSeqNum is set on the message with its
        // own auto-incrementing counter (starting at Address.NON_ADDRESS = -1), so four calls here
        // produce entries with seq -1, 0, 1, 2 regardless of what snapshotDataMessage() set.
        SenderBufferManager bufferManager = snapshotSender.getDataSenderBufferManager();
        bufferManager.sendWithBuffering(snapshotDataMessage());
        bufferManager.sendWithBuffering(snapshotDataMessage());
        bufferManager.sendWithBuffering(snapshotDataMessage());
        bufferManager.sendWithBuffering(snapshotDataMessage());

        // Sink confirms it received up through seq 0 (evicting seq -1 and 0). expectedSeqNum is
        // always ack+1, so by itself this first ack carries no gap signal -- nothing should be
        // expedited yet.
        bufferManager.updateAck(ackMessage(0, 1));
        java.util.List<LogReplicationPendingEntry> pendingAfterFirstAck =
                bufferManager.getPendingMessages().getPendingEntries();
        for (LogReplicationPendingEntry entry : pendingAfterFirstAck) {
            Assert.assertFalse("entry seq=" + entry.getData().getMetadata().getSnapshotSyncSeqNum()
                    + " should not be expedited off a single ack", entry.isExpedited());
        }

        // The sink reports the exact same expectedSeqNum again on the next ack cycle, despite more
        // data having been sent in between -- it's genuinely stuck waiting for seq 1 (e.g. seq 1 was
        // lost and seq 2 arrived out of order and got buffered). That repetition is the actual gap
        // signal, and should now expedite everything still outstanding.
        bufferManager.updateAck(ackMessage(0, 1));

        java.util.List<LogReplicationPendingEntry> pending = bufferManager.getPendingMessages().getPendingEntries();
        Assert.assertEquals(2, pending.size());
        for (LogReplicationPendingEntry entry : pending) {
            Assert.assertTrue("entry seq=" + entry.getData().getMetadata().getSnapshotSyncSeqNum()
                    + " should be marked expedited", entry.isExpedited());
        }
    }

    @Test
    public void steadilyAdvancingExpectedSeqNumNeverExpeditesAHealthyTransfer() {
        // A perfectly healthy transfer where every ack reports a new, higher expectedSeqNum than
        // the last (i.e. real forward progress, no gap) must never trigger an expedited resend --
        // otherwise every single ack of a zero-loss transfer would blindly resend the entire
        // in-flight window, doubling load on the sink this mechanism is meant to protect.
        SenderBufferManager bufferManager = snapshotSender.getDataSenderBufferManager();
        bufferManager.sendWithBuffering(snapshotDataMessage());
        bufferManager.sendWithBuffering(snapshotDataMessage());
        bufferManager.sendWithBuffering(snapshotDataMessage());
        bufferManager.sendWithBuffering(snapshotDataMessage());
        // Auto-assigned seq nums are -1, 0, 1, 2 (see note above).

        bufferManager.updateAck(ackMessage(-1, 0));
        bufferManager.updateAck(ackMessage(0, 1));
        bufferManager.updateAck(ackMessage(1, 2));

        java.util.List<LogReplicationPendingEntry> pending = bufferManager.getPendingMessages().getPendingEntries();
        Assert.assertEquals(1, pending.size());
        Assert.assertFalse("steadily-advancing acks should never expedite", pending.get(0).isExpedited());
    }

    @Test
    public void ackWithoutExpectedSeqNumDoesNotExpediteAnything() {
        // Simulates a rolling upgrade where the sink is still on old code that never populates
        // expectedSeqNum -- must be a safe no-op, not a misinterpretation of the absent field.
        SenderBufferManager bufferManager = snapshotSender.getDataSenderBufferManager();
        bufferManager.sendWithBuffering(snapshotDataMessage());
        bufferManager.sendWithBuffering(snapshotDataMessage());
        bufferManager.sendWithBuffering(snapshotDataMessage());
        // Auto-assigned seq nums are -1, 0, 1 (see note above).

        bufferManager.updateAck(ackMessageWithoutExpectedSeqNum(0));

        java.util.List<LogReplicationPendingEntry> pending = bufferManager.getPendingMessages().getPendingEntries();
        Assert.assertEquals(1, pending.size());
        Assert.assertFalse(pending.get(0).isExpedited());
    }

    @Test
    public void expeditedEntryIsResentOnNextResendCallRegardlessOfItsOwnCadenceTimer() {
        SenderBufferManager bufferManager = snapshotSender.getDataSenderBufferManager();
        when(dataSender.send(any(LogReplicationEntryMsg.class))).thenReturn(new CompletableFuture<>());
        bufferManager.sendWithBuffering(snapshotDataMessage());
        LogReplicationPendingEntry entry = bufferManager.getPendingMessages().getPendingEntries().get(0);
        entry.setExpedited(true);
        int retriesBefore = entry.getRetry();

        bufferManager.resend();

        Assert.assertFalse("expedited flag should be cleared once acted upon", entry.isExpedited());
        Assert.assertEquals(retriesBefore + 1, entry.getRetry());
    }
}
