package org.corfudb.infrastructure.logreplication.replication.receive;

import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ISnapshotSyncPlugin;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.runtime.view.Address;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.UUID;

import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Unit tests for the concurrency/state-machine logic added to LogReplicationSinkManager by the
 * snapshot-sync backpressure/freeze-lifecycle fix: resumeSnapshotApply()'s backoff-and-cap
 * accounting, the receive()/ongoingApply single-flight gate, self-unfreeze
 * (checkSnapshotSyncLiveness()) and stuck-apply detection (checkForStuckApply()). Previously none
 * of this had dedicated unit coverage -- only indirectly, and non-deterministically w.r.t. timing,
 * through the full two-cluster LogReplicationIT integration tests.
 *
 * Uses the {@code @VisibleForTesting} constructor that skips real CorfuRuntime.connect() and
 * reflective plugin classloading (see that constructor's Javadoc), and the package-private
 * (also {@code @VisibleForTesting}) backoff/timestamp fields to fast-forward past real-time delays
 * (backoff waits up to MAX_RETRY_BACKOFF_MS, the self-unfreeze timeout, the 30 minute stuck-apply
 * bound) that would otherwise make these paths impractical to exercise in a unit test.
 */
public class LogReplicationSinkManagerTest extends AbstractViewTest {

    private static final long TOPOLOGY_CONFIG_ID = 5L;

    private CorfuRuntime corfuRuntime;
    private LogReplicationMetadataManager metadataManager;
    private ISnapshotSyncPlugin snapshotSyncPlugin;
    private LogReplicationSinkManager sinkManager;

    @Before
    public void setUp() {
        corfuRuntime = getDefaultRuntime();
        // Force protobuf serializer registration, needed by SinkWriter's constructor chain.
        corfuRuntime.getTableRegistry();

        LogReplicationConfig config = mock(LogReplicationConfig.class);
        LogReplicationConfigManager configManager = mock(LogReplicationConfigManager.class);
        doReturn(configManager).when(config).getConfigManager();
        doReturn(corfuRuntime).when(configManager).getConfigRuntime();

        metadataManager = mock(LogReplicationMetadataManager.class);
        doReturn(TOPOLOGY_CONFIG_ID).when(metadataManager).getTopologyConfigId();

        snapshotSyncPlugin = mock(ISnapshotSyncPlugin.class);

        sinkManager = new LogReplicationSinkManager(corfuRuntime, config, metadataManager, snapshotSyncPlugin);
        sinkManager.updateTopologyConfigId(TOPOLOGY_CONFIG_ID);
    }

    private LogReplicationEntryMsg snapshotStartMsg(UUID syncId, long snapshotTimestamp) {
        LogReplicationEntryMetadataMsg metadata = LogReplicationEntryMetadataMsg.newBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_START)
                .setTopologyConfigID(TOPOLOGY_CONFIG_ID)
                .setSyncRequestId(getUuidMsg(syncId))
                .setSnapshotTimestamp(snapshotTimestamp)
                .setSnapshotSyncSeqNum(Address.NON_ADDRESS)
                .build();
        return LogReplicationEntryMsg.newBuilder().setMetadata(metadata).build();
    }

    // ---- receive() / ongoingApply single-flight gate ----

    @Test
    public void receiveDropsMessageWithMismatchedTopologyConfigId() {
        LogReplicationEntryMetadataMsg metadata = LogReplicationEntryMetadataMsg.newBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_START)
                .setTopologyConfigID(TOPOLOGY_CONFIG_ID + 1)
                .setSyncRequestId(getUuidMsg(UUID.randomUUID()))
                .setSnapshotTimestamp(1L)
                .build();
        LogReplicationEntryMsg msg = LogReplicationEntryMsg.newBuilder().setMetadata(metadata).build();

        Assert.assertNull(sinkManager.receive(msg));
        // Never even reaches isValidSnapshotStart()/processSnapshotStart() -- the plugin must not
        // have been asked to freeze for a message with the wrong topology config id.
        verify(snapshotSyncPlugin, never()).onSnapshotSyncStart(any());
    }

    @Test
    public void processSnapshotStartFreezesAndStampsBaseSnapshotTimestamp() {
        doReturn(true).when(metadataManager).setBaseSnapshotStart(eq(TOPOLOGY_CONFIG_ID), eq(100L));

        Assert.assertNull(sinkManager.receive(snapshotStartMsg(UUID.randomUUID(), 100L)));

        verify(snapshotSyncPlugin, times(1)).onSnapshotSyncStart(corfuRuntime);
        Assert.assertEquals(100L, sinkManager.getBaseSnapshotTimestamp());
    }

    @Test
    public void receiveDropsFreshSnapshotStartWhileOngoingApplyTrue() {
        // Simulate a resumed (or in-flight) apply for a prior attempt still running -- the sole
        // gate that keeps a fresh attempt's SNAPSHOT_START from being accepted (and, transitively,
        // from clobbering the in-flight apply's metadata-store bookkeeping); see receive()'s
        // Javadoc-level comment and this class's isMessageFromNewSnapshotSync().
        sinkManager.getOngoingApply().set(true);

        Assert.assertNull(sinkManager.receive(snapshotStartMsg(UUID.randomUUID(), 200L)));

        verify(snapshotSyncPlugin, never()).onSnapshotSyncStart(any());
        verify(metadataManager, never()).setBaseSnapshotStart(anyLong(), anyLong());
    }

    // ---- startSnapshotApply(): an individual failed attempt must not unfreeze ----

    @Test
    public void individualApplyFailureDoesNotUnfreezeWhileRetriesRemain() throws Exception {
        // There's no way to force a real concurrent trim deterministically in a unit test, so this
        // injects the one otherwise-untriggerable failure directly: the apply call itself throwing,
        // exactly as it would if a concurrent trim removed shadow-stream data mid-read.
        StreamsSnapshotWriter failingWriter = mock(StreamsSnapshotWriter.class);
        doReturn(StreamsSnapshotWriter.Phase.TRANSFER_PHASE).when(failingWriter).getPhase();
        doThrow(new TrimmedException("shadow stream data trimmed concurrently"))
                .when(failingWriter).startSnapshotSyncApply();
        sinkManager.setSnapshotWriter(failingWriter);
        doReturn(true).when(metadataManager).setBaseSnapshotStart(anyLong(), anyLong());
        // Otherwise defaults to 0 (Mockito's default for an unstubbed long-returning method),
        // which would desync SnapshotSinkBufferManager's lastProcessedSeq from the seqNum=0
        // SNAPSHOT_END below and cause it to be silently buffered instead of processed.
        doReturn(Address.NON_ADDRESS).when(metadataManager).getLastSnapshotTransferredSequenceNumber();

        UUID syncId = UUID.randomUUID();
        sinkManager.receive(snapshotStartMsg(syncId, 100L));
        LogReplicationEntryMetadataMsg endMetadata = LogReplicationEntryMetadataMsg.newBuilder()
                .setEntryType(LogReplicationEntryType.SNAPSHOT_END)
                .setTopologyConfigID(TOPOLOGY_CONFIG_ID)
                .setSyncRequestId(getUuidMsg(syncId))
                .setSnapshotTimestamp(100L)
                .setSnapshotSyncSeqNum(0L)
                .build();
        sinkManager.receive(LogReplicationEntryMsg.newBuilder().setMetadata(endMetadata).build());

        long deadline = System.currentTimeMillis() + 10_000;
        while (sinkManager.getOngoingApply().get() && System.currentTimeMillis() < deadline) {
            Thread.sleep(50);
        }
        Assert.assertFalse("ongoingApply must reset even though the attempt failed",
                sinkManager.getOngoingApply().get());

        // The sequence stays marked active, and checkpointing stays frozen -- see
        // applyRetrySequenceActive's Javadoc for why: this attempt may still be retried in place by
        // resumeSnapshotApply(), reading the same shadow-stream data.
        Assert.assertTrue(sinkManager.applyRetrySequenceActive);
        verify(snapshotSyncPlugin, never()).onSnapshotSyncEnd(any());
    }

    // ---- resumeSnapshotApply(): pending/ongoing re-validation ----

    @Test
    public void resumeSnapshotApplySkipsWhenOngoingApplyTrue() {
        sinkManager.getOngoingApply().set(true);
        doReturn(10L).when(metadataManager).getLastStartedSnapshotTimestamp();
        doReturn(10L).when(metadataManager).getLastTransferredSnapshotTimestamp();
        doReturn(5L).when(metadataManager).getLastAppliedSnapshotTimestamp();

        sinkManager.resumeSnapshotApply();

        // Nothing should have been touched -- ongoingApply.get() being true short-circuits before
        // any backoff/attempt accounting.
        Assert.assertEquals(0, sinkManager.resumeAttemptCount);
    }

    @Test
    public void resumeSnapshotApplySkipsWhenNotActuallyPending() {
        // started == transferred, but transferred <= applied: already applied, nothing to resume.
        doReturn(10L).when(metadataManager).getLastStartedSnapshotTimestamp();
        doReturn(10L).when(metadataManager).getLastTransferredSnapshotTimestamp();
        doReturn(10L).when(metadataManager).getLastAppliedSnapshotTimestamp();

        sinkManager.resumeSnapshotApply();

        Assert.assertEquals(0, sinkManager.resumeAttemptCount);
        Assert.assertFalse(sinkManager.getOngoingApply().get());
    }

    // ---- resumeSnapshotApply(): backoff accounting ----

    @Test
    public void resumeSnapshotApplyDefersWithinBackoffWindow() {
        seedPendingApply(10L);
        sinkManager.resumeBackoffForStartedTimestamp = 10L;
        sinkManager.nextResumeAttemptTimeMs = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();

        sinkManager.resumeSnapshotApply();

        Assert.assertEquals(0, sinkManager.resumeAttemptCount);
        Assert.assertFalse(sinkManager.getOngoingApply().get());
    }

    @Test
    public void resumeSnapshotApplyProceedsOnceBackoffElapsedAndStampsBaseSnapshotTimestamp() {
        seedPendingApply(10L);

        sinkManager.resumeSnapshotApply();

        Assert.assertEquals(1, sinkManager.resumeAttemptCount);
        Assert.assertEquals(10L, sinkManager.getBaseSnapshotTimestamp());
        // startSnapshotApplyAsync() submits to an async executor; ongoingApply flips to true
        // synchronously (before submission) so this doesn't need to wait for the task to run.
        Assert.assertTrue(sinkManager.getOngoingApply().get());
    }

    @Test
    public void resumeSnapshotApplyResetsBackoffAndCountForAGenuinelyNewAttempt() {
        seedPendingApply(10L);
        sinkManager.resumeAttemptCount = 3;
        sinkManager.resumeBackoffMs = LogReplicationConfig.MAX_RETRY_BACKOFF_MS;
        sinkManager.resumeBackoffForStartedTimestamp = 10L;
        sinkManager.loggedResumeExhaustedForCurrentAttempt = true;

        // A different startedTimestamp is a genuinely new attempt.
        seedPendingApply(20L);

        sinkManager.resumeSnapshotApply();

        Assert.assertEquals(1, sinkManager.resumeAttemptCount);
        Assert.assertEquals(LogReplicationConfig.INITIAL_RETRY_BACKOFF_MS, sinkManager.resumeBackoffMs);
        Assert.assertFalse(sinkManager.loggedResumeExhaustedForCurrentAttempt);
        Assert.assertFalse("a genuinely new attempt must not inherit the prior attempt's exhausted " +
                "status either", sinkManager.isApplyRetriesExhausted());
    }

    @Test
    public void resumeSnapshotApplyGivesUpAfterConfiguredMaxRetries() {
        final int cap = 3;
        sinkManager.maxSnapshotApplyResumeRetries = cap;
        seedPendingApply(10L);
        sinkManager.resumeBackoffForStartedTimestamp = 10L;
        sinkManager.resumeAttemptCount = cap;
        sinkManager.nextResumeAttemptTimeMs = 0;

        sinkManager.resumeSnapshotApply();

        // Gave up: no new attempt submitted, count not incremented further, alert logged once.
        Assert.assertFalse(sinkManager.getOngoingApply().get());
        Assert.assertEquals(cap, sinkManager.resumeAttemptCount);
        Assert.assertTrue(sinkManager.loggedResumeExhaustedForCurrentAttempt);
        // The public, wire-facing signal (LogReplicationServer surfaces this in the metadata
        // response so the source can cancel and restart immediately -- see
        // WaitSnapshotApplyState.verifyStatusOfSnapshotSyncApply()) must reflect the same state.
        Assert.assertTrue(sinkManager.isApplyRetriesExhausted());

        // Calling again must not re-attempt or re-log (idempotent while still exhausted).
        sinkManager.resumeSnapshotApply();
        Assert.assertEquals(cap, sinkManager.resumeAttemptCount);
        Assert.assertTrue(sinkManager.isApplyRetriesExhausted());
    }

    @Test
    public void resumeSnapshotApplyHonorsASmallerConfiguredCap() {
        // Demonstrates the cap is genuinely configurable, not hardcoded: with a cap of 1, a single
        // attempt already exhausts it.
        sinkManager.maxSnapshotApplyResumeRetries = 1;
        seedPendingApply(10L);
        sinkManager.resumeBackoffForStartedTimestamp = 10L;
        sinkManager.resumeAttemptCount = 1;
        sinkManager.nextResumeAttemptTimeMs = 0;

        sinkManager.resumeSnapshotApply();

        Assert.assertFalse(sinkManager.getOngoingApply().get());
        Assert.assertTrue(sinkManager.loggedResumeExhaustedForCurrentAttempt);
        Assert.assertTrue(sinkManager.isApplyRetriesExhausted());
    }

    @Test
    public void resumeSnapshotApplyGiveUpUnfreezesExactlyOnce() {
        final int cap = 1;
        sinkManager.maxSnapshotApplyResumeRetries = cap;
        seedPendingApply(10L);
        sinkManager.resumeBackoffForStartedTimestamp = 10L;
        sinkManager.resumeAttemptCount = cap;
        sinkManager.nextResumeAttemptTimeMs = 0;
        // Simulate the sequence having been active since an earlier (now abandoned) attempt.
        sinkManager.applyRetrySequenceActive = true;

        sinkManager.resumeSnapshotApply();

        // This is the ONE deliberate unfreeze point for a failed-attempt sequence -- see
        // applyRetrySequenceActive's Javadoc.
        verify(snapshotSyncPlugin, times(1)).onSnapshotSyncEnd(corfuRuntime);
        Assert.assertFalse("the sequence must be marked over once truly abandoned",
                sinkManager.applyRetrySequenceActive);

        // Idempotent: calling again while still exhausted must not unfreeze a second time.
        sinkManager.resumeSnapshotApply();
        verify(snapshotSyncPlugin, times(1)).onSnapshotSyncEnd(corfuRuntime);
    }

    @Test
    public void getCheckpointerGracePeriodMsReflectsConfiguredValue() {
        Assert.assertEquals(LogReplicationConfig.DEFAULT_CHECKPOINTER_GRACE_PERIOD_MS,
                sinkManager.getCheckpointerGracePeriodMs());

        sinkManager.checkpointerGracePeriodMs = 900_000L;

        Assert.assertEquals(900_000L, sinkManager.getCheckpointerGracePeriodMs());
    }

    @Test
    public void isApplyRetriesExhaustedFalseBeforeAnyResumeAttempt() {
        // Baseline: a sink that has never had a stuck attempt must report false, not some
        // uninitialized/stale value.
        Assert.assertFalse(sinkManager.isApplyRetriesExhausted());
    }

    private void seedPendingApply(long startedAndTransferredTimestamp) {
        doReturn(startedAndTransferredTimestamp).when(metadataManager).getLastStartedSnapshotTimestamp();
        doReturn(startedAndTransferredTimestamp).when(metadataManager).getLastTransferredSnapshotTimestamp();
        doReturn(0L).when(metadataManager).getLastAppliedSnapshotTimestamp();
        doReturn(1L).when(metadataManager).getCurrentSnapshotSyncCycleId();
    }

    // ---- checkSnapshotSyncLiveness(): self-unfreeze ----

    @Test
    public void checkSnapshotSyncLivenessDoesNothingOutsideSnapshotSync() {
        // Default state is LOG_ENTRY_SYNC; must not touch the plugin regardless of idle time.
        sinkManager.lastSnapshotSyncActivityTimeMs = 0L;

        sinkManager.checkSnapshotSyncLiveness();

        verify(snapshotSyncPlugin, never()).onSnapshotSyncEnd(any());
    }

    @Test
    public void checkSnapshotSyncLivenessUnfreezesOnceAfterProlongedIdleInTransferPhase() {
        doReturn(true).when(metadataManager).setBaseSnapshotStart(anyLong(), anyLong());
        sinkManager.receive(snapshotStartMsg(UUID.randomUUID(), 1L));
        // A fresh SNAPSHOT_START always resets into TRANSFER_PHASE (StreamsSnapshotWriter.reset()),
        // which checkSnapshotSyncLiveness()'s self-unfreeze requires -- see its Javadoc.
        sinkManager.lastSnapshotSyncActivityTimeMs =
                System.currentTimeMillis() - LogReplicationConfig.SINK_SELF_UNFREEZE_TIMEOUT_MS - 1;

        sinkManager.checkSnapshotSyncLiveness();
        // A second consecutive call with the same stale idle time must not fire again -- proves
        // the guard (selfUnfrozeForCurrentIdlePeriod) via the only externally-observable effect.
        sinkManager.checkSnapshotSyncLiveness();

        verify(snapshotSyncPlugin, times(1)).onSnapshotSyncEnd(corfuRuntime);
    }

    @Test
    public void checkSnapshotSyncLivenessDoesNotFireBeforeTimeoutElapsed() {
        doReturn(true).when(metadataManager).setBaseSnapshotStart(anyLong(), anyLong());
        sinkManager.receive(snapshotStartMsg(UUID.randomUUID(), 1L));
        sinkManager.lastSnapshotSyncActivityTimeMs = System.currentTimeMillis();

        sinkManager.checkSnapshotSyncLiveness();

        verify(snapshotSyncPlugin, never()).onSnapshotSyncEnd(any());
    }

    @Test
    public void checkSnapshotSyncLivenessDoesNotFireDuringApplyPhase() {
        // Deliberately restricted to TRANSFER_PHASE (see the method's Javadoc): unfreezing during
        // an in-progress apply risks the checkpointer trimming shadow-stream data apply still needs.
        doReturn(true).when(metadataManager).setBaseSnapshotStart(anyLong(), anyLong());
        sinkManager.receive(snapshotStartMsg(UUID.randomUUID(), 1L));
        sinkManager.getOngoingApply().set(true);
        sinkManager.lastSnapshotSyncActivityTimeMs =
                System.currentTimeMillis() - LogReplicationConfig.SINK_SELF_UNFREEZE_TIMEOUT_MS - 1;

        sinkManager.checkSnapshotSyncLiveness();

        verify(snapshotSyncPlugin, never()).onSnapshotSyncEnd(any());
    }

    @Test
    public void checkSnapshotSyncLivenessDoesNotFireWhileApplyRetrySequenceActive() {
        // Simulates the gap *between* individual resume attempts (ongoingApply momentarily false,
        // phase back at TRANSFER_PHASE courtesy of resumeSnapshotApply()'s snapshotWriter.reset())
        // -- must stay frozen exactly like the actively-applying case, not just it. See
        // applyRetrySequenceActive's Javadoc for why unfreezing here would be unsafe.
        doReturn(true).when(metadataManager).setBaseSnapshotStart(anyLong(), anyLong());
        sinkManager.receive(snapshotStartMsg(UUID.randomUUID(), 1L));
        sinkManager.applyRetrySequenceActive = true;
        sinkManager.lastSnapshotSyncActivityTimeMs =
                System.currentTimeMillis() - LogReplicationConfig.SINK_SELF_UNFREEZE_TIMEOUT_MS - 1;

        sinkManager.checkSnapshotSyncLiveness();

        verify(snapshotSyncPlugin, never()).onSnapshotSyncEnd(any());
    }

    // ---- stopOnLeadershipLoss() ----

    @Test
    public void stopOnLeadershipLossUnfreezesInTransferPhaseWhenNoRetrySequenceActive() {
        doReturn(true).when(metadataManager).setBaseSnapshotStart(anyLong(), anyLong());
        sinkManager.receive(snapshotStartMsg(UUID.randomUUID(), 1L));

        sinkManager.stopOnLeadershipLoss();

        verify(snapshotSyncPlugin, times(1)).onSnapshotSyncEnd(corfuRuntime);
    }

    @Test
    public void stopOnLeadershipLossDoesNotUnfreezeWhileApplyRetrySequenceActive() {
        // Same reasoning as checkSnapshotSyncLiveness() above: losing leadership in the gap between
        // resume attempts must not unfreeze out from under a retry sequence a metadata poll on this
        // node isn't going to resume again anyway (handleMetadataRequest() is gated on leadership).
        doReturn(true).when(metadataManager).setBaseSnapshotStart(anyLong(), anyLong());
        sinkManager.receive(snapshotStartMsg(UUID.randomUUID(), 1L));
        sinkManager.applyRetrySequenceActive = true;

        sinkManager.stopOnLeadershipLoss();

        verify(snapshotSyncPlugin, never()).onSnapshotSyncEnd(any());
    }

        // ---- checkForStuckApply(): detection-only, must never throw and must be idempotent ----

    @Test
    public void checkForStuckApplyIsANoOpWhenNoApplyOngoing() {
        sinkManager.getOngoingApply().set(false);
        sinkManager.checkForStuckApply();
    }

    @Test
    public void checkForStuckApplyDoesNotAlertBeforeMaxWaitElapsed() {
        sinkManager.getOngoingApply().set(true);
        sinkManager.applyStartTimeMs = System.currentTimeMillis();
        sinkManager.checkForStuckApply();
    }

    @Test
    public void checkForStuckApplyAlertsPastMaxWaitAndIsIdempotent() {
        sinkManager.getOngoingApply().set(true);
        sinkManager.applyStartTimeMs =
                System.currentTimeMillis() - LogReplicationConfig.SNAPSHOT_SYNC_APPLY_MAX_WAIT_MS - 1;

        // Exercises the compareAndSet guard on the first call and its "already logged" branch on
        // the second -- must not throw either way.
        sinkManager.checkForStuckApply();
        sinkManager.checkForStuckApply();
    }

    // ---- isProcessingSnapshotSync() ----

    @Test
    public void isProcessingSnapshotSyncTrueWhileApplyOngoing() {
        sinkManager.getOngoingApply().set(true);
        Assert.assertTrue(sinkManager.isProcessingSnapshotSync());
    }
}
