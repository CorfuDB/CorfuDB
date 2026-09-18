package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.protobuf.ByteString;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ISnapshotSyncPlugin;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager.LogReplicationMetadataType;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.service.CorfuProtocolLogReplication;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Outcome;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Phase;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCheckpointerHelper;
import org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg.Reason;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryType;
import org.corfudb.runtime.SnapshotSyncLease;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.LogReplicationBusyException;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.util.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The sink side of snapshot sync, against a real store. Snapshot sync is governed by the sink's
 * lease only: there is no other protocol to fall back to, anything that cannot be processed is
 * answered with a typed reply instead of being dropped, and the checkpointer is released by the
 * sink on its own, whatever the source does.
 */
public class LogReplicationSinkManagerTest extends AbstractViewTest {

    private static final long TOPOLOGY = 5L;
    private static final long SNAPSHOT = 100L;
    private static final long IDLE_MS = 1500;

    private final String streamName = "sink-manager-test-data";
    private final UUID stream = CorfuRuntime.getStreamID(streamName);

    private CorfuRuntime rt;
    private LogReplicationConfig config;
    private LogReplicationMetadataManager metadata;
    private DistributedCheckpointerHelper checkpointer;
    private ISnapshotSyncPlugin plugin;
    private LogReplicationSinkManager sink;

    @Before
    public void setUp() throws Exception {
        rt = getDefaultRuntime();
        metadata = new LogReplicationMetadataManager(rt, TOPOLOGY, "sink");
        checkpointer = new DistributedCheckpointerHelper(metadata.getCorfuStore());
        // A configured compaction service creates this record on its first pass. With it, admission
        // stays closed after an attempt until a checkpoint and trim pass its recovery cut, so the
        // state an abandoned attempt leaves behind stays in place to be inspected.
        try (TxnContext txn = metadata.getTxnContext()) {
            txn.putRecord(checkpointer.getCompactorMetadataTables().getCompactionManagerTable(),
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY,
                    CheckpointingStatus.newBuilder().setStatus(CheckpointingStatus.StatusType.IDLE).build(), null);
            txn.commit();
        }

        config = mock(LogReplicationConfig.class);
        LogReplicationConfigManager configManager = mock(LogReplicationConfigManager.class);
        when(config.getConfigManager()).thenReturn(configManager);
        when(configManager.getConfigRuntime()).thenReturn(rt);
        when(config.getStreamsIdToNameMap()).thenReturn(Map.of(stream, streamName));
        when(config.getMaxDataSizePerMsg()).thenReturn(1024 * 1024);

        plugin = mock(ISnapshotSyncPlugin.class);
        sink = new LogReplicationSinkManager(rt, config, metadata, plugin);
        sink.configureSnapshotLifecycle(new SnapshotLeaseCoordinator.Timing(60_000, 0, 5_000, IDLE_MS, 3));
        sink.updateTopologyConfigId(TOPOLOGY);
    }

    @After
    public void tearDown() {
        sink.shutdown();
    }

    private void await(BooleanSupplier condition) throws InterruptedException {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean() && System.nanoTime() < limit) { TimeUnit.MILLISECONDS.sleep(10); }
        assertTrue("the sink did not reach the expected state, lease=" + sink.getSnapshotLease(), condition.getAsBoolean());
    }

    private void lead() throws InterruptedException {
        sink.setLeadership(true);
        await(() -> sink.getSnapshotLease().getPhase() == Phase.READY);
    }

    private LogReplicationEntryMetadataMsg.Builder header(LogReplicationEntryType type) {
        return LogReplicationEntryMetadataMsg.newBuilder().setSnapshotLifecycleVersion(1).setEntryType(type)
                .setTopologyConfigID(TOPOLOGY).setSnapshotTimestamp(SNAPSHOT);
    }

    private LogReplicationEntryMsg startMessage() {
        return LogReplicationEntryMsg.newBuilder().setMetadata(header(LogReplicationEntryType.SNAPSHOT_START)
                .setSyncRequestId(getUuidMsg(UUID.randomUUID()))
                .setAdmissionEpoch(sink.getSnapshotLease().getAdmissionEpoch())).build();
    }

    private LogReplicationEntryMsg dataMessage(SnapshotSyncLeaseRecord attempt, long sequence) {
        OpaqueEntry opaque = new OpaqueEntry(SNAPSHOT, Map.of(stream, Collections.singletonList(
                new SMREntry("put", new Object[] {"key-" + sequence, "value"}, Serializers.PRIMITIVE))));
        return CorfuProtocolLogReplication.getLrEntryMsg(ByteString.copyFrom(
                CorfuProtocolLogReplication.generatePayload(Collections.singletonList(opaque))),
                header(LogReplicationEntryType.SNAPSHOT_MESSAGE).setSyncRequestId(attempt.getAttemptId())
                        .setAttemptGeneration(attempt.getGeneration()).setSnapshotSyncSeqNum(sequence).build());
    }

    /** The source proposes, is told to retry while the sink prepares, and is then accepted. */
    private SnapshotSyncLeaseRecord admit() throws InterruptedException {
        LogReplicationEntryMsg start = startMessage();
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true) {
            try {
                LogReplicationEntryMsg accepted = sink.receive(start);
                assertEquals(LogReplicationEntryType.SNAPSHOT_START_ACCEPTED, accepted.getMetadata().getEntryType());
                assertEquals(sink.getSnapshotLease().getGeneration(), accepted.getMetadata().getAttemptGeneration());
                return sink.getSnapshotLease();
            } catch (LogReplicationBusyException busy) {
                // Told to come back: the sink is busy, or has reserved the attempt and prepares it.
                assertEquals(Reason.ADMISSION_CLOSED, busy.getResponse().getReason());
                assertTrue("the proposal was never accepted: " + sink.getSnapshotLease(), System.nanoTime() < limit);
                TimeUnit.MILLISECONDS.sleep(20);
            }
        }
    }

    private Reason rejection(LogReplicationEntryMsg message) {
        LogReplicationBusyException busy = assertThrows(LogReplicationBusyException.class, () -> sink.receive(message));
        assertTrue("the source is told when to come back", busy.getResponse().getRetryAfterMs() > 0);
        assertEquals("the reply carries the lease the source has to act on",
                sink.getSnapshotLease().getGeneration(), busy.getResponse().getSnapshotLease().getGeneration());
        return busy.getResponse().getReason();
    }

    @Test
    public void nothingIsAdmittedBeforeThisNodeLeads() {
        assertEquals(Phase.NOT_READY, sink.getSnapshotLease().getPhase());
        assertFalse(sink.isIncrementalSyncAdmitted());
        assertEquals(Reason.ADMISSION_CLOSED, rejection(startMessage()));
        assertEquals(Reason.ADMISSION_CLOSED, rejection(LogReplicationEntryMsg.newBuilder()
                .setMetadata(header(LogReplicationEntryType.LOG_ENTRY_MESSAGE).setTimestamp(101).setPreviousTimestamp(100)).build()));
        assertEquals(Reason.STALE_ATTEMPT, rejection(dataMessage(SnapshotSyncLeaseRecord.getDefaultInstance(), 0)));
        assertEquals(3, (int) sink.getRxMessageCount().getValue());
        verify(plugin, never()).acquireSnapshot(any(), any());
    }

    @Test
    public void aMessageOfAnotherTopologyIsAnsweredNotDropped() throws Exception {
        lead();
        assertEquals(Reason.STALE_ATTEMPT, rejection(LogReplicationEntryMsg.newBuilder().setMetadata(
                startMessage().getMetadata().toBuilder().setTopologyConfigID(TOPOLOGY + 1)).build()));
        assertEquals(Phase.READY, sink.getSnapshotLease().getPhase());
    }

    /**
     * The sink cluster is upgraded first, so for a while its source still speaks the pre-lease
     * protocol. It must be refused outright: serving it would freeze the checkpointer through the
     * freeze token, with none of the bounds the lease provides.
     */
    @Test
    public void aPreLeaseSourceIsRefusedAndNeverFreezesTheCheckpointer() throws Exception {
        lead();
        LogReplicationEntryMsg legacyStart = LogReplicationEntryMsg.newBuilder().setMetadata(
                startMessage().getMetadata().toBuilder().clearSnapshotLifecycleVersion().clearAdmissionEpoch()).build();
        assertEquals(Reason.UNSUPPORTED_PROTOCOL, rejection(legacyStart));
        assertEquals(Phase.READY, sink.getSnapshotLease().getPhase());
        assertFalse(sink.getSnapshotLease().getProtectionHeld());
        assertFalse(checkpointer.isCheckpointFrozen());
        assertEquals(-1, metadata.queryMetadata(LogReplicationMetadataType.LAST_SNAPSHOT_STARTED));
        verify(plugin, never()).acquireSnapshot(any(), any());
        verify(plugin, never()).onSnapshotSyncStart(any());
    }

    @Test
    public void thePluginIsNotifiedThroughTheLeaseHooksOnly() throws Exception {
        lead();
        SnapshotSyncLeaseRecord attempt = admit();
        assertTrue(checkpointer.isCheckpointFrozen());
        verify(plugin, times(1)).acquireSnapshot(any(), any());

        assertEquals(Reason.STALE_ATTEMPT, rejection(LogReplicationEntryMsg.newBuilder().setMetadata(
                header(LogReplicationEntryType.SNAPSHOT_CANCEL).setSyncRequestId(attempt.getAttemptId())
                        .setAttemptGeneration(attempt.getGeneration())).build()));
        verify(plugin, timeout(10_000).times(1)).releaseSnapshot(any(), any());
        await(() -> !sink.getSnapshotLease().getProtectionHeld());
        assertFalse(checkpointer.isCheckpointFrozen());
        assertEquals("Source cancelled its snapshot cut", sink.getSnapshotLease().getFailure());
        // The pre-lease hooks, whose production implementation writes the freeze token, are dead.
        verify(plugin, never()).onSnapshotSyncStart(any());
        verify(plugin, never()).onSnapshotSyncEnd(any());
    }

    /**
     * The sink reserves an attempt, and freezes the checkpointer for it, when it takes the START. A
     * source that gives up before it sees the acceptance does not know the generation it was given:
     * its cancellation is matched by the attempt id, which only that source has. Without this the
     * reservation, and the freeze, would last until the inactivity limit.
     */
    @Test
    public void aSourceThatGivesUpBeforeItSawItsAdmissionStillReleasesTheCheckpointer() throws Exception {
        lead();
        LogReplicationEntryMsg start = startMessage();
        LogReplicationBusyException reserved = assertThrows(LogReplicationBusyException.class, () -> sink.receive(start));
        assertEquals(Reason.ADMISSION_CLOSED, reserved.getResponse().getReason());
        assertEquals("preparation only takes a moment, and the source is told so",
                SnapshotLeaseCoordinator.PREPARING_RETRY_AFTER_MS, reserved.getResponse().getRetryAfterMs());
        assertTrue(checkpointer.isCheckpointFrozen());

        // Somebody else's cancellation changes nothing.
        assertEquals(Reason.STALE_ATTEMPT, rejection(LogReplicationEntryMsg.newBuilder().setMetadata(
                header(LogReplicationEntryType.SNAPSHOT_CANCEL).setSyncRequestId(getUuidMsg(UUID.randomUUID()))).build()));
        assertTrue(checkpointer.isCheckpointFrozen());

        assertEquals(Reason.STALE_ATTEMPT, rejection(LogReplicationEntryMsg.newBuilder().setMetadata(
                header(LogReplicationEntryType.SNAPSHOT_CANCEL).setSyncRequestId(start.getMetadata().getSyncRequestId())).build()));
        await(() -> !sink.getSnapshotLease().getProtectionHeld());
        assertFalse(checkpointer.isCheckpointFrozen());
        assertEquals("Source cancelled its snapshot cut", sink.getSnapshotLease().getFailure());
    }

    /** A cancellation that names a generation must name the right one. */
    @Test
    public void aCancellationOfAnotherGenerationIsRefused() throws Exception {
        lead();
        SnapshotSyncLeaseRecord attempt = admit();
        assertEquals(Reason.STALE_ATTEMPT, rejection(LogReplicationEntryMsg.newBuilder().setMetadata(
                header(LogReplicationEntryType.SNAPSHOT_CANCEL).setSyncRequestId(attempt.getAttemptId())
                        .setAttemptGeneration(attempt.getGeneration() + 1)).build()));
        assertTrue(SnapshotSyncLease.active(sink.getSnapshotLease()));
        assertTrue(checkpointer.isCheckpointFrozen());
    }

    /**
     * The scenario behind the original incident, from the sink's point of view: the source goes
     * away in the middle of a transfer and never comes back. The sink notices the silence itself and
     * releases the checkpointer long before the attempt's budget is used up.
     */
    @Test
    public void aSourceThatDisappearsMidTransferReleasesTheCheckpointerWellBeforeTheBudget() throws Exception {
        lead();
        SnapshotSyncLeaseRecord attempt = admit();
        sink.receive(dataMessage(attempt, 0));
        assertTrue(checkpointer.isCheckpointFrozen());
        long silentSince = System.nanoTime();

        await(() -> sink.getSnapshotLease().getOutcome() == Outcome.ABORTED && !sink.getSnapshotLease().getProtectionHeld());

        long waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - silentSince);
        assertTrue("released after " + waitedMs + " ms of a 60 s budget", waitedMs < 20_000);
        assertTrue(sink.getSnapshotLease().getFailure(), sink.getSnapshotLease().getFailure().startsWith("No snapshot traffic"));
        assertEquals(1, sink.getSnapshotLease().getConsecutiveAborts());
        assertFalse(checkpointer.isCheckpointFrozen());
        // The source comes back with the old attempt: it is told to start over, not served.
        assertEquals(Reason.STALE_ATTEMPT, rejection(dataMessage(attempt, 1)));
    }

    @Test
    public void aSourceThatKeepsSendingIsNotAbandonedForInactivity() throws Exception {
        lead();
        SnapshotSyncLeaseRecord attempt = admit();
        long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(3 * IDLE_MS);
        for (long sequence = 0; System.nanoTime() < until; sequence++) {
            sink.receive(dataMessage(attempt, sequence));
            TimeUnit.MILLISECONDS.sleep(IDLE_MS / 5);
        }
        assertEquals(Phase.TRANSFERRING, sink.getSnapshotLease().getPhase());
        assertEquals(attempt.getDeadlineMs(), sink.getSnapshotLease().getDeadlineMs());
    }

    /**
     * A sequencer that fails over refuses the transactions it was asked for before anything is
     * written. That is no reason to throw away a transfer that may have been running for an hour.
     */
    @Test
    public void aWriteTheSequencerRefusedIsResentInsteadOfCostingTheWholeTransfer() throws Exception {
        lead();
        StreamsSnapshotWriter writer = spy(new StreamsSnapshotWriter(rt, config, metadata));
        sink.setSnapshotWriter(writer);
        SnapshotSyncLeaseRecord attempt = admit();
        doThrow(new StreamsSnapshotWriter.RetryableWriteException(new IllegalStateException("sequencer failed over")))
                .doCallRealMethod().when(writer).apply(any(LogReplicationEntryMsg.class));

        assertEquals(Reason.OVERLOADED, rejection(dataMessage(attempt, 0)));
        assertEquals(Phase.TRANSFERRING, sink.getSnapshotLease().getPhase());

        // The source sends the same message again, and the transfer goes on where it was.
        sink.receive(dataMessage(attempt, 0));
        sink.receive(dataMessage(attempt, 1));
        await(() -> sink.getSnapshotLease().getTransferredSequence() == 1);
        assertEquals(Outcome.NONE, sink.getSnapshotLease().getOutcome());
        assertEquals(0, sink.getSnapshotLease().getConsecutiveAborts());
    }

    @Test
    public void resetLeavesARunningAttemptAlone() throws Exception {
        lead();
        SnapshotSyncLeaseRecord attempt = admit();
        sink.receive(dataMessage(attempt, 0));
        sink.reset();
        sink.receive(dataMessage(attempt, 1));
        // The writer records its progress in the store; the published view follows within a second.
        await(() -> sink.getSnapshotLease().getTransferredSequence() == 1);
        assertEquals(Phase.TRANSFERRING, sink.getSnapshotLease().getPhase());
    }

    @Test
    public void aTopologyChangeAbandonsTheAttemptAndReleasesTheCheckpointer() throws Exception {
        lead();
        SnapshotSyncLeaseRecord attempt = admit();
        sink.updateTopologyConfigId(TOPOLOGY + 1);
        await(() -> sink.getSnapshotLease().getOutcome() == Outcome.ABORTED && !sink.getSnapshotLease().getProtectionHeld());
        assertEquals("Topology changed", sink.getSnapshotLease().getFailure());
        assertEquals(Reason.STALE_ATTEMPT, rejection(dataMessage(attempt, 0)));
        assertFalse(checkpointer.isCheckpointFrozen());
    }

    @Test
    public void losingLeadershipClosesTheSinkAndTheNextLeadershipAbandonsTheAttempt() throws Exception {
        lead();
        SnapshotSyncLeaseRecord attempt = admit();
        sink.stopOnLeadershipLoss();
        assertEquals(Phase.NOT_READY, sink.getSnapshotLease().getPhase());
        assertEquals(Reason.STALE_ATTEMPT, rejection(dataMessage(attempt, 0)));

        sink.setLeadership(true);
        await(() -> sink.getSnapshotLease().getOutcome() == Outcome.ABORTED && !sink.getSnapshotLease().getProtectionHeld());
        assertEquals("Sink ownership changed", sink.getSnapshotLease().getFailure());
        assertEquals(attempt.getDeadlineMs(), sink.getSnapshotLease().getDeadlineMs());
    }

    @Test
    public void theDriverCannotBeReplacedOnceItRuns() throws Exception {
        lead();
        assertThrows(IllegalStateException.class, () -> sink.configureSnapshotLifecycle(
                new SnapshotLeaseCoordinator.Timing(1_000, 0, 1_000, 0, 0)));
    }

    @Test
    public void leaseTimingComesFromTheConfigurationAndMeaninglessValuesFallBack() {
        SnapshotLeaseCoordinator.Timing defaults = new SnapshotLeaseCoordinator.Timing(
                LogReplicationConfig.DEFAULT_SNAPSHOT_LEASE_DURATION_MS, LogReplicationConfig.DEFAULT_SNAPSHOT_LEASE_MIN_RECOVERY_MS,
                LogReplicationConfig.DEFAULT_SNAPSHOT_LEASE_ALARM_MS, LogReplicationConfig.DEFAULT_SNAPSHOT_LEASE_TRANSFER_IDLE_MS,
                LogReplicationConfig.DEFAULT_SNAPSHOT_LEASE_MAX_APPLY_RETRIES);
        assertEquals(defaults, LogReplicationSinkManager.readLeaseTiming(new Properties(), defaults));

        Properties configured = new Properties();
        configured.setProperty("snapshot_lifecycle_max_duration_ms", "7200000");
        configured.setProperty("snapshot_lifecycle_min_recovery_ms", "0");
        configured.setProperty("snapshot_lifecycle_recovery_alarm_ms", "600000");
        configured.setProperty("snapshot_lifecycle_transfer_idle_ms", "0");
        configured.setProperty("snapshot_lifecycle_max_apply_retries", "0");
        assertEquals(new SnapshotLeaseCoordinator.Timing(7_200_000, 0, 600_000, 0, 0),
                LogReplicationSinkManager.readLeaseTiming(configured, defaults));

        // A budget that is not finite and positive would bring the unbounded freeze back.
        Properties meaningless = new Properties();
        meaningless.setProperty("snapshot_lifecycle_max_duration_ms", "0");
        meaningless.setProperty("snapshot_lifecycle_min_recovery_ms", "-5");
        meaningless.setProperty("snapshot_lifecycle_recovery_alarm_ms", "-1");
        meaningless.setProperty("snapshot_lifecycle_max_apply_retries", "-2");
        assertEquals(defaults, LogReplicationSinkManager.readLeaseTiming(meaningless, defaults));

        Properties malformed = new Properties();
        malformed.setProperty("snapshot_lifecycle_max_duration_ms", "ninety minutes");
        assertThrows(NumberFormatException.class, () -> LogReplicationSinkManager.readLeaseTiming(malformed, defaults));
    }
}
