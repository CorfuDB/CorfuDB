package org.corfudb.infrastructure;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.ByteString;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.replication.receive.LogEntryWriter;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.receive.StreamsSnapshotWriter;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.infrastructure.logreplication.replication.receive.SnapshotLeaseCoordinator;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultSnapshotSyncPlugin;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.service.CorfuProtocolLogReplication;
import org.corfudb.runtime.*;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.LogReplication.*;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.collections.PersistentCorfuTable;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.view.AbstractViewTest;
import org.corfudb.util.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.corfudb.protocols.CorfuProtocolCommon.getUuidMsg;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/** Real Corfu transactions and shadow streams; no storage mocks. */
public class SnapshotLeaseStorageTest extends AbstractViewTest {
    private CorfuRuntime rt;
    private LogReplicationMetadataManager metadata;
    private SnapshotSyncLeaseStore leases;
    private DistributedCheckpointerHelper helper;
    private LogReplicationConfig config;
    private final String streamName = "protected-snapshot-data";
    private final UUID stream = CorfuRuntime.getStreamID(streamName);

    @Before
    public void setup() throws Exception {
        rt = getDefaultRuntime();
        metadata = new LogReplicationMetadataManager(rt, 1, "sink");
        leases = new SnapshotSyncLeaseStore(metadata.getCorfuStore());
        helper = new DistributedCheckpointerHelper(metadata.getCorfuStore());
        leases.update((txn, state) -> SnapshotSyncLease.seedIdle("owner", -1, 1));
        config = mock(LogReplicationConfig.class);
        LogReplicationConfigManager manager = mock(LogReplicationConfigManager.class);
        when(config.getConfigManager()).thenReturn(manager);
        when(manager.getConfigRuntime()).thenReturn(rt);
        when(config.getStreamsIdToNameMap()).thenReturn(Map.of(stream, streamName));
        when(config.getMaxDataSizePerMsg()).thenReturn(1024 * 1024);
    }

    private SnapshotSyncLeaseRecord reserve() {
        return leases.updateOwned("owner", (txn, current) -> {
            LogReplicationEntryMetadataMsg start = LogReplicationEntryMetadataMsg.newBuilder()
                    .setSnapshotLifecycleVersion(1).setAdmissionEpoch(current.getAdmissionEpoch())
                    .setEntryType(LogReplicationEntryType.SNAPSHOT_START).setTopologyConfigID(1)
                    .setSyncRequestId(getUuidMsg(UUID.randomUUID())).setSnapshotTimestamp(100).build();
            metadata.initializeSnapshot(txn, start);
            return SnapshotSyncLease.reserve(current, start, System.currentTimeMillis(), 60000,
                    txn.getTxnSequence(), UUID.randomUUID().toString());
        });
    }

    private SnapshotSyncLeaseRecord transferring() {
        reserve();
        return leases.updateOwned("owner", (txn, current) -> SnapshotSyncLease.prepared(current));
    }

    private LogReplicationEntryMsg data(SnapshotSyncLeaseRecord state) {
        OpaqueEntry opaque = new OpaqueEntry(100, Map.of(stream, Collections.singletonList(
                new SMREntry("put", new Object[] {"key", "value"}, Serializers.PRIMITIVE))));
        return CorfuProtocolLogReplication.getLrEntryMsg(ByteString.copyFrom(
                CorfuProtocolLogReplication.generatePayload(Collections.singletonList(opaque))),
                LogReplicationEntryMetadataMsg.newBuilder().setSnapshotLifecycleVersion(1)
                        .setEntryType(LogReplicationEntryType.SNAPSHOT_MESSAGE).setTopologyConfigID(1)
                        .setSyncRequestId(state.getAttemptId()).setAttemptGeneration(state.getGeneration())
                        .setSnapshotTimestamp(100).setSnapshotSyncSeqNum(0).build());
    }

    private LogReplicationEntryMsg delta(SnapshotSyncLeaseRecord state, long timestamp) {
        OpaqueEntry opaque = new OpaqueEntry(timestamp, Map.of(stream, Collections.singletonList(
                new SMREntry("put", new Object[] {"key", "delta-" + timestamp}, Serializers.PRIMITIVE))));
        return CorfuProtocolLogReplication.getLrEntryMsg(ByteString.copyFrom(
                CorfuProtocolLogReplication.generatePayload(Collections.singletonList(opaque))),
                data(state).getMetadata().toBuilder().setEntryType(LogReplicationEntryType.LOG_ENTRY_MESSAGE)
                        .setTimestamp(timestamp).setPreviousTimestamp(timestamp - 1).build());
    }

    private StreamsSnapshotWriter writer(SnapshotSyncLeaseRecord state, LogReplicationMetadataManager manager) {
        StreamsSnapshotWriter writer = new StreamsSnapshotWriter(rt, config, manager);
        writer.reset(1, 100);
        writer.setLeaseContext(state);
        return writer;
    }

    private void await(java.util.function.BooleanSupplier condition) throws Exception {
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!condition.getAsBoolean() && System.nanoTime() < limit) { Thread.sleep(10); }
        assertTrue("sink lifecycle did not reach the expected state", condition.getAsBoolean());
    }

    /** A sink whose lease table has no record yet, as after an upgrade from the pre-lease version. */
    private LogReplicationSinkManager upgradedSink() {
        leases.update((txn, current) -> SnapshotSyncLeaseRecord.getDefaultInstance());
        LogReplicationSinkManager sink = new LogReplicationSinkManager(rt, config, metadata, new DefaultSnapshotSyncPlugin(rt));
        sink.configureSnapshotLifecycle(new SnapshotLeaseCoordinator.Timing(60000, 0, 5000, 0, 3));
        sink.updateTopologyConfigId(1);
        return sink;
    }

    private LogReplicationSinkManager ownedSink() throws Exception {
        LogReplicationSinkManager sink = upgradedSink();
        sink.setLeadership(true);
        await(() -> sink.getSnapshotLease().getPhase() == SnapshotSyncLeaseRecord.Phase.READY);
        return sink;
    }

    /** Emulates a compaction service that is configured: it creates this record on its first pass. */
    private void compactorIsConfigured() {
        cycle(CheckpointingStatus.StatusType.IDLE, -1);
    }

    private SnapshotSyncLeaseRecord start(LogReplicationSinkManager sink) throws Exception {
        LogReplicationEntryMsg start = LogReplicationEntryMsg.newBuilder().setMetadata(LogReplicationEntryMetadataMsg.newBuilder()
                .setSnapshotLifecycleVersion(1).setEntryType(LogReplicationEntryType.SNAPSHOT_START)
                .setTopologyConfigID(1).setSnapshotTimestamp(100).setSyncRequestId(getUuidMsg(UUID.randomUUID()))
                .setAdmissionEpoch(sink.getSnapshotLease().getAdmissionEpoch())).build();
        // What a source does: it is told to come back while the sink is busy, reserves and prepares,
        // and repeats the same proposal until it is accepted.
        long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true) {
            try {
                assertEquals(LogReplicationEntryType.SNAPSHOT_START_ACCEPTED, sink.receive(start).getMetadata().getEntryType());
                return sink.getSnapshotLease();
            } catch (org.corfudb.runtime.exceptions.LogReplicationBusyException busy) {
                assertEquals(LogReplicationBusyResponseMsg.Reason.ADMISSION_CLOSED, busy.getResponse().getReason());
                assertTrue("the proposal was never accepted: " + sink.getSnapshotLease(), System.nanoTime() < limit);
                TimeUnit.MILLISECONDS.sleep(20);
            }
        }
    }

    @Test
    public void sinkAcceptsBeforeBulkAndReconcilesDuplicateEndAfterApply() throws Exception {
        compactorIsConfigured(); // Recovery stays pending, so incremental sync is shown to resume during it.
        LogReplicationSinkManager sink = ownedSink();
        try {
            metadata.setDataConsistentOnStandby(true);
            PersistentCorfuTable<String, String> table = getNewRuntime(getDefaultNode()).connect().getObjectsView().build().setStreamName(streamName)
                    .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {}).setSerializer(Serializers.PRIMITIVE).open();
            SnapshotSyncLeaseRecord captured = start(sink);
            assertThrows(org.corfudb.runtime.exceptions.LogReplicationBusyException.class, () -> sink.receive(data(captured)
                    .toBuilder().setMetadata(data(captured).getMetadata().toBuilder().setSnapshotLifecycleVersion(0)).build()));
            assertThrows(org.corfudb.runtime.exceptions.LogReplicationBusyException.class, () -> sink.receive(data(captured)
                    .toBuilder().setMetadata(data(captured).getMetadata().toBuilder().setTopologyConfigID(2)).build()));
            assertThrows(org.corfudb.runtime.exceptions.LogReplicationBusyException.class, () -> sink.receive(data(captured)
                    .toBuilder().setMetadata(data(captured).getMetadata().toBuilder().setAttemptGeneration(99)).build()));
            sink.receive(data(captured));
            // Transfer only writes shadow streams: the regular streams stay readable and consistent.
            assertTrue(metadata.getDataConsistentOnStandby().get("sink").getDataConsistent());
            LogReplicationEntryMsg end = data(captured).toBuilder().clearData().setMetadata(data(captured).getMetadata().toBuilder()
                    .setEntryType(LogReplicationEntryType.SNAPSHOT_END).setSnapshotSyncSeqNum(1)).build();
            assertEquals(LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE, sink.receive(end).getMetadata().getEntryType());
            await(() -> sink.getSnapshotLease().getOutcome() == SnapshotSyncLeaseRecord.Outcome.COMPLETED);
            assertEquals("value", table.get("key"));
            assertEquals(1, sink.receive(end).getMetadata().getSnapshotSyncSeqNum());
            await(() -> sink.getSnapshotLease().getPhase() == SnapshotSyncLeaseRecord.Phase.RECOVERING);
            assertFalse(sink.getSnapshotLease().getProtectionHeld());
            await(sink::isIncrementalSyncAdmitted);
            sink.receive(delta(captured, 101));
            assertEquals("delta-101", table.get("key"));
            assertTrue(metadata.getDataConsistentOnStandby().get("sink").getDataConsistent());
            assertThrows(org.corfudb.runtime.exceptions.LogReplicationBusyException.class, () -> sink.receive(data(captured)));
        } finally { sink.shutdown(); }
    }

    @Test
    public void sinkTransferFailureAbandonsAndReleasesWithoutWaitingForSource() throws Exception {
        compactorIsConfigured();
        LogReplicationSinkManager sink = ownedSink();
        try {
            // A sink that never completed a snapshot admits incremental traffic at the lease level,
            // exactly as the pre-lease sink did; the incremental writer's own validation rejects it.
            await(sink::isIncrementalSyncAdmitted);
            sink.receive(delta(SnapshotSyncLeaseRecord.getDefaultInstance(), 101));
            assertEquals(-1, metadata.getLastProcessedLogEntryBatchTimestamp());

            metadata.setDataConsistentOnStandby(true);
            SnapshotSyncLeaseRecord captured = start(sink);
            assertThrows("incremental traffic is closed while a snapshot is in flight",
                    org.corfudb.runtime.exceptions.LogReplicationBusyException.class, () -> sink.receive(delta(captured, 101)));
            StreamsSnapshotWriter failing = mock(StreamsSnapshotWriter.class);
            doThrow(new IllegalStateException("shadow write unavailable")).when(failing).apply(any(LogReplicationEntryMsg.class));
            sink.setSnapshotWriter(failing);
            assertThrows(org.corfudb.runtime.exceptions.LogReplicationBusyException.class, () -> sink.receive(data(captured)));
            await(() -> sink.getSnapshotLease().getPhase() == SnapshotSyncLeaseRecord.Phase.RECOVERING);
            assertEquals(SnapshotSyncLeaseRecord.Outcome.ABORTED, sink.getSnapshotLease().getOutcome());
            assertEquals(1, sink.getSnapshotLease().getConsecutiveAborts());
            assertFalse(sink.getSnapshotLease().getProtectionHeld());
            assertFalse(helper.isCheckpointFrozen());
            // The failure happened before apply touched the regular streams: they are still consistent.
            assertTrue(metadata.getDataConsistentOnStandby().get("sink").getDataConsistent());
            assertThrows(org.corfudb.runtime.exceptions.LogReplicationBusyException.class, () -> sink.receive(delta(captured, 101)));
        } finally { sink.shutdown(); }
    }

    @Test
    public void aRestartedSinkAbandonsTheUnfinishedAttemptAndPreservesItsBudgetAndRecoveryDebt() throws Exception {
        compactorIsConfigured();
        SnapshotSyncLeaseRecord prior = transferring();
        LogReplicationSinkManager sink = new LogReplicationSinkManager(rt, config, metadata, new DefaultSnapshotSyncPlugin(rt));
        try {
            SnapshotLeaseCoordinator.Timing timing = new SnapshotLeaseCoordinator.Timing(60000, 0, 5000, 0, 3);
            sink.configureSnapshotLifecycle(timing);
            sink.updateTopologyConfigId(1);
            assertEquals(SnapshotSyncLeaseRecord.Phase.NOT_READY, sink.getSnapshotLease().getPhase());
            sink.setLeadership(true);
            assertThrows("a live driver is never swapped out", IllegalStateException.class,
                    () -> sink.configureSnapshotLifecycle(timing));
            await(() -> sink.getSnapshotLease().getPhase() == SnapshotSyncLeaseRecord.Phase.RECOVERING);
            assertFalse(sink.getSnapshotLease().getProtectionHeld());
            assertFalse(helper.isCheckpointFrozen());
            assertEquals(SnapshotSyncLeaseRecord.Outcome.ABORTED, sink.getSnapshotLease().getOutcome());
            assertEquals(prior.getGeneration(), sink.getSnapshotLease().getGeneration());
            assertEquals(prior.getDeadlineMs(), sink.getSnapshotLease().getDeadlineMs());
            assertEquals(prior.getProtectionId(), sink.getSnapshotLease().getProtectionId());
            assertNotEquals(prior.getOwnerId(), sink.getSnapshotLease().getOwnerId());
        } finally { sink.shutdown(); }
    }

    @Test
    public void independentCheckpointAndTrimRecoverAnAbandonedSinkWithoutSourcePolling() throws Exception {
        compactorIsConfigured();
        LogReplicationSinkManager sink = ownedSink();
        try {
            SnapshotSyncLeaseRecord captured = start(sink);
            sink.receive(data(captured));
            LogReplicationEntryMsg cancel = data(captured).toBuilder().clearData().setMetadata(data(captured).getMetadata().toBuilder()
                    .setEntryType(LogReplicationEntryType.SNAPSHOT_CANCEL)).build();
            assertThrows(org.corfudb.runtime.exceptions.LogReplicationBusyException.class, () -> sink.receive(cancel));
            await(() -> sink.getSnapshotLease().getPhase() == SnapshotSyncLeaseRecord.Phase.RECOVERING);
            long recoveryCut = sink.getSnapshotLease().getRecoveryCut();
            assertFalse(helper.isCheckpointFrozen());
            CompactorLeaderServices compactor = new CompactorLeaderServices(rt, "test", metadata.getCorfuStore(), mock(LivenessValidator.class));
            assertEquals(CompactorLeaderServices.LeaderInitStatus.SUCCESS, compactor.initCompactionCycle());
            // A failed real cycle must leave the snapshot gate closed even after cleanup.
            compactor.finishCompactionCycle();
            assertEquals(SnapshotSyncLeaseRecord.Phase.RECOVERING, sink.getSnapshotLease().getPhase());
            checkpointAndTrim(compactor);
            assertTrue(rt.getAddressSpaceView().getTrimMark().getSequence() > recoveryCut);
            await(() -> sink.getSnapshotLease().getPhase() == SnapshotSyncLeaseRecord.Phase.READY);
            assertEquals(captured.getDeadlineMs(), sink.getSnapshotLease().getDeadlineMs());
            assertEquals(SnapshotSyncLeaseRecord.Outcome.ABORTED, sink.getSnapshotLease().getOutcome());
            SnapshotSyncLeaseRecord next = start(sink);
            assertEquals(captured.getGeneration() + 1, next.getGeneration());
        } finally { sink.shutdown(); }
    }

    /** One real, successful compaction cycle followed by the trim it allows. */
    private void checkpointAndTrim(CompactorLeaderServices compactor) throws Exception {
        assertEquals(CompactorLeaderServices.LeaderInitStatus.SUCCESS, compactor.initCompactionCycle());
        CorfuRuntime cpRuntime = getNewRuntime(getDefaultNode()).connect();
        ServerTriggeredCheckpointer checkpointer = new ServerTriggeredCheckpointer(CheckpointerBuilder.builder()
                .corfuRuntime(rt).cpRuntime(java.util.Optional.of(cpRuntime)).isClient(false)
                .persistedCacheRoot(java.util.Optional.empty()).build(), metadata.getCorfuStore(), helper.getCompactorMetadataTables());
        try { checkpointer.checkpointTables(); } finally { checkpointer.shutdown(); }
        compactor.finishCompactionCycle();
        new TrimLog().invokePrefixTrim(rt, metadata.getCorfuStore());
    }

    /** The state a sink is in after the pre-lease protocol fully applied snapshot 100. */
    private void legacySnapshotCompleted() {
        try (TxnContext txn = metadata.getTxnContext()) {
            for (LogReplicationMetadataManager.LogReplicationMetadataType type : new LogReplicationMetadataManager.LogReplicationMetadataType[]{
                    LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_STARTED,
                    LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED,
                    LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_APPLIED,
                    LogReplicationMetadataManager.LogReplicationMetadataType.LAST_LOG_ENTRY_BATCH_PROCESSED,
                    LogReplicationMetadataManager.LogReplicationMetadataType.LAST_LOG_ENTRY_APPLIED}) {
                metadata.appendUpdate(txn, type, 100);
            }
            txn.commit();
        }
    }

    private PersistentCorfuTable<String, String> replicatedTable() {
        return getNewRuntime(getDefaultNode()).connect().getObjectsView().build().setStreamName(streamName)
                .setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {}).setSerializer(Serializers.PRIMITIVE).open();
    }

    private LogReplicationEntryMsg startMessage(LogReplicationSinkManager sink) {
        return startMessage(sink, 100);
    }

    private LogReplicationEntryMsg startMessage(LogReplicationSinkManager sink, long sourceSnapshot) {
        return LogReplicationEntryMsg.newBuilder().setMetadata(LogReplicationEntryMetadataMsg.newBuilder()
                .setSnapshotLifecycleVersion(1).setEntryType(LogReplicationEntryType.SNAPSHOT_START)
                .setTopologyConfigID(1).setSnapshotTimestamp(sourceSnapshot).setSyncRequestId(getUuidMsg(UUID.randomUUID()))
                .setAdmissionEpoch(sink.getSnapshotLease().getAdmissionEpoch())).build();
    }

    /**
     * A snapshot timestamp is a position in the source's log, which starts over when the source is
     * rebuilt or restored. A proposal below the previous position is not stale: stale proposals are
     * fenced by the admission epoch. Refusing it would close replication for good, silently.
     */
    @Test
    public void aSourceWhoseLogStartedOverIsStillAdmitted() throws Exception {
        legacySnapshotCompleted(); // Snapshot 100 of the previous incarnation of the source.
        LogReplicationSinkManager sink = ownedSink();
        try {
            LogReplicationEntryMsg proposal = startMessage(sink, 40);
            long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            LogReplicationEntryMsg reply = null;
            while (reply == null) {
                try {
                    reply = sink.receive(proposal);
                } catch (org.corfudb.runtime.exceptions.LogReplicationBusyException busy) {
                    assertTrue("never admitted: " + sink.getSnapshotLease(), System.nanoTime() < limit);
                    TimeUnit.MILLISECONDS.sleep(20);
                }
            }
            assertEquals(LogReplicationEntryType.SNAPSHOT_START_ACCEPTED, reply.getMetadata().getEntryType());
            assertEquals(40, sink.getSnapshotLease().getSourceSnapshot());
            assertEquals(40, metadata.getLastStartedSnapshotTimestamp());
        } finally { sink.shutdown(); }
    }

    // ------------------------------------------------------------ upgrade from the pre-lease version

    /**
     * The sink cluster is upgraded while the pre-lease protocol is in the middle of a snapshot
     * sync: the checkpointer is frozen by the freeze token, and the hook that would delete the token
     * is never called again. This is the state the original incident left clusters in.
     */
    @Test
    public void upgradeSupersedesAPendingPreLeaseSnapshotAndUnfreezesTheCheckpointer() throws Exception {
        compactorIsConfigured();
        try (TxnContext txn = metadata.getTxnContext()) {
            // Snapshot 80 was started by the pre-lease protocol and never applied.
            metadata.appendUpdate(txn, LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_STARTED, 80);
            txn.commit();
        }
        helper.freezeCompaction();
        assertTrue(helper.isCheckpointFrozen());
        LogReplicationSinkManager sink = upgradedSink();
        try {
            sink.setLeadership(true);
            await(() -> sink.getSnapshotLease().getPhase() == SnapshotSyncLeaseRecord.Phase.RECOVERING);
            SnapshotSyncLeaseRecord seeded = sink.getSnapshotLease();
            assertEquals(SnapshotSyncLeaseRecord.Outcome.ABORTED, seeded.getOutcome());
            assertEquals(80, seeded.getSourceSnapshot());
            assertFalse(seeded.getProtectionHeld());
            assertTrue(seeded.getRecoveryCut() >= 0);
            assertFalse("the leftover freeze token is gone", helper.isCheckpointFrozen());
            assertFalse(sink.isIncrementalSyncAdmitted());

            // Another takeover finds the same unfinished snapshot: it is not superseded a second time.
            sink.setLeadership(false);
            sink.setLeadership(true);
            await(() -> sink.getSnapshotLease().getPhase() == SnapshotSyncLeaseRecord.Phase.RECOVERING);
            assertEquals(seeded.getRecoveryCut(), sink.getSnapshotLease().getRecoveryCut());
            assertEquals(seeded.getFailure(), sink.getSnapshotLease().getFailure());

            // Admission reopens only after one checkpoint and trim reclaimed what the old attempt left.
            assertThrows(org.corfudb.runtime.exceptions.LogReplicationBusyException.class, () -> sink.receive(startMessage(sink)));
            assertEquals(SnapshotSyncLeaseRecord.Phase.RECOVERING, sink.getSnapshotLease().getPhase());
            checkpointAndTrim(new CompactorLeaderServices(rt, "test", metadata.getCorfuStore(), mock(LivenessValidator.class)));
            await(() -> sink.getSnapshotLease().getPhase() == SnapshotSyncLeaseRecord.Phase.READY);
            assertEquals(SnapshotSyncLeaseRecord.Outcome.ABORTED, sink.getSnapshotLease().getOutcome());
            assertEquals(1, start(sink).getGeneration());
        } finally { sink.shutdown(); }
    }

    /** The common case: the sink is upgraded while it is in incremental sync. Nothing is restarted. */
    @Test
    public void upgradeOfAnIdleSinkResumesIncrementalSyncWithoutASnapshot() throws Exception {
        legacySnapshotCompleted();
        helper.freezeCompaction(); // An operator's freeze: no pre-lease snapshot is pending, so it is not LR's to delete.
        PersistentCorfuTable<String, String> table = replicatedTable();
        LogReplicationSinkManager sink = upgradedSink();
        try {
            sink.setLeadership(true);
            await(sink::isIncrementalSyncAdmitted);
            SnapshotSyncLeaseRecord seeded = sink.getSnapshotLease();
            assertEquals(SnapshotSyncLeaseRecord.Phase.READY, seeded.getPhase());
            assertEquals(SnapshotSyncLeaseRecord.Outcome.COMPLETED, seeded.getOutcome());
            assertEquals(0, seeded.getGeneration());
            assertEquals(100, seeded.getSourceSnapshot());
            assertFalse(seeded.getProtectionHeld());
            assertTrue("an operator freeze is left to the compactor's own patience", helper.isCheckpointFrozen());

            sink.receive(delta(seeded, 101));
            assertEquals("delta-101", table.get("key"));
            assertEquals(101, metadata.getLastProcessedLogEntryBatchTimestamp());

            // Leadership moves away and back: positions are re-read, nothing is replayed or lost.
            sink.setLeadership(false);
            assertThrows(org.corfudb.runtime.exceptions.LogReplicationBusyException.class, () -> sink.receive(delta(seeded, 102)));
            sink.setLeadership(true);
            await(sink::isIncrementalSyncAdmitted);
            sink.receive(delta(seeded, 102));
            assertEquals("delta-102", table.get("key"));
        } finally { sink.shutdown(); }
    }

    // ------------------------------------------------------------ the freeze token

    /**
     * The token is no longer written by log replication, but operators, tools and plugins can still
     * write it. Its patience is what bounds it, and it used to restart with every repeated freeze:
     * a caller that kept asking kept checkpointing frozen for as long as it kept asking.
     */
    @Test
    public void aRepeatedFreezeDoesNotRestartThePatienceThatBoundsIt() throws Exception {
        helper.freezeCompaction();
        long since = freezeToken().getSequence();
        TimeUnit.MILLISECONDS.sleep(20);
        helper.freezeCompaction();
        assertEquals(since, freezeToken().getSequence());
        assertTrue(helper.isCheckpointFrozen());

        // Extending a freeze on purpose stays possible, and explicit.
        helper.unfreezeCompaction();
        assertFalse(helper.isCheckpointFrozen());
        helper.freezeCompaction();
        assertTrue(freezeToken().getSequence() > since);
    }

    private RpcCommon.TokenMsg freezeToken() {
        try (TxnContext txn = metadata.getTxnContext()) {
            RpcCommon.TokenMsg token = (RpcCommon.TokenMsg) txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE,
                    CompactorMetadataTables.FREEZE_TOKEN).getPayload();
            txn.commit();
            return token;
        }
    }

    // ------------------------------------------------------------ the lease and the compactor

    /**
     * The lease is one record shared with the compactor, whose cycle start writes it. If the
     * incremental writer rewrote it for every replicated transaction, a cycle could practically
     * never start on a sink under steady replication, and the log would grow without bound on the
     * normal path.
     */
    @Test
    public void aCycleStartIsNotAbortedByIncrementalReplication() throws Exception {
        legacySnapshotCompleted();
        PersistentCorfuTable<String, String> table = replicatedTable();
        SnapshotSyncLeaseRecord idle = leases.read();
        LogEntryWriter incremental = new LogEntryWriter(config, metadata);
        incremental.setLeaseContext(idle);
        org.corfudb.runtime.collections.CorfuStore racing = spy(metadata.getCorfuStore());
        doAnswer(call -> {
            // A replicated transaction commits while the cycle start is in flight.
            assertTrue(CompletableFuture.supplyAsync(() -> incremental.apply(delta(idle, 101))).get(10, TimeUnit.SECONDS));
            return metadata.getCorfuStore().listTables(null);
        }).when(racing).listTables(null);
        CompactorLeaderServices leader = new CompactorLeaderServices(rt, "test", racing, mock(LivenessValidator.class));
        assertEquals(CompactorLeaderServices.LeaderInitStatus.SUCCESS, leader.initCompactionCycle());
        assertEquals("delta-101", table.get("key"));
        assertEquals(idle, leases.read());
    }

    /** The other half of that design: admission still fences an incremental write that is in flight. */
    @Test
    public void snapshotAdmissionFencesAnIncrementalTransactionThatAlreadyValidatedTheLease() throws Exception {
        legacySnapshotCompleted();
        PersistentCorfuTable<String, String> table = replicatedTable();
        SnapshotSyncLeaseRecord idle = leases.read();
        LogReplicationMetadataManager observed = spy(metadata);
        java.util.concurrent.atomic.AtomicBoolean raced = new java.util.concurrent.atomic.AtomicBoolean();
        doAnswer(call -> {
            call.callRealMethod();
            if (raced.compareAndSet(false, true)) {
                CompletableFuture.runAsync(this::reserve).get(10, TimeUnit.SECONDS);
            }
            return null;
        }).when(observed).touch(any(TxnContext.class), any());
        LogEntryWriter incremental = new LogEntryWriter(config, observed);
        incremental.setLeaseContext(idle);

        // Two replicated transactions in one message. The first one is not the last of its batch,
        // so the only key it shares with the admission transaction is TOPOLOGY_CONFIG_ID.
        java.util.List<OpaqueEntry> batch = new java.util.ArrayList<>();
        for (long version : new long[]{101, 102}) {
            batch.add(new OpaqueEntry(version, Map.of(stream, Collections.singletonList(
                    new SMREntry("put", new Object[] {"key", "delta-" + version}, Serializers.PRIMITIVE)))));
        }
        LogReplicationEntryMsg message = CorfuProtocolLogReplication.getLrEntryMsg(ByteString.copyFrom(
                CorfuProtocolLogReplication.generatePayload(batch)), delta(idle, 102).getMetadata().toBuilder()
                .setPreviousTimestamp(100).build());

        assertFalse(incremental.apply(message));
        assertTrue(raced.get());
        assertNull(table.get("key"));
        assertEquals(SnapshotSyncLeaseRecord.Phase.PREPARING, leases.read().getPhase());
    }

    /**
     * The backstop that does not depend on the sink. A sink that is hung, or has no leader, never
     * releases its protection; the checkpointer stops honoring it once it is past its deadline plus
     * the grace, and nothing of that attempt can commit any more.
     */
    @Test
    public void protectionThatOutlivedItsDeadlineAndGraceNoLongerFreezesTheCheckpointer() throws Exception {
        long grace = SnapshotSyncLeaseStore.expiryGraceMs();
        long now = System.currentTimeMillis();
        reserve();
        SnapshotSyncLeaseRecord captured = leases.updateOwned("owner", (txn, current) -> SnapshotSyncLease.prepared(current));
        CompactorLeaderServices leader = new CompactorLeaderServices(rt, "test", metadata.getCorfuStore(), mock(LivenessValidator.class));

        // Past the deadline but inside the grace: the sink is still expected to release it.
        SnapshotSyncLeaseRecord late = captured.toBuilder().setAdmittedAtMs(now - grace).setDeadlineMs(now - 1000).build();
        leases.update((txn, current) -> late);
        assertTrue(helper.isCheckpointFrozen());
        assertEquals(CompactorLeaderServices.LeaderInitStatus.FAIL, leader.initCompactionCycle());

        // Past the grace as well.
        SnapshotSyncLeaseRecord expired = late.toBuilder().setAdmittedAtMs(now - 2 * grace - 2000).setDeadlineMs(now - grace - 1000).build();
        leases.update((txn, current) -> expired);
        assertFalse(helper.isCheckpointFrozen());
        assertThrows(SnapshotSyncLease.LeaseRejectedException.class, () -> writer(expired, metadata).apply(data(expired)));
        long before = rt.getAddressSpaceView().getTrimMark().getSequence();
        checkpointAndTrim(leader);
        assertTrue(rt.getAddressSpaceView().getTrimMark().getSequence() > before);
        assertTrue("the record itself is the sink's to settle", leases.read().getProtectionHeld());
    }

    private void cycle(CheckpointingStatus.StatusType status, long cutoff) {
        CompactorMetadataTables tables = helper.getCompactorMetadataTables();
        try (TxnContext txn = metadata.getTxnContext()) {
            txn.putRecord(tables.getCompactionManagerTable(), CompactorMetadataTables.COMPACTION_MANAGER_KEY,
                    CheckpointingStatus.newBuilder().setStatus(status).build(), null);
            txn.putRecord(tables.getCompactionControlsTable(), CompactorMetadataTables.MIN_CHECKPOINT,
                    RpcCommon.TokenMsg.newBuilder().setSequence(cutoff).build(), null);
            txn.commit();
        }
    }

    @Test
    public void shadowTransferAndApplyCommitIdentityProgressAndConsistencyTogether() {
        PersistentCorfuTable<String, String> table = getNewRuntime(getDefaultNode()).connect().getObjectsView().build()
                .setStreamName(streamName).setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setSerializer(Serializers.PRIMITIVE).open();
        table.insert("obsolete", "old");
        SnapshotSyncLeaseRecord captured = transferring();
        StreamsSnapshotWriter writer = writer(captured, metadata);
        writer.apply(data(captured));
        SnapshotSyncLeaseRecord transferred = leases.read();
        assertEquals(0, transferred.getTransferredSequence());
        assertTrue(transferred.getFirstShadowAddress() > transferred.getProtectedAfter());
        assertEquals("old", table.get("obsolete"));
        metadata.setDataConsistentOnStandby(true);
        SnapshotSyncLeaseRecord applying = leases.updateOwned("owner", (txn, current) -> {
            metadata.transferSnapshot(txn, 100);
            return SnapshotSyncLease.transferred(current);
        });
        // Apply is about to rewrite the regular streams: from here on readers must not trust them.
        assertFalse(metadata.getDataConsistentOnStandby().get("sink").getDataConsistent());
        writer.setLeaseContext(applying);
        writer.clearLocalStreams();
        writer.startSnapshotSyncApply();
        metadata.setSnapshotAppliedComplete(data(applying), applying);
        assertEquals("value", table.get("key"));
        assertNull(table.get("obsolete"));
        assertEquals(SnapshotSyncLeaseRecord.Outcome.COMPLETED, leases.read().getOutcome());
        assertTrue(leases.read().getProtectionHeld());
        metadata.refreshSnapshotStatus();
        assertEquals(100, metadata.getCachedSnapshotStatus().getSnapshotApplied());
        assertEquals(leases.read(), metadata.getCachedSnapshotStatus().getSnapshotLease());
        assertTrue(metadata.getDataConsistentOnStandby().get("sink").getDataConsistent());
        LogEntryWriter incremental =
                new LogEntryWriter(config, metadata);
        incremental.setLeaseContext(leases.read());
        assertTrue(incremental.apply(delta(applying, 101)));
        assertEquals("delta-101", table.get("key"));
        leases.update((txn, current) -> SnapshotSyncLease.next(current).setOwnerId("successor").build());
        assertFalse(incremental.apply(delta(applying, 102)));
        assertEquals("delta-101", table.get("key"));
    }

    @Test
    public void partialMultiBatchApplyCanRetryWithoutDuplicatingOrLosingData() {
        PersistentCorfuTable<String, String> table = getNewRuntime(getDefaultNode()).connect().getObjectsView().build()
                .setStreamName(streamName).setTypeToken(new TypeToken<PersistentCorfuTable<String, String>>() {})
                .setSerializer(Serializers.PRIMITIVE).open();
        table.insert("obsolete", "old");
        SnapshotSyncLeaseRecord captured = transferring();
        LogReplicationMetadataManager observed = spy(metadata);
        CorfuRuntime batchingRuntime = mock(CorfuRuntime.class);
        when(batchingRuntime.getParameters()).thenReturn(CorfuRuntime.CorfuRuntimeParameters.builder().maxWriteSize(256).build());
        doReturn(batchingRuntime).when(observed).getRuntime();
        StreamsSnapshotWriter writer = writer(captured, observed);
        java.util.List<SMREntry> entries = new java.util.ArrayList<>();
        for (int i = 0; i < 8; i++) {
            entries.add(new SMREntry("put", new Object[]{"key-" + i, "value-" + i + "x".repeat(40)}, Serializers.PRIMITIVE));
        }
        writer.apply(data(captured).toBuilder().setData(ByteString.copyFrom(CorfuProtocolLogReplication.generatePayload(
                Collections.singletonList(new OpaqueEntry(100, Map.of(stream, entries)))))).build());
        SnapshotSyncLeaseRecord applying = leases.updateOwned("owner", (txn, current) -> {
            metadata.transferSnapshot(txn, 100);
            return SnapshotSyncLease.transferred(current);
        });
        writer.setLeaseContext(applying);
        AtomicInteger transactions = new AtomicInteger();
        doAnswer(call -> {
            if (transactions.incrementAndGet() == 3) { throw new IllegalStateException("temporary apply failure"); }
            return metadata.getTxnContext();
        }).when(observed).getTxnContext();
        writer.clearLocalStreams();
        assertThrows(IllegalStateException.class, writer::startSnapshotSyncApply);
        assertTrue(table.size() > 0 && table.size() < 8);
        assertFalse(metadata.getDataConsistentOnStandby().get("sink").getDataConsistent());
        assertEquals(SnapshotSyncLeaseRecord.Outcome.NONE, leases.read().getOutcome());
        doAnswer(call -> metadata.getTxnContext()).when(observed).getTxnContext();
        writer.setLeaseContext(applying);
        writer.clearLocalStreams();
        writer.startSnapshotSyncApply();
        metadata.setSnapshotAppliedComplete(data(applying), applying);
        assertEquals(8, table.size());
        for (int i = 0; i < 8; i++) { assertEquals("value-" + i + "x".repeat(40), table.get("key-" + i)); }
        assertNull(table.get("obsolete"));
        assertEquals(applying.getDeadlineMs(), leases.read().getDeadlineMs());
        assertTrue(metadata.getDataConsistentOnStandby().get("sink").getDataConsistent());
    }

    @Test
    public void interruptedExpiredAndMismatchedWritersRejectBeforeMutating() {
        SnapshotSyncLeaseRecord captured = transferring();
        StreamsSnapshotWriter current = writer(captured, metadata);
        assertThrows(SnapshotSyncLease.LeaseRejectedException.class, () -> current.apply(data(captured).toBuilder()
                .setMetadata(data(captured).getMetadata().toBuilder().setSyncRequestId(getUuidMsg(UUID.randomUUID()))).build()));
        Thread.currentThread().interrupt();
        try {
            assertThrows(SnapshotSyncLease.LeaseRejectedException.class, () -> current.apply(data(captured)));
        } finally { Thread.interrupted(); }
        StreamsSnapshotWriter expired = writer(captured.toBuilder().setDeadlineMs(1).build(), metadata);
        assertThrows(SnapshotSyncLease.LeaseRejectedException.class, () -> expired.apply(data(captured)));
        try (TxnContext txn = metadata.getTxnContext()) {
            metadata.appendUpdate(txn, LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_STARTED, 101);
            txn.commit();
        }
        assertThrows(SnapshotSyncLease.LeaseRejectedException.class, () -> current.apply(data(captured)));
        assertEquals(-1, leases.read().getTransferredSequence());
    }

    /**
     * The sequencer refuses a transaction (a conflict, or a sequencer that failed over) before
     * anything reaches the log. The batch is certainly not written and the writer has not moved on,
     * so the same message can simply be processed again: no reason to abandon the transfer.
     */
    @Test
    public void aShadowWriteTheSequencerRefusedLeavesNothingBehindAndCanBeRepeated() {
        SnapshotSyncLeaseRecord captured = transferring();
        LogReplicationMetadataManager observed = spy(metadata);
        java.util.concurrent.atomic.AtomicBoolean raced = new java.util.concurrent.atomic.AtomicBoolean();
        doAnswer(call -> {
            call.callRealMethod();
            if (raced.compareAndSet(false, true)) {
                // Something else commits a write of the lease while this transaction is in flight.
                CompletableFuture.runAsync(() -> leases.update((txn, current) -> SnapshotSyncLease.next(current).build()))
                        .get(10, TimeUnit.SECONDS);
            }
            return null;
        }).when(observed).appendUpdate(any(TxnContext.class),
                eq(LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED_SEQUENCE_NUMBER), anyLong());
        StreamsSnapshotWriter writer = writer(captured, observed);

        assertThrows(StreamsSnapshotWriter.RetryableWriteException.class, () -> writer.apply(data(captured)));
        assertEquals(-1, leases.read().getTransferredSequence());
        assertEquals(-1, leases.read().getFirstShadowAddress());

        writer.apply(data(captured));
        assertEquals(0, leases.read().getTransferredSequence());
        assertTrue(leases.read().getFirstShadowAddress() > captured.getProtectedAfter());
    }

    @Test
    public void oldWriterTransactionCannotCommitAfterConcurrentAbandonment() throws Exception {
        SnapshotSyncLeaseRecord captured = transferring();
        try (TxnContext txn = metadata.getTxnContext()) {
            SnapshotSyncLeaseStore.fence(txn, captured,
                    System.currentTimeMillis(), SnapshotSyncLeaseRecord.Phase.TRANSFERRING);
            metadata.transferSnapshot(txn, 100);
            CompletableFuture.runAsync(() -> leases.updateOwned("owner", (other, current) ->
                    SnapshotSyncLease.abandon(current, System.currentTimeMillis(), "deadline"))).get(10, TimeUnit.SECONDS);
            assertThrows(TransactionAbortedException.class, txn::commit);
        }
        assertEquals(-1, metadata.queryMetadata(LogReplicationMetadataManager.LogReplicationMetadataType.LAST_SNAPSHOT_TRANSFERRED));
        assertThrows(SnapshotSyncLease.LeaseRejectedException.class, () -> writer(captured, metadata).apply(data(captured)));
    }

    @Test
    public void markerFailureCannotBeMistakenForAResumableTransfer() {
        SnapshotSyncLeaseRecord captured = transferring();
        LogReplicationMetadataManager faulted = spy(metadata);
        AtomicInteger calls = new AtomicInteger();
        doAnswer(call -> {
            if (calls.incrementAndGet() == 2) { throw new IllegalStateException("marker transaction unavailable"); }
            return metadata.getTxnContext();
        }).when(faulted).getTxnContext();
        assertThrows(IllegalStateException.class, () -> writer(captured, faulted).apply(data(captured)));
        assertEquals(0, leases.read().getTransferredSequence());
        assertEquals(-1, leases.read().getFirstShadowAddress());
        assertThrows(SnapshotSyncLease.LeaseRejectedException.class, () -> SnapshotSyncLease.transferred(leases.read()));
    }

    @Test
    public void checkpointGuardAllowsOnlyAnAlreadyRunningSafeCycleAndBlocksNewCycles() throws Exception {
        SnapshotSyncLeaseRecord captured = reserve();
        assertTrue(helper.isCheckpointFrozen());
        cycle(CheckpointingStatus.StatusType.STARTED, captured.getProtectedAfter());
        assertFalse(helper.isCheckpointFrozen());
        cycle(CheckpointingStatus.StatusType.STARTED, captured.getProtectedAfter() + 1);
        assertTrue(helper.isCheckpointFrozen());
        cycle(CheckpointingStatus.StatusType.COMPLETED, captured.getProtectedAfter());
        assertTrue(helper.isCheckpointFrozen());
        CompactorLeaderServices leader = new CompactorLeaderServices(rt, "test", metadata.getCorfuStore(), mock(LivenessValidator.class));
        assertEquals(CompactorLeaderServices.LeaderInitStatus.FAIL, leader.initCompactionCycle());
    }

    @Test
    public void compactorLosesAdmissionRaceAfterReadingAnUnprotectedLease() throws Exception {
        org.corfudb.runtime.collections.CorfuStore racing = spy(metadata.getCorfuStore());
        doAnswer(call -> {
            CompletableFuture.runAsync(this::reserve).get(10, TimeUnit.SECONDS);
            return metadata.getCorfuStore().listTables(null);
        }).when(racing).listTables(null);
        CompactorLeaderServices leader = new CompactorLeaderServices(rt, "test", racing, mock(LivenessValidator.class));
        assertEquals(CompactorLeaderServices.LeaderInitStatus.FAIL, leader.initCompactionCycle());
        assertTrue(leases.read().getProtectionHeld());
        try (TxnContext txn = metadata.getTxnContext()) {
            assertNull(txn.getRecord(CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload());
            txn.commit();
        }
    }

    @Test
    public void reservationRetriesAfterConcurrentCycleAndCapturesASafeCut() throws Exception {
        CompactorLeaderServices leader = new CompactorLeaderServices(rt, "test", metadata.getCorfuStore(), mock(LivenessValidator.class));
        AtomicInteger runs = new AtomicInteger();
        SnapshotSyncLeaseRecord admitted = leases.updateOwned("owner", (txn, current) -> {
            if (runs.incrementAndGet() == 1) {
                assertEquals(CompactorLeaderServices.LeaderInitStatus.SUCCESS,
                        CompletableFuture.supplyAsync(leader::initCompactionCycle).join());
            }
            return SnapshotSyncLease.reserve(current, LogReplicationEntryMetadataMsg.newBuilder()
                    .setSnapshotLifecycleVersion(1).setAdmissionEpoch(current.getAdmissionEpoch())
                    .setSyncRequestId(getUuidMsg(UUID.randomUUID())).build(), System.currentTimeMillis(), 60000,
                    txn.getTxnSequence(), "protection");
        });
        assertEquals(2, runs.get());
        try (TxnContext txn = metadata.getTxnContext()) {
            RpcCommon.TokenMsg cutoff = (RpcCommon.TokenMsg) txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE,
                    CompactorMetadataTables.MIN_CHECKPOINT).getPayload();
            assertTrue(cutoff.getSequence() <= admitted.getProtectedAfter());
            txn.commit();
        }
        assertFalse(helper.isCheckpointFrozen());
    }

    @Test
    public void trimHonorsProtectedBoundaryAndRequiresSuccessfulCheckpoint() {
        SnapshotSyncLeaseRecord captured = reserve();
        CorfuRuntime observedRuntime = mock(CorfuRuntime.class);
        org.corfudb.runtime.view.AddressSpaceView addressSpace = mock(org.corfudb.runtime.view.AddressSpaceView.class);
        when(observedRuntime.getAddressSpaceView()).thenReturn(addressSpace);
        TrimLog trim = new TrimLog();
        cycle(CheckpointingStatus.StatusType.FAILED, captured.getProtectedAfter());
        trim.invokePrefixTrim(observedRuntime, metadata.getCorfuStore());
        cycle(CheckpointingStatus.StatusType.COMPLETED, captured.getProtectedAfter() + 1);
        trim.invokePrefixTrim(observedRuntime, metadata.getCorfuStore());
        verifyNoInteractions(addressSpace);
        cycle(CheckpointingStatus.StatusType.COMPLETED, captured.getProtectedAfter());
        trim.invokePrefixTrim(observedRuntime, metadata.getCorfuStore());
        verify(addressSpace).prefixTrim(new org.corfudb.protocols.wireprotocol.Token(0, captured.getProtectedAfter()));
        verify(addressSpace).gc();
    }

    @Test
    public void topologyMigrationFencesWritersWithoutErasingProtectionOrDebt() {
        SnapshotSyncLeaseRecord captured = transferring();
        metadata.setupTopologyConfigId(2);
        SnapshotSyncLeaseRecord current = leases.read();
        assertEquals(SnapshotSyncLeaseRecord.Outcome.ABORTED, current.getOutcome());
        assertEquals(captured.getProtectionId(), current.getProtectionId());
        assertEquals(captured.getDeadlineMs(), current.getDeadlineMs());
        assertTrue(current.getProtectionHeld());
        assertThrows(SnapshotSyncLease.LeaseRejectedException.class, () -> writer(captured, metadata).apply(data(captured)));
    }

    @Test
    public void aLaterSchemaIsRefusedByTheLeaseDriverButStillHonoredByTheCheckpointer() {
        assertThrows(SnapshotSyncLease.LeaseRejectedException.class, () -> leases.updateOwned("old-owner", (txn, state) -> state));
        SnapshotSyncLeaseRecord future = leases.read().toBuilder().setSchemaVersion(2).build();
        try (TxnContext txn = metadata.getTxnContext()) {
            SnapshotSyncLeaseStore.write(txn, future);
            txn.commit();
        }
        // Whoever drives or follows the lease refuses a record of a later schema...
        assertThrows(SnapshotSyncLease.LeaseRejectedException.class, leases::read);
        // ...but the checkpointer keeps working from the fields every schema keeps. Refusing the
        // record here would stop compaction altogether, silently, until it is upgraded too.
        assertFalse(helper.isCheckpointFrozen());
        SnapshotSyncLeaseRecord held = future.toBuilder().setProtectionHeld(true).setProtectedAfter(5)
                .setDeadlineMs(System.currentTimeMillis() + 60000).build();
        try (TxnContext txn = metadata.getTxnContext()) {
            SnapshotSyncLeaseStore.write(txn, held);
            txn.commit();
        }
        assertTrue(helper.isCheckpointFrozen());
    }
}
