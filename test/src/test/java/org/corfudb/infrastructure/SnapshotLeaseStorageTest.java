package org.corfudb.infrastructure;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.ByteString;
import org.corfudb.infrastructure.logreplication.LogReplicationConfig;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationMetadataManager;
import org.corfudb.infrastructure.logreplication.replication.receive.StreamsSnapshotWriter;
import org.corfudb.infrastructure.logreplication.replication.receive.LogReplicationSinkManager;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.DefaultSnapshotSyncPlugin;
import org.corfudb.infrastructure.logreplication.utils.LogReplicationConfigManager;
import org.corfudb.protocols.logprotocol.OpaqueEntry;
import org.corfudb.protocols.logprotocol.SMREntry;
import org.corfudb.protocols.service.CorfuProtocolLogReplication;
import org.corfudb.runtime.*;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.LogReplication.*;
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
        leases.update((txn, state) -> SnapshotSyncLease.initial("owner"));
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

    private LogReplicationSinkManager ownedSink() throws Exception {
        leases.update((txn, current) -> SnapshotSyncLeaseRecord.getDefaultInstance());
        LogReplicationSinkManager sink = new LogReplicationSinkManager(rt, config, metadata, new DefaultSnapshotSyncPlugin(rt));
        sink.enableSnapshotLifecycle(60000, 0, 5000);
        sink.updateTopologyConfigId(1);
        sink.setLeadership(true);
        await(() -> sink.getSnapshotLease().getPhase() == SnapshotSyncLeaseRecord.Phase.READY);
        return sink;
    }

    private SnapshotSyncLeaseRecord start(LogReplicationSinkManager sink) throws Exception {
        LogReplicationEntryMsg start = LogReplicationEntryMsg.newBuilder().setMetadata(LogReplicationEntryMetadataMsg.newBuilder()
                .setSnapshotLifecycleVersion(1).setEntryType(LogReplicationEntryType.SNAPSHOT_START)
                .setTopologyConfigID(1).setSnapshotTimestamp(100).setSyncRequestId(getUuidMsg(UUID.randomUUID()))
                .setAdmissionEpoch(sink.getSnapshotLease().getAdmissionEpoch())).build();
        assertThrows(org.corfudb.runtime.exceptions.LogReplicationBusyException.class, () -> sink.receive(start));
        await(() -> sink.getSnapshotLease().getPhase() == SnapshotSyncLeaseRecord.Phase.TRANSFERRING);
        assertEquals(LogReplicationEntryType.SNAPSHOT_START_ACCEPTED, sink.receive(start).getMetadata().getEntryType());
        return sink.getSnapshotLease();
    }

    @Test
    public void sinkAcceptsBeforeBulkAndReconcilesDuplicateEndAfterApply() throws Exception {
        LogReplicationSinkManager sink = ownedSink();
        try {
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
            LogReplicationEntryMsg end = data(captured).toBuilder().clearData().setMetadata(data(captured).getMetadata().toBuilder()
                    .setEntryType(LogReplicationEntryType.SNAPSHOT_END).setSnapshotSyncSeqNum(1)).build();
            assertEquals(LogReplicationEntryType.SNAPSHOT_TRANSFER_COMPLETE, sink.receive(end).getMetadata().getEntryType());
            await(() -> sink.getSnapshotLease().getOutcome() == SnapshotSyncLeaseRecord.Outcome.COMPLETED);
            assertEquals("value", table.get("key"));
            assertEquals(1, sink.receive(end).getMetadata().getSnapshotSyncSeqNum());
            await(() -> sink.getSnapshotLease().getPhase() == SnapshotSyncLeaseRecord.Phase.RECOVERING);
            sink.receive(delta(captured, 101));
            assertEquals("delta-101", table.get("key"));
            assertTrue(metadata.getDataConsistentOnStandby().get("sink").getDataConsistent());
            assertThrows(org.corfudb.runtime.exceptions.LogReplicationBusyException.class, () -> sink.receive(data(captured)));
        } finally { sink.shutdown(); }
    }

    @Test
    public void sinkTransferFailureAbandonsAndReleasesWithoutWaitingForSource() throws Exception {
        LogReplicationSinkManager sink = ownedSink();
        try {
            assertThrows(org.corfudb.runtime.exceptions.LogReplicationBusyException.class,
                    () -> sink.receive(delta(SnapshotSyncLeaseRecord.getDefaultInstance(), 101)));
            SnapshotSyncLeaseRecord captured = start(sink);
            StreamsSnapshotWriter failing = mock(StreamsSnapshotWriter.class);
            doThrow(new IllegalStateException("shadow write unavailable")).when(failing).apply(any(LogReplicationEntryMsg.class));
            sink.setSnapshotWriter(failing);
            assertThrows(org.corfudb.runtime.exceptions.LogReplicationBusyException.class, () -> sink.receive(data(captured)));
            await(() -> sink.getSnapshotLease().getPhase() == SnapshotSyncLeaseRecord.Phase.RECOVERING);
            assertEquals(SnapshotSyncLeaseRecord.Outcome.ABORTED, sink.getSnapshotLease().getOutcome());
            assertFalse(sink.getSnapshotLease().getProtectionHeld());
            assertFalse(metadata.getDataConsistentOnStandby().get("sink").getDataConsistent());
        } finally { sink.shutdown(); }
    }

    @Test
    public void persistedLifecycleActivatesOnRestartAndPreservesRecoveryDebt() throws Exception {
        SnapshotSyncLeaseRecord prior = transferring();
        LogReplicationSinkManager sink = new LogReplicationSinkManager(rt, config, metadata, new DefaultSnapshotSyncPlugin(rt));
        try {
            assertTrue(sink.isSnapshotLifecycleEnabled());
            assertThrows(IllegalStateException.class, () -> sink.enableSnapshotLifecycle(1000, 0, 1000));
            sink.updateTopologyConfigId(1);
            sink.setLeadership(true);
            await(() -> sink.getSnapshotLease().getPhase() == SnapshotSyncLeaseRecord.Phase.RECOVERING);
            assertEquals(SnapshotSyncLeaseRecord.Outcome.ABORTED, sink.getSnapshotLease().getOutcome());
            assertEquals(prior.getGeneration(), sink.getSnapshotLease().getGeneration());
            assertEquals(prior.getDeadlineMs(), sink.getSnapshotLease().getDeadlineMs());
            assertEquals(prior.getProtectionId(), sink.getSnapshotLease().getProtectionId());
            assertNotEquals(prior.getOwnerId(), sink.getSnapshotLease().getOwnerId());
        } finally { sink.shutdown(); }
    }

    @Test
    public void independentCheckpointAndTrimRecoverAnAbandonedSinkWithoutSourcePolling() throws Exception {
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
            assertEquals(CompactorLeaderServices.LeaderInitStatus.SUCCESS, compactor.initCompactionCycle());
            CorfuRuntime cpRuntime = getNewRuntime(getDefaultNode()).connect();
            ServerTriggeredCheckpointer checkpointer = new ServerTriggeredCheckpointer(CheckpointerBuilder.builder()
                    .corfuRuntime(rt).cpRuntime(java.util.Optional.of(cpRuntime)).isClient(false)
                    .persistedCacheRoot(java.util.Optional.empty()).build(), metadata.getCorfuStore(), helper.getCompactorMetadataTables());
            try { checkpointer.checkpointTables(); } finally { checkpointer.shutdown(); }
            compactor.finishCompactionCycle();
            new TrimLog().invokePrefixTrim(rt, metadata.getCorfuStore());
            assertTrue(rt.getAddressSpaceView().getTrimMark().getSequence() > recoveryCut);
            await(() -> sink.getSnapshotLease().getPhase() == SnapshotSyncLeaseRecord.Phase.READY);
            assertEquals(captured.getDeadlineMs(), sink.getSnapshotLease().getDeadlineMs());
            assertEquals(SnapshotSyncLeaseRecord.Outcome.ABORTED, sink.getSnapshotLease().getOutcome());
            SnapshotSyncLeaseRecord next = start(sink);
            assertEquals(captured.getGeneration() + 1, next.getGeneration());
        } finally { sink.shutdown(); }
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
        SnapshotSyncLeaseRecord applying = leases.updateOwned("owner", (txn, current) -> {
            metadata.transferSnapshot(txn, 100);
            return SnapshotSyncLease.transferred(current);
        });
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
        org.corfudb.infrastructure.logreplication.replication.receive.LogEntryWriter incremental =
                new org.corfudb.infrastructure.logreplication.replication.receive.LogEntryWriter(config, metadata);
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
    public void unknownSchemaAndOldOwnerFailClosed() {
        assertThrows(SnapshotSyncLease.LeaseRejectedException.class, () -> leases.updateOwned("old-owner", (txn, state) -> state));
        SnapshotSyncLeaseRecord future = leases.read().toBuilder().setSchemaVersion(2).build();
        try (TxnContext txn = metadata.getTxnContext()) {
            SnapshotSyncLeaseStore.write(txn, future);
            txn.commit();
        }
        assertThrows(SnapshotSyncLease.LeaseRejectedException.class, leases::read);
        assertThrows(SnapshotSyncLease.LeaseRejectedException.class, helper::isCheckpointFrozen);
    }
}
