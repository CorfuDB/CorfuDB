package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.common.util.concurrent.MoreExecutors;
import org.corfudb.infrastructure.health.HealthMonitor;
import org.corfudb.infrastructure.health.Issue;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ISnapshotSyncPlugin;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.DistributedCheckpointerHelper;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Phase;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Outcome;
import org.corfudb.runtime.SnapshotSyncLease;
import org.corfudb.runtime.SnapshotSyncLeaseStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.LogReplicationBusyException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.view.AddressSpaceView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SnapshotLeaseCoordinatorTest {

    private final AtomicLong clock = new AtomicLong(1000);
    private final AtomicLong nanos = new AtomicLong(1000000000);
    private final AtomicLong trim = new AtomicLong(0);
    private final AtomicLong safeCut = new AtomicLong(-1);
    private final AtomicReference<CheckpointingStatus.StatusType> cycle = new AtomicReference<>(CheckpointingStatus.StatusType.IDLE);
    private final AtomicReference<SnapshotSyncLeaseRecord> persisted = new AtomicReference<>(SnapshotSyncLeaseRecord.getDefaultInstance());
    private CorfuRuntime runtime;
    private LogReplicationMetadataManager metadata;
    private ISnapshotSyncPlugin plugin;
    private SnapshotLeaseCoordinator.Worker worker;
    private SnapshotSyncLeaseStore store;
    private DistributedCheckpointerHelper checkpointer;
    private TxnContext txn;
    private SnapshotLeaseCoordinator coordinator;

    @Test
    void legacyWorkAndUnsupportedPluginsCannotActivateTheNewPolicy() {
        persisted.set(SnapshotSyncLeaseRecord.getDefaultInstance());
        when(metadata.legacySnapshotPending(txn)).thenReturn(true);
        create(MoreExecutors.newDirectExecutorService(), 0);
        assertEquals(Phase.NOT_READY, coordinator.status().getPhase());
        assertThrows(LogReplicationBusyException.class, () -> coordinator.start(proposal()));
        when(metadata.legacySnapshotPending(txn)).thenReturn(false);
        when(checkpointer.isCheckpointFrozen(txn)).thenReturn(true);
        coordinator.tick();
        assertEquals(Phase.NOT_READY, coordinator.status().getPhase());
        when(checkpointer.isCheckpointFrozen(txn)).thenReturn(false);
        coordinator.tick();
        assertEquals(Phase.READY, coordinator.status().getPhase());
        when(plugin.supportsOwnedSnapshotLifecycle()).thenReturn(false);
        assertThrows(IllegalArgumentException.class, () -> new SnapshotLeaseCoordinator(runtime, metadata, plugin, worker,
                1000, 0, 10, 0, new SnapshotLeaseCoordinator.Environment(clock::get, nanos::get, store, checkpointer,
                MoreExecutors.newDirectExecutorService(), false)));
    }

    @Test
    void preparationFailureRetainsProtectionUntilOwnedRelease() {
        doThrow(new IllegalStateException("plugin acquisition failed")).when(plugin).acquireSnapshot(any(), any());
        coordinator.start(proposal());
        coordinator.tick();
        assertEquals(Phase.ABORTING, coordinator.status().getPhase());
        assertTrue(coordinator.status().getProtectionHeld());
        verify(worker, never()).prepare(any());
        coordinator.tick();
        assertEquals(Phase.RECOVERING, coordinator.status().getPhase());
        verify(plugin).releaseSnapshot(any(), any());
    }

    @Test
    void transferGenerationAndSingleReceiverAreCheckedBeforeProcessing() {
        SnapshotSyncLeaseRecord captured = transfer();
        LogReplicationEntryMetadataMsg entry = proposal().toBuilder().setAttemptGeneration(captured.getGeneration()).build();
        assertThrows(LogReplicationBusyException.class, () -> coordinator.enterTransfer(entry.toBuilder().setAttemptGeneration(99).build()));
        assertEquals(captured, coordinator.enterTransfer(entry));
        assertThrows(LogReplicationBusyException.class, () -> coordinator.enterTransfer(entry));
        coordinator.exitTransfer();
        assertEquals(captured, coordinator.enterTransfer(entry));
        coordinator.exitTransfer();
    }

    @Test
    void transientApplyRetriesUseTheOriginalDeadlineAndRespectRetryEligibility() {
        create(MoreExecutors.newDirectExecutorService(), 2);
        SnapshotSyncLeaseRecord captured = transfer();
        doThrow(new IllegalStateException("temporary apply failure")).when(worker).apply(any());
        coordinator.transferComplete(captured, 0);
        coordinator.tick();
        assertEquals(1, coordinator.status().getApplyRetries());
        assertEquals(captured.getDeadlineMs(), coordinator.status().getDeadlineMs());
        coordinator.tick();
        verify(worker, times(1)).apply(any());
        clock.set(captured.getDeadlineMs());
        coordinator.tick();
        assertEquals(Phase.RECOVERING, coordinator.status().getPhase());
    }

    @Test
    void trimmedApplyAbandonsEvenWhenRetriesRemain() {
        verifyFatalApplyFailure(new TrimmedException());
    }

    @Test
    void rejectedApplyLeaseAbandonsEvenWhenRetriesRemain() {
        verifyFatalApplyFailure(new SnapshotSyncLease.LeaseRejectedException("writer fenced"));
    }

    private void verifyFatalApplyFailure(RuntimeException failure) {
        create(MoreExecutors.newDirectExecutorService(), 2);
        SnapshotSyncLeaseRecord attempt = transfer();
        coordinator.transferComplete(attempt, 0);
        doThrow(failure).when(worker).apply(any());
        coordinator.tick();
        assertEquals(Phase.ABORTING, coordinator.status().getPhase());
        assertEquals(0, coordinator.status().getApplyRetries());
        assertEquals(attempt.getDeadlineMs(), coordinator.status().getDeadlineMs());
        assertTrue(coordinator.status().getProtectionHeld());
        verify(worker, times(1)).apply(any());
        verify(plugin, never()).releaseSnapshot(any(), any());
    }

    @Test
    void workerAbandonmentDoesNotInterruptItsOwnThread() {
        AtomicBoolean interrupted = new AtomicBoolean();
        SnapshotSyncLeaseRecord attempt = transfer();
        coordinator.transferComplete(attempt, 0);
        doAnswer(invocation -> {
            coordinator.abandon("worker cancelled its attempt");
            interrupted.set(Thread.currentThread().isInterrupted());
            return null;
        }).when(worker).apply(any());
        coordinator.tick();
        verify(worker).apply(any());
        assertEquals(Phase.ABORTING, coordinator.status().getPhase());
        assertFalse(interrupted.get());
    }

    @Test
    void missingOrTrimmedShadowBoundaryNeverStartsApply() {
        SnapshotSyncLeaseRecord captured = transfer();
        coordinator.transferComplete(captured, 0);
        persisted.set(persisted.get().toBuilder().setTransferredSequence(0).setFirstShadowAddress(-1).build());
        coordinator.tick();
        assertEquals(Phase.ABORTING, coordinator.status().getPhase());
        verify(worker, never()).apply(any());
    }

    @Test
    void recoveryTriggerIsCoalescedAndAMissingCheckpointCannotOpenAdmission() {
        transfer();
        clock.set(2000);
        coordinator.tick();
        when(txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE,
                CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM)).thenReturn(new CorfuStoreEntry(
                CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM, null, null));
        coordinator.tick();
        verify(txn).putRecord(any(), eq(CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM), any(), isNull());
        when(txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE,
                CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM)).thenReturn(new CorfuStoreEntry(
                CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM, RpcCommon.TokenMsg.getDefaultInstance(), null));
        coordinator.tick();
        verify(txn, times(1)).putRecord(any(), eq(CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM), any(), isNull());
        assertEquals(Phase.RECOVERING, coordinator.status().getPhase());
    }
    @BeforeEach
    void setup() {
        runtime = mock(CorfuRuntime.class);
        AddressSpaceView addressSpace = mock(AddressSpaceView.class);
        when(runtime.getAddressSpaceView()).thenReturn(addressSpace);
        when(addressSpace.getTrimMark()).thenAnswer(invocation -> new Token(0, trim.get()));
        metadata = mock(LogReplicationMetadataManager.class);
        plugin = mock(ISnapshotSyncPlugin.class);
        when(plugin.supportsOwnedSnapshotLifecycle()).thenReturn(true);
        worker = mock(SnapshotLeaseCoordinator.Worker.class);
        store = mock(SnapshotSyncLeaseStore.class);
        checkpointer = mock(DistributedCheckpointerHelper.class, RETURNS_DEEP_STUBS);
        txn = mock(TxnContext.class);
        when(metadata.getTxnContext()).thenReturn(txn);
        when(txn.getTxnSequence()).thenReturn(100L);
        when(txn.getRecord(anyString(), any())).thenAnswer(invocation -> {
            Object key = invocation.getArgument(1);
            Object payload = key.equals(CompactorMetadataTables.COMPACTION_MANAGER_KEY)
                    ? CheckpointingStatus.newBuilder().setStatus(cycle.get()).build()
                    : RpcCommon.TokenMsg.newBuilder().setSequence(safeCut.get()).build();
            return new CorfuStoreEntry((com.google.protobuf.Message) key, (com.google.protobuf.Message) payload, null);
        });
        when(store.read()).thenAnswer(invocation -> persisted.get());
        when(store.update(any())).thenAnswer(invocation -> {
            BiFunction<TxnContext, SnapshotSyncLeaseRecord, SnapshotSyncLeaseRecord> change = invocation.getArgument(0);
            SnapshotSyncLeaseRecord next = change.apply(txn, persisted.get());
            persisted.set(next);
            return next;
        });
        when(store.updateOwned(anyString(), any())).thenAnswer(invocation -> {
            if (!persisted.get().getOwnerId().equals(invocation.getArgument(0))) {
                throw new SnapshotSyncLease.LeaseRejectedException("owner changed");
            }
            BiFunction<TxnContext, SnapshotSyncLeaseRecord, SnapshotSyncLeaseRecord> change = invocation.getArgument(1);
            SnapshotSyncLeaseRecord next = change.apply(txn, persisted.get());
            persisted.set(next);
            return next;
        });
        create(MoreExecutors.newDirectExecutorService(), 0);
    }

    private void create(ExecutorService executor, int retries) {
        if (coordinator != null) { coordinator.close(); }
        coordinator = new SnapshotLeaseCoordinator(runtime, metadata, plugin, worker, 1000, 100, 50, retries,
                new SnapshotLeaseCoordinator.Environment(clock::get, nanos::get, store, checkpointer, executor, false));
        coordinator.leadership(true);
        coordinator.tick();
    }

    @AfterEach
    void cleanup() { coordinator.close(); }

    private LogReplicationEntryMetadataMsg proposal() {
        return LogReplicationEntryMetadataMsg.newBuilder().setSnapshotLifecycleVersion(1)
                .setAdmissionEpoch(coordinator.status().getAdmissionEpoch())
                .setSyncRequestId(RpcCommon.UuidMsg.newBuilder().setMsb(1).setLsb(2))
                .setTopologyConfigID(3).setSnapshotTimestamp(50).build();
    }

    private SnapshotSyncLeaseRecord transfer() {
        coordinator.start(proposal());
        coordinator.tick();
        assertEquals(Phase.TRANSFERRING, coordinator.status().getPhase());
        return coordinator.status();
    }

    @Test
    void duplicateStartDoesNotAcquireResetOrRenew() {
        LogReplicationEntryMetadataMsg start = proposal();
        SnapshotSyncLeaseRecord reserved = coordinator.start(start);
        assertEquals(reserved, coordinator.start(start));
        coordinator.tick();
        SnapshotSyncLeaseRecord current = coordinator.start(start);
        assertEquals(reserved.getDeadlineMs(), current.getDeadlineMs());
        verify(plugin, times(1)).acquireSnapshot(eq(runtime), any());
        verify(metadata, times(1)).initializeSnapshot(eq(txn), eq(start));
        assertThrows(LogReplicationBusyException.class, () -> coordinator.start(start.toBuilder()
                .setSyncRequestId(RpcCommon.UuidMsg.newBuilder().setMsb(3)).build()));
    }

    @Test
    void sourceDisappearanceDoesNotPreventDeadlineReleaseAndRecovery() {
        transfer();
        clock.set(2000);
        coordinator.tick();
        assertEquals(Phase.RECOVERING, coordinator.status().getPhase());
        assertEquals(Outcome.ABORTED, coordinator.status().getOutcome());
        assertFalse(coordinator.status().getProtectionHeld());
        assertThrows(LogReplicationBusyException.class, () -> coordinator.start(proposal()));
        cycle.set(CheckpointingStatus.StatusType.FAILED);
        clock.set(1000000);
        coordinator.tick();
        assertEquals(Phase.RECOVERING, coordinator.status().getPhase());
        cycle.set(CheckpointingStatus.StatusType.COMPLETED);
        safeCut.set(100);
        trim.set(100);
        coordinator.tick();
        assertEquals(Phase.RECOVERING, coordinator.status().getPhase());
        trim.set(101);
        coordinator.tick();
        assertEquals(Phase.READY, coordinator.status().getPhase());
    }

    @Test
    void monotonicBudgetCannotBeExtendedByWallClockRollback() {
        transfer();
        clock.set(1100);
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(1000));
        coordinator.tick();
        assertEquals(Outcome.ABORTED, coordinator.status().getOutcome());
    }

    @Test
    void transferMustDrainBeforeProtectionIsReleased() {
        SnapshotSyncLeaseRecord attempt = transfer();
        coordinator.enterTransfer(proposal().toBuilder().setAttemptGeneration(attempt.getGeneration()).build());
        assertThrows(LogReplicationBusyException.class, () -> coordinator.enterTransfer(proposal()));
        clock.set(2000);
        coordinator.tick();
        assertEquals(Phase.ABORTING, coordinator.status().getPhase());
        verify(plugin, never()).releaseSnapshot(any(), any());
        coordinator.exitTransfer();
        coordinator.tick();
        assertEquals(Phase.RECOVERING, coordinator.status().getPhase());
    }

    @Test
    void releaseFailureIsRetriedAndRecoveryTimeBeginsOnlyOnConfirmation() {
        transfer();
        doThrow(new IllegalStateException("uncertain release")).doNothing().when(plugin).releaseSnapshot(any(), any());
        coordinator.abandon("test");
        coordinator.tick();
        assertEquals(Phase.RELEASING, coordinator.status().getPhase());
        assertTrue(coordinator.status().getProtectionHeld());
        assertEquals(0, coordinator.status().getReleasedAtMs());
        clock.set(1100);
        coordinator.tick();
        assertEquals(1100, coordinator.status().getReleasedAtMs());
        verify(plugin, times(2)).releaseSnapshot(eq(runtime), any());
    }

    @Test
    void exhaustedApplyRetriesAbandonWithoutAnotherSourcePoll() {
        SnapshotSyncLeaseRecord attempt = transfer();
        coordinator.transferComplete(attempt, 0);
        doThrow(new IllegalStateException("apply failed")).when(worker).apply(any());
        coordinator.tick();
        assertEquals(Phase.ABORTING, coordinator.status().getPhase());
        coordinator.tick();
        assertEquals(Phase.RECOVERING, coordinator.status().getPhase());
    }

    @Test
    void successIsPreservedAcrossLostCompletionAndReleaseFailure() {
        SnapshotSyncLeaseRecord attempt = transfer();
        coordinator.transferComplete(attempt, 0);
        doAnswer(invocation -> { persisted.set(SnapshotSyncLease.completed(persisted.get())); throw new IllegalStateException("lost commit reply"); })
                .when(worker).apply(any());
        coordinator.tick();
        coordinator.tick();
        verify(worker).completed(any());
        coordinator.tick();
        assertEquals(Outcome.COMPLETED, coordinator.status().getOutcome());
        assertEquals(Phase.RECOVERING, coordinator.status().getPhase());
        verify(worker, times(1)).apply(any());
    }

    @Test
    void successorPreservesAbandonmentAndRejectsAnOldOwner() {
        transfer();
        long deadline = persisted.get().getDeadlineMs();
        create(MoreExecutors.newDirectExecutorService(), 0);
        assertEquals(Outcome.ABORTED, coordinator.status().getOutcome());
        assertEquals(deadline, coordinator.status().getDeadlineMs());
        persisted.set(SnapshotSyncLease.next(persisted.get()).setOwnerId("another owner").build());
        coordinator.tick();
        assertEquals(Phase.NOT_READY, coordinator.status().getPhase());
        assertThrows(LogReplicationBusyException.class, () -> coordinator.start(proposal()));
    }

    @Test
    void recoveryAlarmNeverReopensAdmissionAndClearsOnlyAfterRecovery() {
        transfer();
        coordinator.abandon("failure");
        coordinator.tick();
        try (var monitor = mockStatic(HealthMonitor.class)) {
            clock.set(1100);
            coordinator.checkHealth();
            monitor.verify(() -> HealthMonitor.reportIssue(argThat(issue -> issue.getIssueId() == Issue.IssueId.SNAPSHOT_RECOVERY_BLOCKED)));
            assertEquals(Phase.RECOVERING, coordinator.status().getPhase());
            cycle.set(CheckpointingStatus.StatusType.COMPLETED);
            safeCut.set(100);
            trim.set(101);
            coordinator.tick();
            coordinator.checkHealth();
            monitor.verify(() -> HealthMonitor.resolveIssue(any()));
        }
    }

    @Test
    void recoveryGaugeReflectsTheGateAndIsRemovedOnClose() {
        io.micrometer.core.instrument.simple.SimpleMeterRegistry registry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        try (var metrics = mockStatic(org.corfudb.common.metrics.micrometer.MeterRegistryProvider.class)) {
            metrics.when(org.corfudb.common.metrics.micrometer.MeterRegistryProvider::getInstance)
                    .thenReturn(java.util.Optional.of(registry));
            create(MoreExecutors.newDirectExecutorService(), 0);
            var gauge = registry.get("logreplication.snapshot.recovery.blocked").gauge();
            assertEquals(0.0, gauge.value());
            transfer();
            coordinator.abandon("failure");
            coordinator.tick();
            clock.set(1100);
            coordinator.checkHealth();
            assertEquals(1.0, gauge.value());
            cycle.set(CheckpointingStatus.StatusType.COMPLETED);
            safeCut.set(100);
            trim.set(101);
            coordinator.tick();
            coordinator.checkHealth();
            assertEquals(0.0, gauge.value());
            coordinator.close();
            assertNull(registry.find("logreplication.snapshot.recovery.blocked").gauge());
        } finally { registry.close(); }
    }

    @Test
    void interruptIgnoringApplyIsNotReplacedOrUnfrozen() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        create(executor, 0);
        coordinator.start(proposal());
        coordinator.tick();
        executor.submit(() -> { }).get(2, TimeUnit.SECONDS);
        SnapshotSyncLeaseRecord attempt = coordinator.status();
        coordinator.transferComplete(attempt, 0);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        doAnswer(invocation -> {
            entered.countDown();
            while (release.getCount() != 0) {
                try { release.await(); } catch (InterruptedException ignored) {
                    // Observe cancellation, but deliberately keep the worker running until released by the test.
                    interrupted.countDown();
                }
            }
            return null;
        }).when(worker).apply(any());
        try {
            coordinator.tick();
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            clock.set(3000);
            coordinator.tick();
            assertTrue(interrupted.await(2, TimeUnit.SECONDS));
            coordinator.tick();
            assertEquals(Phase.ABORTING, coordinator.status().getPhase());
            clock.set(3100);
            coordinator.tick();
            assertEquals(Phase.FAULTED, coordinator.status().getPhase());
            assertTrue(coordinator.status().getProtectionHeld());
            verify(worker, times(1)).apply(any());
            verify(plugin, never()).releaseSnapshot(any(), any());
        } finally {
            release.countDown();
            executor.submit(() -> { }).get(2, TimeUnit.SECONDS);
        }
        coordinator.tick();
        executor.submit(() -> { }).get(2, TimeUnit.SECONDS);
        assertEquals(Phase.RECOVERING, coordinator.status().getPhase());
    }
}
