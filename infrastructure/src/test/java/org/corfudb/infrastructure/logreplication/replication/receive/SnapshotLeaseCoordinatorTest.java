package org.corfudb.infrastructure.logreplication.replication.receive;

import com.google.common.util.concurrent.MoreExecutors;
import org.corfudb.infrastructure.health.HealthMonitor;
import org.corfudb.infrastructure.health.Issue;
import org.corfudb.infrastructure.logreplication.infrastructure.plugins.ISnapshotSyncPlugin;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.DistributedCheckpointerHelper;
import org.corfudb.runtime.LogReplication.LogReplicationBusyResponseMsg;
import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Phase;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Outcome;
import org.corfudb.runtime.SnapshotSyncLease;
import org.corfudb.runtime.SnapshotSyncLeaseStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.LogReplicationBusyException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.view.AddressSpaceView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.UUID;
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

    private static final long DURATION_MS = 1000;
    private static final long RECOVERY_MS = 100;
    private static final long ALARM_MS = 50;

    private final AtomicLong clock = new AtomicLong(1000);
    private final AtomicLong nanos = new AtomicLong(1000000000);
    private final AtomicLong trim = new AtomicLong(0);
    private final AtomicLong safeCut = new AtomicLong(-1);
    // Null emulates a cluster on which the compactor never ran: it never wrote its manager record.
    private final AtomicReference<CheckpointingStatus.StatusType> cycle = new AtomicReference<>(CheckpointingStatus.StatusType.IDLE);
    private final AtomicReference<SnapshotSyncLeaseRecord> persisted = new AtomicReference<>(SnapshotSyncLeaseRecord.getDefaultInstance());
    private final TableName failingTable = TableName.newBuilder().setNamespace("ns").setTableName("stuck").build();
    // Every attempt of a real source carries a fresh identity. proposal() is stable within one
    // attempt, and recover() moves on to the next one.
    private long attemptLsb = 2;
    private CorfuRuntime runtime;
    private LogReplicationMetadataManager metadata;
    private ISnapshotSyncPlugin plugin;
    private SnapshotLeaseCoordinator.Worker worker;
    private SnapshotSyncLeaseStore store;
    private DistributedCheckpointerHelper checkpointer;
    private TxnContext txn;
    private SnapshotLeaseCoordinator coordinator;

    @BeforeEach
    void setup() {
        runtime = mock(CorfuRuntime.class);
        AddressSpaceView addressSpace = mock(AddressSpaceView.class);
        when(runtime.getAddressSpaceView()).thenReturn(addressSpace);
        when(addressSpace.getTrimMark()).thenAnswer(invocation -> new Token(0, trim.get()));
        metadata = mock(LogReplicationMetadataManager.class);
        plugin = mock(ISnapshotSyncPlugin.class);
        worker = mock(SnapshotLeaseCoordinator.Worker.class);
        store = mock(SnapshotSyncLeaseStore.class);
        checkpointer = mock(DistributedCheckpointerHelper.class, RETURNS_DEEP_STUBS);
        txn = mock(TxnContext.class);
        when(metadata.getTxnContext()).thenReturn(txn);
        when(txn.getTxnSequence()).thenReturn(100L);
        when(txn.getRecord(anyString(), any())).thenAnswer(invocation -> {
            Object key = invocation.getArgument(1);
            Object payload;
            if (key.equals(CompactorMetadataTables.COMPACTION_MANAGER_KEY)) {
                payload = cycle.get() == null ? null : CheckpointingStatus.newBuilder().setStatus(cycle.get()).setCycleCount(7).build();
            } else if (key instanceof TableName) {
                payload = CheckpointingStatus.newBuilder().setStatus(CheckpointingStatus.StatusType.FAILED).build();
            } else {
                payload = RpcCommon.TokenMsg.newBuilder().setSequence(safeCut.get()).build();
            }
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
        create(MoreExecutors.newDirectExecutorService(), 0, 0);
    }

    private void create(ExecutorService executor, int retries, long idleMs) {
        if (coordinator != null) { coordinator.close(); }
        coordinator = new SnapshotLeaseCoordinator(runtime, metadata, plugin, worker,
                new SnapshotLeaseCoordinator.Timing(DURATION_MS, RECOVERY_MS, ALARM_MS, idleMs, retries),
                new SnapshotLeaseCoordinator.Environment(clock::get, nanos::get, store, checkpointer, executor, false));
        coordinator.leadership(true);
        coordinator.tick();
        drain(executor);
        // Taking the lease over installs the incremental writer once; tests count what follows.
        clearInvocations(worker);
    }

    private static void drain(ExecutorService executor) {
        try {
            executor.submit(() -> { }).get(2, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new AssertionError("The effects executor did not drain", e);
        }
    }

    @AfterEach
    void cleanup() { coordinator.close(); }

    private LogReplicationEntryMetadataMsg proposal() {
        return LogReplicationEntryMetadataMsg.newBuilder().setSnapshotLifecycleVersion(1)
                .setAdmissionEpoch(coordinator.status().getAdmissionEpoch())
                .setSyncRequestId(RpcCommon.UuidMsg.newBuilder().setMsb(1).setLsb(attemptLsb))
                .setTopologyConfigID(3).setSnapshotTimestamp(50).build();
    }

    private SnapshotSyncLeaseRecord transfer() {
        coordinator.start(proposal());
        coordinator.tick();
        assertEquals(Phase.TRANSFERRING, coordinator.status().getPhase());
        return coordinator.status();
    }

    /** Drives the record from whatever ended the attempt to READY again. */
    private void recover() {
        coordinator.tick();
        assertEquals(Phase.RECOVERING, coordinator.status().getPhase());
        clock.addAndGet(RECOVERY_MS);
        cycle.set(CheckpointingStatus.StatusType.COMPLETED);
        safeCut.set(100);
        trim.set(101);
        coordinator.tick();
        assertEquals(Phase.READY, coordinator.status().getPhase());
        attemptLsb++;
    }

    // ---------------------------------------------------------------- takeover and migration

    @Test
    void anIdleSinkIsSeededAdmittingAndInstallsTheIncrementalWriter() {
        persisted.set(SnapshotSyncLeaseRecord.getDefaultInstance());
        when(metadata.lastAppliedSnapshot(txn)).thenReturn(40L);
        when(metadata.persistedTopologyConfigId(txn)).thenReturn(3L);
        if (coordinator != null) { coordinator.close(); }
        coordinator = new SnapshotLeaseCoordinator(runtime, metadata, plugin, worker,
                new SnapshotLeaseCoordinator.Timing(DURATION_MS, RECOVERY_MS, ALARM_MS, 0, 0),
                new SnapshotLeaseCoordinator.Environment(clock::get, nanos::get, store, checkpointer,
                        MoreExecutors.newDirectExecutorService(), false));
        assertEquals(Phase.NOT_READY, coordinator.status().getPhase(), "nothing is published before leadership");
        assertThrows(LogReplicationBusyException.class, () -> coordinator.start(proposal()));
        coordinator.leadership(true);
        coordinator.tick();

        SnapshotSyncLeaseRecord seeded = coordinator.status();
        assertEquals(Phase.READY, seeded.getPhase());
        assertEquals(Outcome.COMPLETED, seeded.getOutcome());
        assertEquals(0, seeded.getGeneration());
        assertEquals(40, seeded.getSourceSnapshot());
        assertEquals(3, seeded.getTopologyConfigId());
        assertFalse(seeded.getProtectionHeld());
        verify(worker).completed(seeded);
        verify(txn, never()).delete(anyString(), any());
        verify(metadata, never()).abandonSnapshot(any());
        assertEquals(Phase.TRANSFERRING, transfer().getPhase(), "the first attempt is admitted right away");
    }

    @Test
    void aPendingLegacySnapshotIsSupersededAndOwesACheckpointBeforeAdmission() {
        persisted.set(SnapshotSyncLeaseRecord.getDefaultInstance());
        when(metadata.legacySnapshotPending(txn)).thenReturn(true);
        create(MoreExecutors.newDirectExecutorService(), 0, 0);

        SnapshotSyncLeaseRecord seeded = coordinator.status();
        assertEquals(Phase.RECOVERING, seeded.getPhase());
        assertEquals(Outcome.ABORTED, seeded.getOutcome());
        assertEquals(100, seeded.getRecoveryCut());
        assertFalse(seeded.getProtectionHeld());
        // The hook that would have deleted the pre-lease freeze token is never called again.
        verify(txn).delete(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.FREEZE_TOKEN);
        verify(metadata).abandonSnapshot(txn);
        assertThrows(LogReplicationBusyException.class, () -> coordinator.start(proposal()));

        // Nothing but a checkpoint and trim past the cut reopens admission.
        clock.set(1000000);
        coordinator.tick();
        assertEquals(Phase.RECOVERING, coordinator.status().getPhase());
        verify(worker, never()).completed(any()); // No snapshot is known to be complete: no incremental writer.
        cycle.set(CheckpointingStatus.StatusType.COMPLETED);
        safeCut.set(100);
        trim.set(101);
        coordinator.tick();
        assertEquals(Phase.READY, coordinator.status().getPhase());
        assertEquals(Outcome.ABORTED, coordinator.status().getOutcome(), "incremental traffic waits for a snapshot");
        assertEquals(Phase.TRANSFERRING, transfer().getPhase());
    }

    @Test
    void anOperatorFreezeTokenIsLeftAloneWhenNoLegacySnapshotIsPending() {
        persisted.set(SnapshotSyncLeaseRecord.getDefaultInstance());
        create(MoreExecutors.newDirectExecutorService(), 0, 0);
        assertEquals(Phase.READY, coordinator.status().getPhase());
        verify(txn, never()).delete(anyString(), any());
    }

    /**
     * During a rolling upgrade leadership can return to a node that still runs the pre-lease
     * protocol. It ignores the record, starts a snapshot of its own and freezes the checkpointer
     * with its token, which nothing in this version would ever delete.
     */
    @Test
    void aSnapshotLeftByAPreLeaseLeaderIsSupersededExactlyOnce() {
        assertEquals(Outcome.COMPLETED, coordinator.status().getOutcome());
        when(metadata.legacySnapshotPending(txn)).thenReturn(true);
        when(metadata.lastStartedSnapshot(txn)).thenReturn(80L);
        create(MoreExecutors.newDirectExecutorService(), 0, 0);

        SnapshotSyncLeaseRecord superseded = coordinator.status();
        assertEquals(Phase.RECOVERING, superseded.getPhase());
        assertEquals(Outcome.ABORTED, superseded.getOutcome(), "the source must start over");
        assertEquals(100, superseded.getRecoveryCut());
        assertEquals(80, superseded.getSourceSnapshot());
        assertEquals(SnapshotLeaseCoordinator.LEGACY_SUPERSEDED, superseded.getFailure());
        assertFalse(superseded.getProtectionHeld());
        verify(txn).delete(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.FREEZE_TOKEN);
        verify(metadata).abandonSnapshot(txn);
        assertThrows(LogReplicationBusyException.class, () -> coordinator.start(proposal()));
        coordinator.tick();
        verify(worker, never()).completed(any()); // The incremental writer is not installed for it.

        // The same unfinished snapshot does not push the recovery cut forward at every takeover.
        clearInvocations(txn, metadata);
        when(txn.getTxnSequence()).thenReturn(500L);
        create(MoreExecutors.newDirectExecutorService(), 0, 0);
        assertEquals(Phase.RECOVERING, coordinator.status().getPhase());
        assertEquals(100, coordinator.status().getRecoveryCut());
        verify(txn, never()).delete(anyString(), any());
        verify(metadata, never()).abandonSnapshot(any());
    }

    @Test
    void aForeignSnapshotFoundNextToAnActiveAttemptIsSupersededAfterTheAttemptIsAbandoned() {
        transfer();
        when(metadata.legacySnapshotPending(txn)).thenReturn(true);
        when(metadata.lastStartedSnapshot(txn)).thenReturn(80L); // the attempt of the lease is for snapshot 50
        create(MoreExecutors.newDirectExecutorService(), 0, 0);

        // Abandoned, superseded and, in the same reconciliation, released.
        SnapshotSyncLeaseRecord state = coordinator.status();
        assertEquals(Phase.RECOVERING, state.getPhase());
        assertEquals(Outcome.ABORTED, state.getOutcome());
        assertEquals(SnapshotLeaseCoordinator.LEGACY_SUPERSEDED, state.getFailure());
        assertEquals(80, state.getSourceSnapshot());
        assertEquals(1, state.getConsecutiveAborts());
        assertFalse(state.getProtectionHeld());
        assertTrue(state.getRecoveryCut() >= 100);
        verify(txn).delete(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.FREEZE_TOKEN);
        verify(plugin).releaseSnapshot(eq(runtime), any());
    }

    @Test
    void anUnfinishedAttemptOfTheLeaseItselfIsNotMistakenForAForeignSnapshot() {
        // After an abandoned attempt the timestamps look exactly like a pending pre-lease snapshot.
        coordinator.start(proposal());
        coordinator.abandon("source cancelled");
        when(metadata.legacySnapshotPending(txn)).thenReturn(true);
        when(metadata.lastStartedSnapshot(txn)).thenReturn(50L);
        create(MoreExecutors.newDirectExecutorService(), 0, 0);

        assertEquals("source cancelled", coordinator.status().getFailure());
        verify(txn, never()).delete(anyString(), any());
    }

    /** The previous leader's incremental writer validates the lease without writing it. */
    @Test
    void everyTakeoverFencesThePreviousLeadersIncrementalWriter() {
        verify(metadata, times(1)).fenceIncrementalWriters(txn);
        create(MoreExecutors.newDirectExecutorService(), 0, 0);
        verify(metadata, times(2)).fenceIncrementalWriters(txn);
    }

    @Test
    void aReservationThatLosesACommitRaceIsAnsweredWithATypedReply() {
        SnapshotSyncLeaseRecord before = persisted.get();
        // doThrow, not when(): when(store.updateOwned(anyString(), any())) would call the answer that
        // is already stubbed, with the empty owner that anyString() evaluates to.
        doThrow(new TransactionAbortedException(
                new TxResolutionInfo(UUID.randomUUID(), new Token(0, 0)), AbortCause.CONFLICT, new Throwable(), null))
                .when(store).updateOwned(anyString(), any());
        LogReplicationBusyException busy = assertThrows(LogReplicationBusyException.class, () -> coordinator.start(proposal()));
        assertEquals(LogReplicationBusyResponseMsg.Reason.OVERLOADED, busy.getResponse().getReason());
        assertTrue(busy.getResponse().getRetryAfterMs() > 0);
        assertEquals(before, persisted.get());
        assertEquals(Phase.READY, coordinator.status().getPhase());
    }

    @Test
    void anUnboundedOrMeaninglessPolicyIsRefused() {
        for (SnapshotLeaseCoordinator.Timing invalid : new SnapshotLeaseCoordinator.Timing[]{
                new SnapshotLeaseCoordinator.Timing(0, 0, 10, 0, 0), new SnapshotLeaseCoordinator.Timing(1000, -1, 10, 0, 0),
                new SnapshotLeaseCoordinator.Timing(1000, 0, 0, 0, 0), new SnapshotLeaseCoordinator.Timing(1000, 0, 10, 0, -1)}) {
            assertThrows(IllegalArgumentException.class, () -> new SnapshotLeaseCoordinator(runtime, metadata, plugin, worker,
                    invalid, new SnapshotLeaseCoordinator.Environment(clock::get, nanos::get, store, checkpointer,
                    MoreExecutors.newDirectExecutorService(), false)));
        }
    }

    @Test
    void everyTakeoverReinstallsTheIncrementalWriterFromThePersistedPositions() {
        // Another node may have led in between and moved the replicated state on.
        coordinator.leadership(false);
        assertEquals(Phase.NOT_READY, coordinator.status().getPhase());
        coordinator.leadership(true);
        coordinator.tick();
        verify(worker, times(1)).completed(any());
        coordinator.tick();
        verify(worker, times(1)).completed(any());
    }

    @Test
    void successorPreservesAbandonmentAndAStolenRecordIsReacquired() {
        transfer();
        long deadline = persisted.get().getDeadlineMs();
        create(MoreExecutors.newDirectExecutorService(), 0, 0);
        assertEquals(Outcome.ABORTED, coordinator.status().getOutcome());
        assertEquals(deadline, coordinator.status().getDeadlineMs());
        verify(metadata, atLeastOnce()).abandonSnapshot(txn);

        persisted.set(SnapshotSyncLease.next(persisted.get()).setOwnerId("another owner").build());
        coordinator.tick();
        assertEquals(Phase.NOT_READY, coordinator.status().getPhase());
        assertThrows(LogReplicationBusyException.class, () -> coordinator.start(proposal()));
        assertFalse(coordinator.incrementalInstalled(persisted.get().getGeneration()));

        // While a lock changes hands two nodes can believe they lead. Taking the record back and
        // forth every second would abandon every attempt the legitimate leader admits.
        coordinator.tick();
        assertEquals("another owner", persisted.get().getOwnerId());
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(SnapshotLeaseCoordinator.REACQUIRE_BACKOFF_MS) - 1);
        coordinator.tick();
        assertEquals("another owner", persisted.get().getOwnerId());

        // This node still leads: it does not go silent, it takes the record back.
        nanos.addAndGet(1);
        coordinator.tick();
        assertNotEquals("another owner", persisted.get().getOwnerId());
        assertNotEquals(Phase.NOT_READY, coordinator.status().getPhase());
    }

    /**
     * A lock renewal that fails because the store is slow makes the takeover slow too, so losing
     * leadership in the middle of it is the likely case, not the odd one.
     */
    @Test
    void aLeadershipLossDuringTheTakeoverDoesNotLeaveTheNodeInitialized() {
        coordinator.leadership(false);
        coordinator.leadership(true);
        // doAnswer, not when(): when(store.update(any())) would call the answer that is already
        // stubbed, with the null that any() evaluates to.
        doAnswer(invocation -> {
            BiFunction<TxnContext, SnapshotSyncLeaseRecord, SnapshotSyncLeaseRecord> change = invocation.getArgument(0);
            SnapshotSyncLeaseRecord next = change.apply(txn, persisted.get());
            persisted.set(next);
            coordinator.leadership(false); // Lost while the transaction was in flight.
            return next;
        }).doAnswer(invocation -> {
            BiFunction<TxnContext, SnapshotSyncLeaseRecord, SnapshotSyncLeaseRecord> change = invocation.getArgument(0);
            SnapshotSyncLeaseRecord next = change.apply(txn, persisted.get());
            persisted.set(next);
            return next;
        }).when(store).update(any());
        clearInvocations(metadata, worker);

        coordinator.tick();
        assertEquals(Phase.NOT_READY, coordinator.status().getPhase());
        verify(worker, never()).completed(any());

        // The next leadership runs the whole takeover again: fencing, checks and reinstallation.
        coordinator.leadership(true);
        coordinator.tick();
        assertEquals(Phase.READY, coordinator.status().getPhase());
        verify(metadata, times(2)).fenceIncrementalWriters(txn);
        verify(worker, times(1)).completed(any());
    }

    @Test
    void theStatusServedAfterATakeoverIsNeverTheOneCachedBeforeIt() {
        coordinator.leadership(false);
        clearInvocations(metadata);
        coordinator.leadership(true);
        java.util.List<Phase> reportedWhenRefreshed = new java.util.ArrayList<>();
        doAnswer(invocation -> {
            reportedWhenRefreshed.add(coordinator.status().getPhase());
            return null;
        }).when(metadata).refreshSnapshotStatus();
        coordinator.tick();
        assertEquals(Phase.NOT_READY, reportedWhenRefreshed.get(0), "refreshed before the node reports itself ready");
        assertEquals(Phase.READY, coordinator.status().getPhase());
    }

    /** An incremental write that is in flight is never overtaken by a takeover or a reinstallation. */
    @Test
    void aTakeoverWaitsForAnIncrementalWriteThatIsInFlight() {
        long generation = coordinator.status().getGeneration();
        assertTrue(coordinator.incrementalInstalled(generation));
        assertFalse(coordinator.enterIncremental(generation + 1), "not the generation that is installed");
        assertTrue(coordinator.enterIncremental(generation));

        coordinator.leadership(false);
        assertFalse(coordinator.incrementalInstalled(generation));
        coordinator.leadership(true);
        coordinator.tick();
        assertEquals(Phase.NOT_READY, coordinator.status().getPhase(), "the write is still running");
        verify(worker, never()).completed(any());

        coordinator.exitIncremental();
        assertFalse(coordinator.enterIncremental(generation), "nothing is admitted before the writer is reinstalled");
        coordinator.tick();
        assertEquals(Phase.READY, coordinator.status().getPhase());
        verify(worker, times(1)).completed(any());
        assertTrue(coordinator.enterIncremental(generation));
        coordinator.exitIncremental();
    }

    @Test
    void steadyIncrementalTrafficDoesNotKeepProtectionFromBeingReleased() {
        SnapshotSyncLeaseRecord attempt = transfer();
        coordinator.transferComplete(attempt, 0);
        doAnswer(invocation -> { persisted.set(SnapshotSyncLease.completed(persisted.get())); return null; })
                .when(worker).apply(any());
        coordinator.tick(); // applies
        coordinator.tick(); // installs the incremental writer
        assertTrue(coordinator.enterIncremental(attempt.getGeneration()));
        coordinator.tick(); // an incremental write is in flight, as it nearly always is under load
        assertFalse(coordinator.status().getProtectionHeld());
        assertEquals(Phase.RECOVERING, coordinator.status().getPhase());
        coordinator.exitIncremental();
    }

    /** Reconciliation acts on a record it read a moment ago, while effects run on another thread. */
    @Test
    void aPreparationDispatchedOnAStaleReadDoesNotRunAgainUnderATransferThatStarted() {
        SnapshotSyncLeaseRecord reserved = coordinator.start(proposal());
        coordinator.tick();
        assertEquals(Phase.TRANSFERRING, coordinator.status().getPhase());
        // The next pass read the record just before the preparation committed.
        when(store.read()).thenReturn(reserved).thenAnswer(invocation -> persisted.get());

        coordinator.tick();

        verify(worker, times(1)).prepare(any());
        verify(plugin, times(1)).acquireSnapshot(eq(runtime), any());
        assertEquals(Phase.TRANSFERRING, coordinator.status().getPhase());
        assertEquals(Outcome.NONE, coordinator.status().getOutcome());
    }

    // ---------------------------------------------------------------- admission and transfer

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
    void theEndOfTheTransferMarksTheSinkInconsistentInTheSameTransaction() {
        SnapshotSyncLeaseRecord attempt = transfer();
        verify(metadata, never()).transferSnapshot(any(), anyLong());
        coordinator.transferComplete(attempt, 7);
        assertEquals(Phase.APPLYING, coordinator.status().getPhase());
        assertEquals(7, coordinator.status().getEndSequence());
        verify(metadata).transferSnapshot(txn, attempt.getSourceSnapshot());
    }

    // ---------------------------------------------------------------- reconciling on events

    /**
     * Every step on this side is a reconciliation away from the event that makes it due. Left to
     * the period alone, each of them would add up to a second to every snapshot sync, and to the
     * time the checkpointer stays frozen.
     */
    /** A step of the driver is running: the source is told to come back in a moment, not in seconds. */
    @Test
    void aProposalThatMeetsARunningStepIsToldToComeBackSoon() throws Exception {
        ExecutorService effects = Executors.newSingleThreadExecutor();
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            create(effects, 0, 0);
            coordinator.start(proposal());
            doAnswer(invocation -> {
                running.countDown();
                release.await(10, TimeUnit.SECONDS);
                return null;
            }).when(worker).prepare(any());
            coordinator.tick(); // Preparation starts, and holds the worker.
            assertTrue(running.await(10, TimeUnit.SECONDS));
            attemptLsb++;       // Another proposal than the one being prepared.

            LogReplicationBusyException busy = assertThrows(LogReplicationBusyException.class,
                    () -> coordinator.start(proposal()));
            assertEquals(LogReplicationBusyResponseMsg.Reason.ADMISSION_CLOSED, busy.getResponse().getReason());
            assertEquals(SnapshotLeaseCoordinator.MOMENTARY_RETRY_AFTER_MS, busy.getResponse().getRetryAfterMs());
        } finally {
            release.countDown();
            drain(effects);
            effects.shutdownNow();
        }
    }

    @Test
    void aReservationAsksForItsPreparationAtOnce() {
        long before = coordinator.requestedReconciliations.get();
        coordinator.start(proposal());
        assertEquals(Phase.PREPARING, coordinator.status().getPhase());
        assertEquals(before + 1, coordinator.requestedReconciliations.get());

        coordinator.start(proposal());
        assertEquals(before + 1, coordinator.requestedReconciliations.get(), "a duplicate START changes nothing");
    }

    /** A reconciliation does nothing while a message is being received, so it is asked for after. */
    @Test
    void theEndOfATransferAsksForItsApplyOnceTheMessageIsDone() {
        SnapshotSyncLeaseRecord attempt = transfer();
        LogReplicationEntryMetadataMsg entry = proposal().toBuilder().setAttemptGeneration(attempt.getGeneration()).build();
        coordinator.enterTransfer(entry);
        coordinator.exitTransfer();
        long before = coordinator.requestedReconciliations.get();

        coordinator.enterTransfer(entry);
        coordinator.exitTransfer();
        assertEquals(before, coordinator.requestedReconciliations.get(), "an ordinary message asks for nothing");

        coordinator.enterTransfer(entry);
        coordinator.transferComplete(attempt, 7);
        assertEquals(before, coordinator.requestedReconciliations.get(), "not while the message is being received");
        coordinator.exitTransfer();
        assertEquals(before + 1, coordinator.requestedReconciliations.get());
    }

    @Test
    void anAbandonmentAsksForItsReleaseAtOnce() {
        transfer();
        long before = coordinator.requestedReconciliations.get();
        coordinator.abandon("test");
        assertEquals(Phase.ABORTING, coordinator.status().getPhase());
        assertEquals(before + 1, coordinator.requestedReconciliations.get());

        // A cancellation that comes after the attempt ended abandons nothing: nothing is due, and
        // the worker, which may be releasing that attempt, is left alone.
        coordinator.abandon("again");
        assertEquals(before + 1, coordinator.requestedReconciliations.get());
        assertEquals("test", coordinator.status().getFailure());
    }

    /**
     * An effect that moved the lease on makes the next step due. One that failed and left it where
     * it was must be retried by the period: asking again at once would spin.
     */
    @Test
    void onlyAnEffectThatMovedTheLeaseOnAsksForTheNextStep() {
        coordinator.start(proposal());
        long before = coordinator.requestedReconciliations.get();
        coordinator.tick(); // Preparation: PREPARING to TRANSFERRING.
        assertEquals(Phase.TRANSFERRING, coordinator.status().getPhase());
        assertEquals(before + 1, coordinator.requestedReconciliations.get());

        doThrow(new IllegalStateException("uncertain release")).when(plugin).releaseSnapshot(any(), any());
        coordinator.abandon("test");
        before = coordinator.requestedReconciliations.get();
        coordinator.tick(); // The release fails after draining: ABORTING to RELEASING.
        assertEquals(Phase.RELEASING, coordinator.status().getPhase());
        assertEquals(before + 1, coordinator.requestedReconciliations.get());

        coordinator.tick(); // It fails again, and the lease stays where it was.
        coordinator.tick();
        assertEquals(Phase.RELEASING, coordinator.status().getPhase());
        assertEquals(before + 1, coordinator.requestedReconciliations.get(), "a failing release must not spin");
    }

    @Test
    void installingTheIncrementalWriterAsksForTheReleaseThatFollows() {
        SnapshotSyncLeaseRecord attempt = transfer();
        coordinator.transferComplete(attempt, 7);
        doAnswer(invocation -> {
            persisted.set(SnapshotSyncLease.completed(persisted.get()));
            return null;
        }).when(worker).apply(any());
        coordinator.tick(); // Apply completes the attempt.
        assertEquals(Outcome.COMPLETED, coordinator.status().getOutcome());
        long before = coordinator.requestedReconciliations.get();

        coordinator.tick(); // Installs the incremental writer; the lease itself does not move.
        verify(worker).completed(any());
        assertEquals(before + 1, coordinator.requestedReconciliations.get());
    }

    /** What a source is told when its START is reserved: come back soon, preparation has started. */
    @Test
    void aRefusalCanCarryItsOwnRetryHint() {
        assertEquals(SnapshotLeaseCoordinator.MOMENTARY_RETRY_AFTER_MS, coordinator.rejected(
                LogReplicationBusyResponseMsg.Reason.ADMISSION_CLOSED, SnapshotLeaseCoordinator.MOMENTARY_RETRY_AFTER_MS)
                .getResponse().getRetryAfterMs());
        assertTrue(coordinator.rejected(LogReplicationBusyResponseMsg.Reason.ADMISSION_CLOSED).getResponse().getRetryAfterMs()
                > SnapshotLeaseCoordinator.MOMENTARY_RETRY_AFTER_MS);
    }

    // ---------------------------------------------------------------- budget and inactivity

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
    void aSourceThatGoesSilentCostsTheIdleThresholdNotTheWholeBudget() {
        create(MoreExecutors.newDirectExecutorService(), 0, 200);
        SnapshotSyncLeaseRecord attempt = transfer();
        LogReplicationEntryMetadataMsg entry = proposal().toBuilder().setAttemptGeneration(attempt.getGeneration()).build();

        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(199));
        coordinator.tick();
        assertEquals(Phase.TRANSFERRING, coordinator.status().getPhase());

        // Accepted traffic, including a duplicate START, proves the source is alive.
        coordinator.enterTransfer(entry);
        coordinator.exitTransfer();
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(199));
        coordinator.tick();
        assertEquals(Phase.TRANSFERRING, coordinator.status().getPhase());
        coordinator.start(proposal().toBuilder().setAdmissionEpoch(1).build());
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(199));
        coordinator.tick();
        assertEquals(Phase.TRANSFERRING, coordinator.status().getPhase());

        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(1));
        coordinator.tick();
        assertEquals(Outcome.ABORTED, coordinator.status().getOutcome());
        assertTrue(coordinator.status().getFailure().contains("No snapshot traffic"), coordinator.status().getFailure());
        assertTrue(clock.get() < attempt.getDeadlineMs(), "well inside the budget");
    }

    @Test
    void aMessageBeingWrittenIsNotInactivity() {
        create(MoreExecutors.newDirectExecutorService(), 0, 200);
        SnapshotSyncLeaseRecord attempt = transfer();
        coordinator.enterTransfer(proposal().toBuilder().setAttemptGeneration(attempt.getGeneration()).build());
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(500)); // past the idle threshold, inside the budget
        coordinator.tick();
        assertEquals(Phase.TRANSFERRING, coordinator.status().getPhase(), "the sink is the slow one here");
        coordinator.exitTransfer();
    }

    @Test
    void applyNeverCountsAsInactivity() {
        create(MoreExecutors.newDirectExecutorService(), 3, 200);
        SnapshotSyncLeaseRecord attempt = transfer();
        coordinator.transferComplete(attempt, 0);
        doThrow(new IllegalStateException("temporary apply failure")).when(worker).apply(any());
        coordinator.tick();
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(500)); // past the idle threshold, inside the budget
        coordinator.tick();
        assertEquals(Phase.APPLYING, coordinator.status().getPhase(), "the source is expected to be silent during apply");
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

    // ---------------------------------------------------------------- apply

    @Test
    void transientApplyRetriesUseTheOriginalDeadlineAndRespectRetryEligibility() {
        create(MoreExecutors.newDirectExecutorService(), 2, 0);
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
        create(MoreExecutors.newDirectExecutorService(), 2, 0);
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

    // ---------------------------------------------------------------- cleanup and recovery

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

    /**
     * The compactor deletes the trigger whenever a cycle ends, whatever its outcome. Repeating the
     * request as soon as it is gone would run failing cycles back to back: each writes checkpoint
     * data, none trims, and the log grows faster than if the sink had never asked.
     */
    @Test
    void aCheckpointThatKeepsFailingIsNotRequestedBackToBack() {
        when(txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE,
                CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM)).thenReturn(new CorfuStoreEntry(
                CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM, null, null)); // Always consumed already.
        transfer();
        coordinator.abandon("failure");
        coordinator.tick();
        cycle.set(CheckpointingStatus.StatusType.FAILED);

        coordinator.tick();
        verify(txn, times(1)).putRecord(any(), eq(CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM), any(), isNull());
        for (int pass = 0; pass < 10; pass++) {
            coordinator.tick();
        }
        verify(txn, times(1)).putRecord(any(), eq(CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM), any(), isNull());

        long backoff = SnapshotLeaseCoordinator.CHECKPOINT_REQUEST_BACKOFF_MS;
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(backoff));
        coordinator.tick();
        verify(txn, times(2)).putRecord(any(), eq(CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM), any(), isNull());
        // Each further request waits twice as long as the previous one.
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(backoff));
        coordinator.tick();
        verify(txn, times(2)).putRecord(any(), eq(CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM), any(), isNull());
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(backoff));
        coordinator.tick();
        verify(txn, times(3)).putRecord(any(), eq(CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM), any(), isNull());
        assertEquals(Phase.RECOVERING, coordinator.status().getPhase());
    }

    @Test
    void aSatisfyingCycleIsNotFollowedByARequestForAnotherOneWhileItsTrimIsPending() {
        when(txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE,
                CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM)).thenReturn(new CorfuStoreEntry(
                CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM, null, null));
        transfer();
        coordinator.abandon("failure");
        coordinator.tick();
        // The compactor commits the end of a cycle, deletes the trigger, and only then trims.
        cycle.set(CheckpointingStatus.StatusType.COMPLETED);
        safeCut.set(100);

        coordinator.tick();
        coordinator.tick();
        verify(txn, never()).putRecord(any(), eq(CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM), any(), any());

        // A later cycle that fails does not take back what the satisfying one reclaimed.
        cycle.set(CheckpointingStatus.StatusType.FAILED);
        trim.set(101);
        clock.addAndGet(RECOVERY_MS);
        coordinator.tick();
        assertEquals(Phase.READY, coordinator.status().getPhase());
        verify(txn, never()).putRecord(any(), eq(CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM), any(), any());
    }

    @Test
    void aTrimThatNeverFollowsItsCycleIsEventuallyAskedForAgain() {
        when(txn.getRecord(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE,
                CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM)).thenReturn(new CorfuStoreEntry(
                CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM, null, null));
        transfer();
        coordinator.abandon("failure");
        coordinator.tick();
        cycle.set(CheckpointingStatus.StatusType.COMPLETED);
        safeCut.set(100);
        coordinator.tick();
        verify(txn, never()).putRecord(any(), eq(CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM), any(), any());

        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(SnapshotLeaseCoordinator.TRIM_WAIT_MS));
        coordinator.tick();
        verify(txn, times(1)).putRecord(any(), eq(CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM), any(), isNull());
    }

    /**
     * After a role change the lock holder still settles what this cluster's sink left behind, but a
     * cluster that is not a sink admits nothing, and a forced checkpoint with an immediate trim is
     * not something to impose on a source.
     */
    @Test
    void aClusterThatIsNotASinkSettlesItsRecordWithoutRequestingCheckpoints() {
        transfer();
        coordinator.sinkRole(false);
        coordinator.abandon("Topology changed");
        coordinator.tick();
        assertFalse(coordinator.status().getProtectionHeld());
        verify(plugin).releaseSnapshot(eq(runtime), any());
        coordinator.tick();
        assertEquals(Phase.READY, coordinator.status().getPhase());
        verify(txn, never()).putRecord(any(), eq(CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM), any(), any());

        try (var monitor = mockStatic(HealthMonitor.class)) {
            clock.addAndGet(10 * ALARM_MS);
            coordinator.tick();
            coordinator.checkHealth();
            monitor.verify(() -> HealthMonitor.reportIssue(any()), never());
        }
    }

    @Test
    void theTrimMarkIsOnlyFetchedOnceACheckpointCoversTheCut() {
        transfer();
        coordinator.abandon("failure");
        coordinator.tick();
        clearInvocations(runtime.getAddressSpaceView());
        cycle.set(CheckpointingStatus.StatusType.STARTED);
        coordinator.tick();
        cycle.set(CheckpointingStatus.StatusType.COMPLETED);
        safeCut.set(99);
        coordinator.tick();
        verify(runtime.getAddressSpaceView(), never()).getTrimMark();
        safeCut.set(100);
        coordinator.tick();
        verify(runtime.getAddressSpaceView()).getTrimMark();
    }

    @Test
    void admissionReopensWithoutACheckpointWhenNoCheckpointerCanRun() {
        // Compaction never ran on this cluster: its manager record was never written.
        transfer();
        coordinator.abandon("failure");
        coordinator.tick();
        cycle.set(null);
        coordinator.tick();
        assertEquals(Phase.READY, coordinator.status().getPhase());
        verify(txn, never()).putRecord(any(), eq(CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM), any(), any());
        cycle.set(CheckpointingStatus.StatusType.IDLE);
        attemptLsb++;

        // An operator disabled compaction: nothing will ever trim past the cut either.
        transfer();
        coordinator.abandon("failure");
        coordinator.tick();
        when(checkpointer.isCompactionDisabled()).thenReturn(true);
        coordinator.tick();
        assertEquals(Phase.READY, coordinator.status().getPhase());
        when(checkpointer.isCompactionDisabled()).thenReturn(false);
        attemptLsb++;
        assertEquals(Phase.TRANSFERRING, transfer().getPhase());
    }

    // ---------------------------------------------------------------- alarms and metrics

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
            monitor.verify(() -> HealthMonitor.resolveIssue(argThat(issue -> issue.getIssueId() == Issue.IssueId.SNAPSHOT_RECOVERY_BLOCKED)));
        }
    }

    @Test
    void aBlockedRecoveryNamesTheTablesOfTheFailedCheckpoint() {
        when(txn.keySet(any(Table.class))).thenReturn(Collections.singleton(failingTable));
        transfer();
        coordinator.abandon("failure");
        coordinator.tick();
        cycle.set(CheckpointingStatus.StatusType.FAILED);
        coordinator.tick();
        try (var monitor = mockStatic(HealthMonitor.class)) {
            clock.set(1100);
            coordinator.checkHealth();
            monitor.verify(() -> HealthMonitor.reportIssue(argThat(issue ->
                    issue.getIssueId() == Issue.IssueId.SNAPSHOT_RECOVERY_BLOCKED
                            && issue.getDescription().contains("last checkpoint cycle FAILED")
                            && issue.getDescription().contains("ns$stuck=FAILED"))));
        }
    }

    @Test
    void repeatedlyAbandonedAttemptsRaiseAnAlarmThatACompletedSnapshotClears() {
        try (var monitor = mockStatic(HealthMonitor.class)) {
            for (int attempt = 1; attempt <= SnapshotLeaseCoordinator.FAILING_ATTEMPTS_ALARM; attempt++) {
                transfer();
                coordinator.abandon("failure " + attempt);
                coordinator.checkHealth();
                if (attempt < SnapshotLeaseCoordinator.FAILING_ATTEMPTS_ALARM) {
                    monitor.verify(() -> HealthMonitor.reportIssue(argThat(issue ->
                            issue.getIssueId() == Issue.IssueId.SNAPSHOT_SYNC_FAILING)), never());
                }
                recover();
            }
            assertEquals(SnapshotLeaseCoordinator.FAILING_ATTEMPTS_ALARM, coordinator.status().getConsecutiveAborts());
            coordinator.checkHealth();
            monitor.verify(() -> HealthMonitor.reportIssue(argThat(issue ->
                    issue.getIssueId() == Issue.IssueId.SNAPSHOT_SYNC_FAILING)), atLeastOnce());

            SnapshotSyncLeaseRecord attempt = transfer();
            coordinator.transferComplete(attempt, 0);
            doAnswer(invocation -> { persisted.set(SnapshotSyncLease.completed(persisted.get())); return null; })
                    .when(worker).apply(any());
            coordinator.tick();
            coordinator.tick();
            assertEquals(0, coordinator.status().getConsecutiveAborts());
            coordinator.checkHealth();
            monitor.verify(() -> HealthMonitor.resolveIssue(argThat(issue ->
                    issue.getIssueId() == Issue.IssueId.SNAPSHOT_SYNC_FAILING)));
        }
    }

    @Test
    void gaugesReflectTheLeaseAndAreRemovedOnClose() {
        io.micrometer.core.instrument.simple.SimpleMeterRegistry registry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        try (var metrics = mockStatic(org.corfudb.common.metrics.micrometer.MeterRegistryProvider.class)) {
            metrics.when(org.corfudb.common.metrics.micrometer.MeterRegistryProvider::getInstance)
                    .thenReturn(java.util.Optional.of(registry));
            create(MoreExecutors.newDirectExecutorService(), 0, 0);
            var blocked = registry.get("logreplication.snapshot.recovery.blocked").gauge();
            var budget = registry.get("logreplication.snapshot.lease.budget.remaining.ms").gauge();
            var aborts = registry.get("logreplication.snapshot.lease.consecutive.aborts").gauge();
            assertEquals(0.0, blocked.value());
            assertEquals(0.0, budget.value());
            transfer();
            clock.addAndGet(400);
            coordinator.tick();
            assertEquals(DURATION_MS - 400, budget.value());
            coordinator.abandon("failure");
            coordinator.tick();
            assertEquals(0.0, budget.value());
            assertEquals(1.0, aborts.value());
            clock.addAndGet(ALARM_MS);
            coordinator.checkHealth();
            assertEquals(1.0, blocked.value());
            cycle.set(CheckpointingStatus.StatusType.COMPLETED);
            safeCut.set(100);
            trim.set(101);
            clock.addAndGet(RECOVERY_MS);
            coordinator.tick();
            coordinator.checkHealth();
            assertEquals(0.0, blocked.value());
            assertNotNull(registry.find("logreplication.snapshot.lease.phase.duration").tag("phase", "TRANSFERRING").timer());
            coordinator.close();
            assertNull(registry.find("logreplication.snapshot.recovery.blocked").gauge());
            assertNull(registry.find("logreplication.snapshot.lease.budget.remaining.ms").gauge());
        } finally { registry.close(); }
    }

    // ---------------------------------------------------------------- a worker that does not return

    @Test
    void interruptIgnoringApplyIsNotReplacedOrUnfrozen() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        create(executor, 0, 0);
        coordinator.start(proposal());
        coordinator.tick();
        drain(executor);
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
            // The sink cannot release it, but the checkpointer stops honoring it past the grace.
            long grace = SnapshotSyncLeaseStore.expiryGraceMs();
            assertTrue(SnapshotSyncLeaseStore.protectionActive(coordinator.status(), attempt.getDeadlineMs() + grace - 1));
            assertFalse(SnapshotSyncLeaseStore.protectionActive(coordinator.status(), attempt.getDeadlineMs() + grace));
        } finally {
            release.countDown();
            drain(executor);
        }
        coordinator.tick();
        drain(executor);
        assertEquals(Phase.RECOVERING, coordinator.status().getPhase());
    }

    @Test
    void theBudgetStillAppliesToAWorkerThatOutlivesALeadershipLoss() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        create(executor, 0, 0);
        coordinator.start(proposal());
        coordinator.tick();
        drain(executor);
        SnapshotSyncLeaseRecord attempt = coordinator.status();
        coordinator.transferComplete(attempt, 0);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(invocation -> {
            entered.countDown();
            while (release.getCount() != 0) {
                try { release.await(); } catch (InterruptedException ignored) { /* keeps running */ }
            }
            return null;
        }).when(worker).apply(any());
        try {
            coordinator.tick();
            assertTrue(entered.await(2, TimeUnit.SECONDS));
            coordinator.leadership(false);
            coordinator.leadership(true);
            coordinator.tick();
            assertEquals(Phase.APPLYING, persisted.get().getPhase(), "a running writer is never reset underneath itself");
            clock.set(attempt.getDeadlineMs());
            coordinator.tick();
            assertEquals(Phase.ABORTING, persisted.get().getPhase());
            assertEquals(Phase.NOT_READY, coordinator.status().getPhase(), "still draining, so nothing is admitted");
        } finally {
            release.countDown();
            drain(executor);
        }
        coordinator.tick();
        drain(executor);
        coordinator.tick();
        drain(executor);
        assertNotEquals(Phase.NOT_READY, coordinator.status().getPhase());
        assertFalse(persisted.get().getProtectionHeld());
    }
}
