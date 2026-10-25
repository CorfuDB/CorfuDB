package org.corfudb.runtime;

import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Outcome;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Phase;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotSyncLeaseTest {
    private final SnapshotSyncLeaseRecord ready = SnapshotSyncLease.seedIdle("owner", -1, 0);
    private final LogReplicationEntryMetadataMsg start = LogReplicationEntryMetadataMsg.newBuilder()
            .setSyncRequestId(UuidMsg.newBuilder().setMsb(1).setLsb(2)).setSnapshotTimestamp(25)
            .setTopologyConfigID(3).setSnapshotLifecycleVersion(1).setAdmissionEpoch(1).build();

    @Test
    void faultedCleanupRetainsProtectionAndCanBeReconciledAfterTheWorkerStops() {
        SnapshotSyncLeaseRecord abort = SnapshotSyncLease.abandon(reserve(), 200, "blocked worker");
        SnapshotSyncLeaseRecord fault = SnapshotSyncLease.faulted(abort);
        assertEquals(Phase.FAULTED, fault.getPhase());
        assertTrue(fault.getProtectionHeld());
        assertEquals(abort.getDeadlineMs(), fault.getDeadlineMs());
        assertSame(fault, SnapshotSyncLease.faulted(fault));
        assertSame(ready, SnapshotSyncLease.faulted(ready));
        assertEquals(Phase.FAULTED, SnapshotSyncLease.faulted(SnapshotSyncLease.drained(abort, 90)).getPhase());

        // An overdue cleanup does not alternate between two phases: FAULTED stays until the release,
        // which can then complete however long the cleanup took, and the failure is recorded once.
        SnapshotSyncLeaseRecord drained = SnapshotSyncLease.drained(fault, 90);
        assertEquals(Phase.FAULTED, drained.getPhase());
        assertEquals(90, drained.getRecoveryCut());
        assertSame(drained, SnapshotSyncLease.faulted(drained));
        assertEquals(fault.getFailure(), SnapshotSyncLease.drained(drained, 95).getFailure());
        SnapshotSyncLeaseRecord released = SnapshotSyncLease.released(drained, 300);
        assertEquals(Phase.RECOVERING, released.getPhase());
        assertFalse(released.getProtectionHeld());
        assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.released(fault, 300), "no recovery cut yet");
    }
    private SnapshotSyncLeaseRecord reserve() {
        return SnapshotSyncLease.reserve(ready, start, 100, 1000, 50, "protection");
    }

    @Test
    void recoveryRequiresReleaseMinimumIntervalSuccessfulCheckpointAndUsefulTrim() {
        SnapshotSyncLeaseRecord abandoned = SnapshotSyncLease.abandon(reserve(), 1100, "deadline");
        SnapshotSyncLeaseRecord drained = SnapshotSyncLease.drained(abandoned, 80);
        SnapshotSyncLeaseRecord released = SnapshotSyncLease.released(drained, 1200);
        assertEquals(Outcome.ABORTED, released.getOutcome());
        assertFalse(released.getProtectionHeld());
        assertEquals(released, SnapshotSyncLease.recovered(released, 1199, 60, 80, 81));
        assertEquals(released, SnapshotSyncLease.recovered(released, 1259, 60, 80, 81));
        assertEquals(released, SnapshotSyncLease.recovered(released, 1000000, 60, -1, 81));
        assertEquals(released, SnapshotSyncLease.recovered(released, 1000000, 60, 79, 81));
        assertEquals(released, SnapshotSyncLease.recovered(released, 1000000, 60, 80, 80));
        SnapshotSyncLeaseRecord reopened = SnapshotSyncLease.recovered(released, 1260, 60, 80, 81);
        assertEquals(Phase.READY, reopened.getPhase());
        assertTrue(reopened.getAdmissionEpoch() > start.getAdmissionEpoch());
        assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.reserve(reopened, start, 1300, 1000, 90, "next"));
    }

    @Test
    void progressRetriesAndSuccessCannotRenewTheOriginalBudget() {
        SnapshotSyncLeaseRecord preparing = reserve();
        SnapshotSyncLeaseRecord transferring = SnapshotSyncLease.prepared(preparing);
        SnapshotSyncLeaseRecord applying = SnapshotSyncLease.transferred(transferring);
        SnapshotSyncLeaseRecord completed = SnapshotSyncLease.completed(applying);
        assertEquals(1100, preparing.getDeadlineMs());
        for (SnapshotSyncLeaseRecord state : Arrays.asList(transferring, applying, completed)) {
            assertEquals(preparing.getDeadlineMs(), state.getDeadlineMs());
            assertTrue(state.getProtectionHeld());
        }
        assertSame(completed, SnapshotSyncLease.abandon(completed, 2000, "late timer"));
        assertEquals(Outcome.COMPLETED, completed.getOutcome());
        assertEquals(Phase.RELEASING, SnapshotSyncLease.drained(completed, 90).getPhase());
    }

    @Test
    void fullIdentityDistinguishesEqualSourceCutsAndUuidPrefixes() {
        SnapshotSyncLeaseRecord state = reserve();
        assertTrue(SnapshotSyncLease.matches(state, start));
        assertFalse(SnapshotSyncLease.matches(ready, start));
        assertFalse(SnapshotSyncLease.matches(state, start.toBuilder().setSyncRequestId(
                start.getSyncRequestId().toBuilder().setLsb(99)).build()));
        assertFalse(SnapshotSyncLease.matches(state, start.toBuilder().setSnapshotTimestamp(26).build()));
        assertFalse(SnapshotSyncLease.matches(state, start.toBuilder().setTopologyConfigID(4).build()));
    }

    @Test
    void rejectsNonFiniteOrDisabledBudgetsAndInvalidAdmission() {
        for (long duration : new long[]{0, -1, Long.MAX_VALUE}) {
            assertThrows(IllegalArgumentException.class, () -> SnapshotSyncLease.reserve(ready, start, 100, duration, 50, "x"));
        }
        for (LogReplicationEntryMetadataMsg invalid : Arrays.asList(
                start.toBuilder().setSnapshotLifecycleVersion(0).build(),
                start.toBuilder().setAdmissionEpoch(0).build(), start.toBuilder().clearSyncRequestId().build())) {
            assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.reserve(ready, invalid, 100, 1000, 50, "x"));
        }
        assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.reserve(reserve(), start, 100, 1000, 50, "x"));
        assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.reserve(
                ready.toBuilder().setProtectionHeld(true).build(), start, 100, 1000, 50, "x"));
    }

    @Test
    void everyWriterFenceRejectsStaleEffects() {
        SnapshotSyncLeaseRecord captured = SnapshotSyncLease.prepared(reserve());
        SnapshotSyncLease.checkWriter(captured, captured, 100, Phase.TRANSFERRING);
        for (UnaryOperator<SnapshotSyncLeaseRecord.Builder> mutation : Arrays.<UnaryOperator<SnapshotSyncLeaseRecord.Builder>>asList(
                b -> b.setOwnerId("successor"), b -> b.setGeneration(99),
                b -> b.setAttemptId(UuidMsg.newBuilder().setMsb(1).setLsb(3)),
                b -> b.setTopologyConfigId(99), b -> b.setSourceSnapshot(99),
                b -> b.setPhase(Phase.ABORTING), b -> b.setProtectionHeld(false))) {
            SnapshotSyncLeaseRecord changed = mutation.apply(captured.toBuilder()).build();
            assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.checkWriter(changed, captured, 101, Phase.TRANSFERRING));
        }
        assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.checkWriter(captured, captured, 99, Phase.TRANSFERRING));
        assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.checkWriter(captured, captured, 1100, Phase.TRANSFERRING));
    }

    @Test
    void aLostShadowMarkerCannotBecomeAnApplyableTransfer() {
        SnapshotSyncLeaseRecord transfer = SnapshotSyncLease.prepared(reserve()).toBuilder().setTransferredSequence(0).build();
        assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.transferred(transfer));
        assertEquals(Phase.APPLYING, SnapshotSyncLease.transferred(transfer.toBuilder().setFirstShadowAddress(51).build()).getPhase());
        assertEquals(Phase.APPLYING, SnapshotSyncLease.transferred(SnapshotSyncLease.prepared(reserve())).getPhase());
    }

    @Test
    void oldCheckpointCutCanFinishButNewCutCannotTrimProtectedWrites() {
        SnapshotSyncLeaseRecord state = reserve();
        assertTrue(SnapshotSyncLeaseStore.permitsTrim(state, 49, 500));
        assertTrue(SnapshotSyncLeaseStore.permitsTrim(state, 50, 500));
        assertFalse(SnapshotSyncLeaseStore.permitsTrim(state, 51, 500));
        assertTrue(SnapshotSyncLeaseStore.permitsTrim(ready, 100000, 500));
    }

    @Test
    void theCheckpointerIgnoresProtectionOnlyPastItsDeadlinePlusTheGrace() {
        SnapshotSyncLeaseRecord held = reserve(); // admitted at 100, deadline 1100
        long grace = SnapshotSyncLeaseStore.expiryGraceMs();
        assertFalse(SnapshotSyncLease.protectionExpired(held, 1100, grace), "the deadline alone is the sink's business");
        assertFalse(SnapshotSyncLease.protectionExpired(held, 1100 + grace - 1, grace));
        assertTrue(SnapshotSyncLease.protectionExpired(held, 1100 + grace, grace));
        assertTrue(SnapshotSyncLeaseStore.protectionActive(held, 1100 + grace - 1));
        assertFalse(SnapshotSyncLeaseStore.protectionActive(held, 1100 + grace));
        assertFalse(SnapshotSyncLeaseStore.permitsTrim(held, 51, 1100 + grace - 1));
        assertTrue(SnapshotSyncLeaseStore.permitsTrim(held, 51, 1100 + grace));

        // Abandonment and a faulted cleanup keep the original deadline, so the same backstop applies.
        SnapshotSyncLeaseRecord faulted = SnapshotSyncLease.faulted(SnapshotSyncLease.abandon(held, 1100, "deadline"));
        assertTrue(faulted.getProtectionHeld());
        assertTrue(SnapshotSyncLeaseStore.protectionActive(faulted, 1100 + grace - 1));
        assertFalse(SnapshotSyncLeaseStore.protectionActive(faulted, 1100 + grace));

        assertFalse(SnapshotSyncLease.protectionExpired(ready, Long.MAX_VALUE, grace), "nothing is held");
        assertFalse(SnapshotSyncLease.protectionExpired(held, Long.MAX_VALUE, -1), "a negative grace never expires");
        assertFalse(SnapshotSyncLease.protectionExpired(held.toBuilder().setDeadlineMs(0).build(), Long.MAX_VALUE, grace));
        SnapshotSyncLeaseRecord farFuture = held.toBuilder().setDeadlineMs(Long.MAX_VALUE - 1).build();
        assertFalse(SnapshotSyncLease.protectionExpired(farFuture, Long.MAX_VALUE - 1, grace), "no overflow");
        assertTrue(SnapshotSyncLease.protectionExpired(farFuture, Long.MAX_VALUE, grace));
    }

    @Test
    void theGraceCanBeOverriddenButNeverMadeNegative() {
        String previous = System.getProperty(SnapshotSyncLeaseStore.EXPIRY_GRACE_PROPERTY);
        try {
            System.setProperty(SnapshotSyncLeaseStore.EXPIRY_GRACE_PROPERTY, "5000");
            assertEquals(5000, SnapshotSyncLeaseStore.expiryGraceMs());
            System.setProperty(SnapshotSyncLeaseStore.EXPIRY_GRACE_PROPERTY, "-1");
            assertEquals(SnapshotSyncLeaseStore.DEFAULT_EXPIRY_GRACE_MS, SnapshotSyncLeaseStore.expiryGraceMs());
            System.clearProperty(SnapshotSyncLeaseStore.EXPIRY_GRACE_PROPERTY);
            assertEquals(SnapshotSyncLeaseStore.DEFAULT_EXPIRY_GRACE_MS, SnapshotSyncLeaseStore.expiryGraceMs());
        } finally {
            if (previous == null) {
                System.clearProperty(SnapshotSyncLeaseStore.EXPIRY_GRACE_PROPERTY);
            } else {
                System.setProperty(SnapshotSyncLeaseStore.EXPIRY_GRACE_PROPERTY, previous);
            }
        }
    }

    @Test
    void anIdleSeedAdmitsTheFirstAttemptAndNeverCollidesWithIt() {
        SnapshotSyncLeaseRecord idle = SnapshotSyncLease.seedIdle("owner", 40, 3);
        assertEquals(Phase.READY, idle.getPhase());
        assertEquals(Outcome.COMPLETED, idle.getOutcome());
        assertEquals(0, idle.getGeneration());
        assertEquals(40, idle.getSourceSnapshot());
        assertEquals(3, idle.getTopologyConfigId());
        assertFalse(idle.getProtectionHeld());
        assertFalse(idle.hasAttemptId());
        assertFalse(SnapshotSyncLease.active(idle));
        assertFalse(SnapshotSyncLease.matches(idle, start));

        SnapshotSyncLeaseRecord first = SnapshotSyncLease.reserve(idle, start, 100, 1000, 50, "protection");
        assertEquals(1, first.getGeneration());
        assertEquals(Outcome.NONE, first.getOutcome());
        assertTrue(first.getProtectionHeld());
    }

    @Test
    void aSupersededLegacyAttemptOwesACheckpointBeforeAdmissionReopens() {
        SnapshotSyncLeaseRecord seeded = SnapshotSyncLease.seedRecovering("owner", 1000, 80, 70, "superseded");
        assertEquals(70, seeded.getSourceSnapshot(), "the superseded snapshot is remembered, so it is superseded once");
        assertFalse(seeded.hasAttemptId());
        assertEquals(Phase.RECOVERING, seeded.getPhase());
        assertEquals(Outcome.ABORTED, seeded.getOutcome());
        assertFalse(seeded.getProtectionHeld());
        assertEquals(80, seeded.getRecoveryCut());
        assertEquals(1000, seeded.getReleasedAtMs());
        assertEquals("superseded", seeded.getFailure());
        assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.reserve(seeded, start, 1100, 1000, 90, "x"));
        assertEquals(seeded, SnapshotSyncLease.recovered(seeded, 5000, 60, 79, 81), "the checkpoint predates the cut");
        assertEquals(seeded, SnapshotSyncLease.recovered(seeded, 5000, 60, 80, 80), "the log was not trimmed past the cut");
        SnapshotSyncLeaseRecord reopened = SnapshotSyncLease.recovered(seeded, 5000, 60, 80, 81);
        assertEquals(Phase.READY, reopened.getPhase());
        assertEquals(Outcome.ABORTED, reopened.getOutcome(), "incremental traffic stays closed until a snapshot completes");
        assertThrows(IllegalArgumentException.class, () -> SnapshotSyncLease.seedRecovering("owner", 1000, -1, 70, "x"));
    }

    /**
     * During a rolling upgrade leadership can return to a node that still runs the pre-lease
     * protocol and ignores the record. A snapshot it left unfinished is superseded at the next takeover.
     */
    @Test
    void aSnapshotLeftByAPreLeaseLeaderClosesAdmissionAndForcesANewSnapshot() {
        // Admission was open and the last snapshot complete: both are no longer true.
        SnapshotSyncLeaseRecord idle = SnapshotSyncLease.supersededByLegacy(ready, 2000, 90, 75, "legacy");
        assertEquals(Phase.RECOVERING, idle.getPhase());
        assertEquals(Outcome.ABORTED, idle.getOutcome());
        assertEquals(90, idle.getRecoveryCut());
        assertEquals(75, idle.getSourceSnapshot());
        assertEquals(2000, idle.getReleasedAtMs());
        assertEquals(2000, idle.getAbortedAtMs());
        assertFalse(idle.getProtectionHeld());
        assertEquals("legacy", idle.getFailure());
        assertEquals(ready.getRevision() + 1, idle.getRevision());
        assertEquals(0, idle.getConsecutiveAborts(), "it is not a failure of a lease attempt");
        assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.reserve(idle, start, 2100, 1000, 95, "x"));

        // A recovery that was already owed only moves its cut forward, never back.
        SnapshotSyncLeaseRecord recovering = SnapshotSyncLease.released(
                SnapshotSyncLease.drained(SnapshotSyncLease.abandon(reserve(), 500, "cancel"), 80), 600);
        assertEquals(95, SnapshotSyncLease.supersededByLegacy(recovering, 2000, 95, 75, "legacy").getRecoveryCut());
        assertEquals(80, SnapshotSyncLease.supersededByLegacy(recovering, 2000, 60, 75, "legacy").getRecoveryCut());

        // Protection that is still held is left to the regular cleanup, which releases it.
        SnapshotSyncLeaseRecord completed = SnapshotSyncLease.completed(
                SnapshotSyncLease.transferred(SnapshotSyncLease.prepared(reserve())));
        SnapshotSyncLeaseRecord releasing = SnapshotSyncLease.supersededByLegacy(completed, 2000, 90, 75, "legacy");
        assertEquals(Phase.RELEASING, releasing.getPhase());
        assertEquals(Outcome.ABORTED, releasing.getOutcome());
        assertTrue(releasing.getProtectionHeld());
        assertEquals(Phase.RECOVERING, SnapshotSyncLease.released(SnapshotSyncLease.drained(releasing, 99), 2100).getPhase());
        SnapshotSyncLeaseRecord aborting = SnapshotSyncLease.abandon(reserve(), 500, "ownership");
        assertEquals(Phase.ABORTING, SnapshotSyncLease.supersededByLegacy(aborting, 2000, 90, 75, "legacy").getPhase());

        assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.supersededByLegacy(reserve(), 2000, 90, 75, "legacy"));
        assertThrows(IllegalArgumentException.class, () -> SnapshotSyncLease.supersededByLegacy(ready, 2000, -1, 75, "legacy"));
    }

    @Test
    void recoveryCanBeBypassedOnlyWhileRecovering() {
        SnapshotSyncLeaseRecord recovering = SnapshotSyncLease.released(
                SnapshotSyncLease.drained(SnapshotSyncLease.abandon(reserve(), 500, "cancel"), 80), 600);
        SnapshotSyncLeaseRecord reopened = SnapshotSyncLease.recoveredWithoutCheckpoint(recovering);
        assertEquals(Phase.READY, reopened.getPhase());
        assertEquals(recovering.getAdmissionEpoch() + 1, reopened.getAdmissionEpoch());
        assertTrue(reopened.getFailure().isEmpty());
        assertEquals(Outcome.ABORTED, reopened.getOutcome());
        for (SnapshotSyncLeaseRecord other : Arrays.asList(ready, reserve(), SnapshotSyncLease.abandon(reserve(), 500, "cancel"))) {
            assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.recoveredWithoutCheckpoint(other));
        }
    }

    @Test
    void abandonedAttemptsAreCountedUntilASnapshotCompletes() {
        SnapshotSyncLeaseRecord first = SnapshotSyncLease.abandon(reserve(), 500, "first");
        assertEquals(1, first.getConsecutiveAborts());
        assertSame(first, SnapshotSyncLease.abandon(first, 600, "duplicate"));
        SnapshotSyncLeaseRecord reopened = SnapshotSyncLease.recoveredWithoutCheckpoint(
                SnapshotSyncLease.released(SnapshotSyncLease.drained(first, 80), 700));
        LogReplicationEntryMetadataMsg retry = start.toBuilder().setAdmissionEpoch(reopened.getAdmissionEpoch()).build();
        SnapshotSyncLeaseRecord second = SnapshotSyncLease.reserve(reopened, retry, 800, 1000, 90, "protection");
        assertEquals(1, second.getConsecutiveAborts(), "admission does not forgive earlier failures");
        assertEquals(2, SnapshotSyncLease.abandon(second, 900, "second").getConsecutiveAborts());
        SnapshotSyncLeaseRecord done = SnapshotSyncLease.completed(SnapshotSyncLease.transferred(SnapshotSyncLease.prepared(second)));
        assertEquals(0, done.getConsecutiveAborts());
        SnapshotSyncLeaseRecord saturated = SnapshotSyncLease.abandon(
                second.toBuilder().setConsecutiveAborts(Integer.MAX_VALUE - 1).build(), 900, "saturated");
        assertEquals(Integer.MAX_VALUE, saturated.getConsecutiveAborts());
    }

    @Test
    void invalidTransitionsAndUnknownSchemaFailClosed() {
        assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.prepared(ready));
        assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.transferred(ready));
        assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.completed(ready));
        assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.drained(ready, 90));
        assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.released(ready, 100));
        SnapshotSyncLeaseRecord completed = SnapshotSyncLease.completed(SnapshotSyncLease.transferred(SnapshotSyncLease.prepared(reserve())));
        assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.released(completed, 100));
        assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.recovered(ready, 100, 1, 90, 91));
        SnapshotSyncLeaseRecord recovering = SnapshotSyncLease.released(SnapshotSyncLease.drained(completed, 90), 200);
        assertThrows(IllegalArgumentException.class, () -> SnapshotSyncLease.recovered(recovering, 300, -1, 90, 91));
        assertThrows(IllegalStateException.class, () -> SnapshotSyncLease.next(SnapshotSyncLeaseRecord.getDefaultInstance()));
    }

    @Test
    void allActivePhasesCanBeAbandonedAndProtectionSurvivesUntilRelease() {
        SnapshotSyncLeaseRecord preparing = reserve();
        for (SnapshotSyncLeaseRecord active : Arrays.asList(preparing, SnapshotSyncLease.prepared(preparing),
                SnapshotSyncLease.transferred(SnapshotSyncLease.prepared(preparing)))) {
            assertTrue(SnapshotSyncLease.active(active));
            SnapshotSyncLeaseRecord abandoned = SnapshotSyncLease.abandon(active, 500, "cancel");
            assertTrue(abandoned.getProtectionHeld());
            assertEquals(Outcome.ABORTED, abandoned.getOutcome());
            assertFalse(SnapshotSyncLease.active(abandoned));
            assertSame(abandoned, SnapshotSyncLease.abandon(abandoned, 600, "duplicate"));
        }
    }
}
