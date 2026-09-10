package org.corfudb.runtime;

import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.LogReplication.SnapshotSyncLeaseRecord.Outcome;
import org.corfudb.runtime.LogReplication.SnapshotSyncLeaseRecord.Phase;
import org.corfudb.runtime.proto.RpcCommon.UuidMsg;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.*;

class SnapshotSyncLeaseTest {
    @Test
    void faultedCleanupRetainsProtectionAndCanBeReconciledAfterTheWorkerStops() {
        SnapshotSyncLeaseRecord abort = SnapshotSyncLease.abandon(reserve(), 200, "blocked worker");
        SnapshotSyncLeaseRecord fault = SnapshotSyncLease.faulted(abort);
        assertEquals(Phase.FAULTED, fault.getPhase());
        assertTrue(fault.getProtectionHeld());
        assertEquals(abort.getDeadlineMs(), fault.getDeadlineMs());
        assertSame(fault, SnapshotSyncLease.faulted(fault));
        assertSame(ready, SnapshotSyncLease.faulted(ready));
        assertEquals(Phase.RELEASING, SnapshotSyncLease.drained(fault, 90).getPhase());
        assertEquals(Phase.FAULTED, SnapshotSyncLease.faulted(SnapshotSyncLease.drained(abort, 90)).getPhase());
    }
    private final SnapshotSyncLeaseRecord ready = SnapshotSyncLease.initial("owner");
    private final LogReplicationEntryMetadataMsg start = LogReplicationEntryMetadataMsg.newBuilder()
            .setSyncRequestId(UuidMsg.newBuilder().setMsb(1).setLsb(2)).setSnapshotTimestamp(25)
            .setTopologyConfigID(3).setSnapshotLifecycleVersion(1).setAdmissionEpoch(1).build();

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
        assertTrue(SnapshotSyncLeaseStore.permitsTrim(state, 49));
        assertTrue(SnapshotSyncLeaseStore.permitsTrim(state, 50));
        assertFalse(SnapshotSyncLeaseStore.permitsTrim(state, 51));
        assertTrue(SnapshotSyncLeaseStore.permitsTrim(ready, 100000));
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
