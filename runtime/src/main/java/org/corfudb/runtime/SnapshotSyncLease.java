package org.corfudb.runtime;

import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Outcome;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord.Phase;

/**
 * Pure transitions for the durable, domain-wide snapshot lease. Callers commit transitions
 * transactionally with the corresponding LR metadata changes. Time/progress never renews a lease.
 */
public final class SnapshotSyncLease {
    public static final int VERSION = 1;

    private SnapshotSyncLease() {
    }

    /**
     * First record for a sink with no unfinished snapshot: either it never synced, or its last
     * snapshot was fully applied by the pre-lease protocol. Admission is open and incremental
     * traffic is admitted exactly as the pre-lease sink admitted it, subject to the incremental
     * writer's own metadata validation. Generation 0 never collides with an admitted attempt,
     * because {@link #reserve} always increments the generation first.
     */
    public static SnapshotSyncLeaseRecord seedIdle(String owner, long lastAppliedSnapshot, long topologyConfigId) {
        return seed(owner).setPhase(Phase.READY).setOutcome(Outcome.COMPLETED)
                .setSourceSnapshot(lastAppliedSnapshot).setTopologyConfigId(topologyConfigId).build();
    }

    /**
     * First record for a sink that was taken over while a pre-lease snapshot was unfinished. That
     * attempt is superseded: it is recorded as aborted, and admission stays closed until a
     * checkpoint and trim pass {@code recoveryCut}, so whatever the old attempt left in the log is
     * reclaimed before the checkpointer can be frozen again. The source timestamp of the superseded
     * snapshot is recorded, so that a later takeover does not supersede the same snapshot again.
     */
    public static SnapshotSyncLeaseRecord seedRecovering(String owner, long now, long recoveryCut,
                                                         long supersededSnapshot, String reason) {
        if (recoveryCut < 0) {
            throw new IllegalArgumentException("A recovery cut is required");
        }
        return seed(owner).setPhase(Phase.RECOVERING).setOutcome(Outcome.ABORTED).setSourceSnapshot(supersededSnapshot)
                .setAbortedAtMs(now).setReleasedAtMs(now).setRecoveryCut(recoveryCut).setFailure(reason).build();
    }

    private static SnapshotSyncLeaseRecord.Builder seed(String owner) {
        return SnapshotSyncLeaseRecord.newBuilder().setSchemaVersion(VERSION).setOwnerId(owner)
                .setAdmissionEpoch(1).setRevision(1)
                .setSourceSnapshot(-1).setProtectedAfter(-1).setFirstShadowAddress(-1)
                .setTransferredSequence(-1).setRecoveryCut(-1).setEndSequence(-1);
    }

    public static boolean active(SnapshotSyncLeaseRecord state) {
        return state.getPhase() == Phase.PREPARING || state.getPhase() == Phase.TRANSFERRING
                || state.getPhase() == Phase.APPLYING;
    }

    public static boolean matches(SnapshotSyncLeaseRecord state, LogReplicationEntryMetadataMsg entry) {
        return state.hasAttemptId() && state.getAttemptId().equals(entry.getSyncRequestId())
                && state.getSourceSnapshot() == entry.getSnapshotTimestamp()
                && state.getTopologyConfigId() == entry.getTopologyConfigID();
    }

    public static SnapshotSyncLeaseRecord reserve(SnapshotSyncLeaseRecord state,
                                                   LogReplicationEntryMetadataMsg start,
                                                   long now, long duration, long sinkCut,
                                                   String protectionId) {
        if (duration <= 0 || now > Long.MAX_VALUE - duration) {
            throw new IllegalArgumentException("A finite positive snapshot duration is required");
        }
        if (start.getSnapshotLifecycleVersion() != VERSION || state.getPhase() != Phase.READY
                || state.getProtectionHeld() || start.getAdmissionEpoch() != state.getAdmissionEpoch()
                || !start.hasSyncRequestId()) {
            throw new LeaseRejectedException("Snapshot admission is closed or stale");
        }
        return next(state).setPhase(Phase.PREPARING).setOutcome(Outcome.NONE)
                .setGeneration(Math.incrementExact(state.getGeneration()))
                .setAdmissionEpoch(Math.incrementExact(state.getAdmissionEpoch()))
                .setAttemptId(start.getSyncRequestId()).setExternalRequestId(start.getExternalRequestId())
                .setSourceSnapshot(start.getSnapshotTimestamp()).setTopologyConfigId(start.getTopologyConfigID())
                .setAdmittedAtMs(now).setDeadlineMs(now + duration).setProtectedAfter(sinkCut)
                .setProtectionHeld(true).setProtectionId(protectionId).setFirstShadowAddress(-1)
                .setTransferredSequence(-1).setRecoveryCut(-1).setReleasedAtMs(0).setAbortedAtMs(0)
                .setCleanupStartedAtMs(0).setEndSequence(-1)
                .setApplyRetries(0).setRetryAtMs(0).clearFailure().build();
    }

    public static SnapshotSyncLeaseRecord prepared(SnapshotSyncLeaseRecord state) {
        requirePhase(state, Phase.PREPARING);
        return next(state).setPhase(Phase.TRANSFERRING).build();
    }

    public static SnapshotSyncLeaseRecord transferred(SnapshotSyncLeaseRecord state) {
        requirePhase(state, Phase.TRANSFERRING);
        if (state.getTransferredSequence() >= 0 && state.getFirstShadowAddress() < 0) {
            throw new LeaseRejectedException("Missing durable shadow boundary");
        }
        return next(state).setPhase(Phase.APPLYING).build();
    }

    public static SnapshotSyncLeaseRecord completed(SnapshotSyncLeaseRecord state) {
        requirePhase(state, Phase.APPLYING);
        return next(state).setPhase(Phase.RELEASING).setOutcome(Outcome.COMPLETED)
                .setConsecutiveAborts(0).build();
    }

    public static SnapshotSyncLeaseRecord abandon(SnapshotSyncLeaseRecord state, long now, String reason) {
        if (!active(state)) {
            return state; // A deadline cannot turn durable success into failure.
        }
        return next(state).setPhase(Phase.ABORTING).setOutcome(Outcome.ABORTED)
                .setAbortedAtMs(now).setFailure(reason)
                .setConsecutiveAborts(Math.min(Integer.MAX_VALUE - 1, state.getConsecutiveAborts()) + 1).build();
    }

    /**
     * A node running the pre-lease protocol led this sink in between (leadership can bounce between
     * upgraded and not yet upgraded nodes during a rolling upgrade) and left a snapshot of its own
     * unfinished. Whatever the record said before, the sink is no longer known to hold a fully
     * applied snapshot: the outcome becomes ABORTED, which makes the source start over, and
     * admission stays closed until a checkpoint and trim pass {@code sinkCut}, so what that
     * snapshot left in the log is reclaimed first. Protection that is still held is released by the
     * regular cleanup, which records a recovery cut of its own. The source timestamp of the
     * superseded snapshot replaces the record's, so that it is superseded only once.
     */
    public static SnapshotSyncLeaseRecord supersededByLegacy(SnapshotSyncLeaseRecord state, long now, long sinkCut,
                                                             long supersededSnapshot, String reason) {
        if (active(state)) {
            throw new LeaseRejectedException("An active attempt must be abandoned first");
        }
        if (sinkCut < 0) {
            throw new IllegalArgumentException("A recovery cut is required");
        }
        SnapshotSyncLeaseRecord.Builder superseded = next(state).setOutcome(Outcome.ABORTED).setFailure(reason)
                .setSourceSnapshot(supersededSnapshot).setRecoveryCut(Math.max(state.getRecoveryCut(), sinkCut));
        if (state.getPhase() == Phase.READY || state.getPhase() == Phase.RECOVERING) {
            // Protection is not held in these phases; only the recovery gate has to close again.
            superseded.setPhase(Phase.RECOVERING).setAbortedAtMs(now).setReleasedAtMs(now);
        }
        return superseded.build();
    }

    /**
     * The attempt's writers have stopped: records the cut that a checkpoint and trim must pass before
     * the next snapshot. A FAULTED record stays FAULTED until it is released. It only marks that the
     * cleanup is overdue, and moving it back to RELEASING would make an overdue cleanup alternate
     * between the two phases on every reconciliation, rewriting the record each time and never
     * letting a release that takes longer than one reconciliation finish.
     */
    public static SnapshotSyncLeaseRecord drained(SnapshotSyncLeaseRecord state, long sinkCut) {
        if (state.getPhase() != Phase.ABORTING && state.getPhase() != Phase.RELEASING && state.getPhase() != Phase.FAULTED) {
            throw new LeaseRejectedException("Attempt has not finished or been abandoned");
        }
        return next(state).setPhase(state.getPhase() == Phase.FAULTED ? Phase.FAULTED : Phase.RELEASING)
                .setRecoveryCut(Math.max(state.getRecoveryCut(), sinkCut)).build();
    }

    public static SnapshotSyncLeaseRecord faulted(SnapshotSyncLeaseRecord state) {
        if (state.getPhase() != Phase.ABORTING && state.getPhase() != Phase.RELEASING) {
            return state;
        }
        return next(state).setPhase(Phase.FAULTED).setFailure("Snapshot cleanup exceeded its bound: " + state.getFailure()).build();
    }

    public static SnapshotSyncLeaseRecord released(SnapshotSyncLeaseRecord state, long now) {
        if (state.getPhase() != Phase.RELEASING && state.getPhase() != Phase.FAULTED) {
            throw new LeaseRejectedException("Expected RELEASING or FAULTED, found " + state.getPhase());
        }
        if (state.getRecoveryCut() < 0) {
            throw new LeaseRejectedException("Recovery cut must be recorded before release");
        }
        return next(state).setPhase(Phase.RECOVERING).setProtectionHeld(false).setReleasedAtMs(now).build();
    }

    public static SnapshotSyncLeaseRecord recovered(SnapshotSyncLeaseRecord state, long now,
                                                    long minimumRecoveryMs, long successfulCut,
                                                    long trimMark) {
        requirePhase(state, Phase.RECOVERING);
        if (minimumRecoveryMs < 0) {
            throw new IllegalArgumentException("Negative recovery interval");
        }
        if (now < state.getReleasedAtMs() || now - state.getReleasedAtMs() < minimumRecoveryMs
                || successfulCut < state.getRecoveryCut() || trimMark <= state.getRecoveryCut()) {
            return state;
        }
        return next(state).setPhase(Phase.READY).clearFailure()
                .setAdmissionEpoch(Math.incrementExact(state.getAdmissionEpoch())).build();
    }

    /**
     * Reopens admission without waiting for a checkpoint. Only valid when no checkpointer can run
     * at all (compaction was never started on this cluster, or an operator disabled it): there is
     * then no checkpointer to starve and no trim that could ever pass the recovery cut, so waiting
     * would only keep replication closed forever.
     */
    public static SnapshotSyncLeaseRecord recoveredWithoutCheckpoint(SnapshotSyncLeaseRecord state) {
        requirePhase(state, Phase.RECOVERING);
        return next(state).setPhase(Phase.READY).clearFailure()
                .setAdmissionEpoch(Math.incrementExact(state.getAdmissionEpoch())).build();
    }

    /**
     * True once held protection is older than its deadline plus {@code graceMs}. The checkpointer
     * uses this as a backstop that does not depend on the sink: no writer of the attempt can commit
     * after the deadline (see {@link #checkWriter}), so whatever the attempt wrote is garbage by
     * then and reclaiming it is safe even if the sink is hung or has no leader. The grace absorbs
     * clock skew between the sink and the checkpointer.
     */
    public static boolean protectionExpired(SnapshotSyncLeaseRecord state, long now, long graceMs) {
        if (!state.getProtectionHeld() || state.getDeadlineMs() <= 0 || graceMs < 0) {
            return false;
        }
        long limit = state.getDeadlineMs() > Long.MAX_VALUE - graceMs
                ? Long.MAX_VALUE : state.getDeadlineMs() + graceMs;
        return now >= limit;
    }

    /** Reject old-owner, old-attempt and post-abort transactions before any effect is appended. */
    public static void checkWriter(SnapshotSyncLeaseRecord state, SnapshotSyncLeaseRecord captured,
                                   long now, Phase phase) {
        if (!state.getOwnerId().equals(captured.getOwnerId())
                || state.getGeneration() != captured.getGeneration()
                || !state.getAttemptId().equals(captured.getAttemptId())
                || state.getTopologyConfigId() != captured.getTopologyConfigId()
                || state.getSourceSnapshot() != captured.getSourceSnapshot()
                || state.getPhase() != phase || !state.getProtectionHeld()
                || now < state.getAdmittedAtMs() || now >= state.getDeadlineMs()) {
            throw new LeaseRejectedException("Snapshot writer was fenced or its deadline expired");
        }
    }

    public static SnapshotSyncLeaseRecord.Builder next(SnapshotSyncLeaseRecord state) {
        if (state.getSchemaVersion() != VERSION) {
            throw new LeaseRejectedException("Unsupported snapshot lease schema");
        }
        return state.toBuilder().setRevision(Math.incrementExact(state.getRevision()));
    }

    private static void requirePhase(SnapshotSyncLeaseRecord state, Phase expected) {
        if (state.getPhase() != expected) {
            throw new LeaseRejectedException("Expected " + expected + ", found " + state.getPhase());
        }
    }

    public static class LeaseRejectedException extends IllegalStateException {
        public LeaseRejectedException(String message) {
            super(message);
        }
    }
}
