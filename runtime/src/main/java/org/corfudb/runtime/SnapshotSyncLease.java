package org.corfudb.runtime;

import org.corfudb.runtime.LogReplication.LogReplicationEntryMetadataMsg;
import org.corfudb.runtime.LogReplication.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.LogReplication.SnapshotSyncLeaseRecord.Outcome;
import org.corfudb.runtime.LogReplication.SnapshotSyncLeaseRecord.Phase;

/**
 * Pure transitions for the durable, domain-wide snapshot lease. Callers commit transitions
 * transactionally with the corresponding LR metadata changes. Time/progress never renews a lease.
 */
public final class SnapshotSyncLease {
    public static final int VERSION = 1;

    private SnapshotSyncLease() {
    }

    public static SnapshotSyncLeaseRecord initial(String owner) {
        return SnapshotSyncLeaseRecord.newBuilder().setSchemaVersion(VERSION).setOwnerId(owner)
                .setPhase(Phase.READY).setAdmissionEpoch(1).setRevision(1)
                .setSourceSnapshot(-1).setProtectedAfter(-1).setFirstShadowAddress(-1)
                .setTransferredSequence(-1).setRecoveryCut(-1).build();
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
        return next(state).setPhase(Phase.RELEASING).setOutcome(Outcome.COMPLETED).build();
    }

    public static SnapshotSyncLeaseRecord abandon(SnapshotSyncLeaseRecord state, long now, String reason) {
        if (!active(state)) {
            return state; // A deadline cannot turn durable success into failure.
        }
        return next(state).setPhase(Phase.ABORTING).setOutcome(Outcome.ABORTED)
                .setAbortedAtMs(now).setFailure(reason).build();
    }

    public static SnapshotSyncLeaseRecord drained(SnapshotSyncLeaseRecord state, long sinkCut) {
        if (state.getPhase() != Phase.ABORTING && state.getPhase() != Phase.RELEASING && state.getPhase() != Phase.FAULTED) {
            throw new LeaseRejectedException("Attempt has not finished or been abandoned");
        }
        return next(state).setPhase(Phase.RELEASING)
                .setRecoveryCut(Math.max(state.getRecoveryCut(), sinkCut)).build();
    }

    public static SnapshotSyncLeaseRecord faulted(SnapshotSyncLeaseRecord state) {
        if (state.getPhase() != Phase.ABORTING && state.getPhase() != Phase.RELEASING) {
            return state;
        }
        return next(state).setPhase(Phase.FAULTED).setFailure("Snapshot cleanup exceeded its bound: " + state.getFailure()).build();
    }

    public static SnapshotSyncLeaseRecord released(SnapshotSyncLeaseRecord state, long now) {
        requirePhase(state, Phase.RELEASING);
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
