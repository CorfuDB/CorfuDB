package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.infrastructure.health.HealthMonitor;
import org.corfudb.infrastructure.health.Issue;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuCompactorManagement.SnapshotSyncLeaseRecord;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.DistributedCheckpointer;
import org.corfudb.runtime.SnapshotSyncLeaseStore;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.proto.RpcCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.corfudb.infrastructure.health.Component.COMPACTOR;
import static org.corfudb.infrastructure.health.Issue.IssueId.CHECKPOINT_FROZEN;
import static org.corfudb.infrastructure.health.Issue.IssueId.CHECKPOINT_STALLED;
import static org.corfudb.infrastructure.health.Issue.IssueId.COMPACTION_CYCLE_FAILED;
import static org.corfudb.infrastructure.health.Issue.IssueId.SNAPSHOT_RECOVERY_BLOCKED;
import static org.corfudb.infrastructure.health.Issue.IssueId.SNAPSHOT_SYNC_FAILING;
import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * This class does all services that the coordinator has to perform. The actions performed by the coordinator are -
 * 1. Set CompactionManager's status as STARTED, marking the start of a compaction cycle
 * 2. Validate liveness of checkpointing tables in order to detect slow or dead clients
 * 3. Set CompactoinManager's status as COMPLETED or FAILED based on the checkpointing status of all the tables. This
 * marks the end of the compaction cycle
 */
public class CompactorLeaderServices {
    private final CorfuRuntime corfuRuntime;
    private final CorfuStore corfuStore;
    private final String nodeEndpoint;
    private final TrimLog trimLog;
    private final Logger log;
    private final LivenessValidator livenessValidator;
    @Getter
    private final CompactorMetadataTables compactorMetadataTables;

    public static final int MAX_RETRIES = 5;

    /**
     * Time a table is allowed to sit queued (IDLE, never picked up by a checkpointer) within an
     * active compaction cycle before it is reported as a stalled-checkpoint health issue.
     *
     * This complements the existing liveness checks in {@link #validateLiveness()}, which only
     * monitor checkpoints that have already started (i.e. present in ActiveCheckpointsTable). A
     * table that never gets a chance to start -- e.g. because an external freeze (such as an
     * in-progress log replication snapshot sync being repeatedly canceled and restarted) keeps
     * getting reasserted before the compactor reaches it -- never shows up as "active" and is
     * otherwise invisible until an operator notices the cycle itself is taking unexpectedly long.
     */
    private static final Duration STALLED_CHECKPOINT_THRESHOLD = Duration.ofMinutes(5);

    private static final int MAX_STALLED_TABLES_IN_ISSUE_DESCRIPTION = 10;

    private static final String NO_STALLED_CHECKPOINTS_DESC = "No stalled checkpoints";

    /**
     * How long snapshot sync protection may still be held past its own deadline before it is
     * reported. The log replication sink releases protection at the deadline at the latest, so
     * anything older means the sink is not reconciling (hung worker, no leader).
     */
    private static final Duration OVERDUE_PROTECTION_THRESHOLD = Duration.ofMinutes(5);

    /** How long the operator freeze token may freeze checkpointing before it is reported. */
    private static final Duration FROZEN_TOKEN_THRESHOLD = Duration.ofMinutes(60);

    private static final String NOT_FROZEN_DESC = "Checkpointing is not frozen for too long";

    /** How long cleanup or recovery after a snapshot sync may keep the next one from being admitted. */
    private static final Duration BLOCKED_SNAPSHOT_THRESHOLD = Duration.ofMinutes(30);

    /** Snapshot sync attempts abandoned in a row after which snapshot sync is reported as failing. */
    private static final int FAILING_SNAPSHOT_ATTEMPTS = 3;

    // What this leader currently reports, with the description it was reported with.
    private final Map<Issue.IssueId, String> reported = new EnumMap<>(Issue.IssueId.class);

    // Progress of the checkpointers in the current cycle, see checkForStalledCheckpoints().
    private long observedCycle = -1;
    private int waitingTables;
    private long lastCheckpointProgressMs;

    /**
     * This enum contains the leader's initCompactionCycle status
     * If the status is SUCCESS, the compaction cycle has been started
     * If the status is FAIL, the compaction cycle startup has failed
     */
    public static enum LeaderInitStatus {
        SUCCESS,
        FAIL
    }

    public CompactorLeaderServices(CorfuRuntime corfuRuntime, String nodeEndpoint, CorfuStore corfuStore,
                                   LivenessValidator livenessValidator)
            throws Exception {
        this.compactorMetadataTables = new CompactorMetadataTables(corfuStore);
        this.corfuRuntime = corfuRuntime;
        this.nodeEndpoint = nodeEndpoint;
        this.corfuStore = corfuStore;
        this.livenessValidator = livenessValidator;
        this.trimLog = new TrimLog();
        this.log = LoggerFactory.getLogger("compactor-leader");
    }

    /**
     * Trim and mark the start of the compaction cycle and populate CheckpointStatusTable
     * with all the tables in the registry.
     *
     * @return compaction cycle start status
     */
    public LeaderInitStatus initCompactionCycle() {
        long minAddressBeforeCycleStarts;
        log.info("=============Initiating Distributed Compaction============");

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            // CorfuStore is WRITE_AFTER_WRITE: include the shared guard in the write set,
            // not just the read set, to serialize this cycle with snapshot admission.
            // Protection that outlived its deadline plus the grace does not block a new cycle:
            // no writer of that attempt can commit any more, so the cycle only reclaims garbage.
            SnapshotSyncLeaseRecord lease = SnapshotSyncLeaseStore.readForRetention(txn);
            if (SnapshotSyncLeaseStore.protectionActive(lease, System.currentTimeMillis())) {
                txn.commit();
                log.info("Compaction cycle not started: a log replication snapshot sync protects the log "
                        + "(generation={}, phase={}, deadlineMs={})", lease.getGeneration(), lease.getPhase(),
                        lease.getDeadlineMs());
                return LeaderInitStatus.FAIL;
            }
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();

            if (managerStatus != null && managerStatus.getStatus() == StatusType.STARTED) {
                txn.commit();
                log.warn("Compaction cycle already started");
                return LeaderInitStatus.FAIL;
            }
            SnapshotSyncLeaseStore.write(txn, lease);

            long newCycleCount = managerStatus == null ? 0 : managerStatus.getCycleCount() + 1;
            List<TableName> tableNames = new ArrayList<>(corfuStore.listTables(null));
            CheckpointingStatus idleStatus = buildCheckpointStatus(StatusType.IDLE, nodeEndpoint, newCycleCount);

            txn.clear(CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME);
            txn.clear(CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME);
            //Populate CheckpointingStatusTable
            for (TableName table : tableNames) {
                txn.putRecord(compactorMetadataTables.getCheckpointingStatusTable(), table, idleStatus, null);
            }

            // Also record the minToken as the earliest token BEFORE checkpointing is initiated
            // We take the current transaction's snapshot timestamp as this safe point
            // Either the transaction fails and gets retried or it gets committed at which point
            // future sequencer regressions will not regress to an earlier point.
            // This is the safest point to trim from, since all data up to this point will surely
            // be included in the upcoming checkpoint cycle
            minAddressBeforeCycleStarts = txn.getTxnSequence();
            txn.putRecord(compactorMetadataTables.getCompactionControlsTable(), CompactorMetadataTables.MIN_CHECKPOINT,
                    RpcCommon.TokenMsg.newBuilder()
                            .setSequence(minAddressBeforeCycleStarts)
                            .build(),
                    null);

            CheckpointingStatus newManagerStatus = buildCheckpointStatus(StatusType.STARTED,
                    tableNames.size(), System.currentTimeMillis(), newCycleCount); // put the current time when cycle starts
            txn.putRecord(compactorMetadataTables.getCompactionManagerTable(), CompactorMetadataTables.COMPACTION_MANAGER_KEY,
                    newManagerStatus, null);

            txn.commit();
        } catch (Exception e) {
            log.error("Exception while initiating Compaction cycle {}. Stack Trace {}", e, e.getStackTrace());
            return LeaderInitStatus.FAIL;
        }
        log.info("Init compaction cycle is successful. Min token {}", minAddressBeforeCycleStarts);
        return LeaderInitStatus.SUCCESS;
    }

    /**
     * Validates the liveness of any on-going checkpoint of a table
     * ActiveCheckpointTable contains the list of tables for which checkpointing has started. This method is scheduled
     * to execute continuously by the leader, which monitors the checkpoint activity of the tables present in
     * ActiveCheckpointTable.
     * if there are no tables present,
     * ... check for idle tables in CheckpointStatusTable (To track progress when tables are checkpointed rapidly)
     * ... if there's no progress for timeout ms, call finishCompactionCycle() to mark the end of the cycle
     * if there are any slow checkpointers,
     * ... monitor checkpointing of the table by observing if the checkpointStream's tail moves forward
     * ... if it does not move forward for timeout ms, then mark it as failed
     * Also, when checkpoint of a table is found to be failed, the cycle is immediately marked as failed.
     */
    public void validateLiveness() {
        List<TableName> activeCheckpointTables = getAllActiveCheckpointsTables();
        long currentTime = System.currentTimeMillis();

        if (activeCheckpointTables.isEmpty()) {
            LivenessValidator.Status statusToChange = livenessValidator.shouldChangeManagerStatus(
                    Duration.ofMillis(currentTime));
            if (statusToChange == LivenessValidator.Status.FINISH) {
                log.info("Invoking finishCompactionCycle");
                finishCompactionCycle();
            }
        }

        for (TableName table : activeCheckpointTables) {
            if (!livenessValidator.isTableCheckpointActive(table, Duration.ofMillis(currentTime)) &&
                    checkFailureAndFinishCompactionCycle(table)) {
                log.info("Invoking finishCompactionCycle");
                finishCompactionCycle();
                break;
            }
        }

        checkForStalledCheckpoints(currentTime, !activeCheckpointTables.isEmpty());
    }

    /**
     * A cycle is stalled when tables are still waiting to be checkpointed and nothing has moved for
     * the threshold: no table is being checkpointed and the number waiting has not gone down.
     * Checkpointers work through the tables one after another, so tables that wait are what every
     * healthy cycle looks like for most of its duration; time since the cycle started says nothing.
     */
    @VisibleForTesting
    void checkForStalledCheckpoints(long currentTimeMillis, boolean checkpointsActive) {
        Optional<CheckpointingStatus> managerStatus;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            managerStatus = Optional.ofNullable((CheckpointingStatus) txn.getRecord(
                    CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload());
            txn.commit();
        } catch (Exception e) {
            log.warn("Unable to acquire Manager Status while checking for stalled checkpoints, ", e);
            return;
        }

        if (!managerStatus.isPresent() || managerStatus.get().getStatus() != StatusType.STARTED) {
            // No cycle in progress right now, so nothing can be "stalled".
            observedCycle = -1;
            resolve(CHECKPOINT_STALLED, NO_STALLED_CHECKPOINTS_DESC);
            return;
        }

        List<TableName> stalledTables = new ArrayList<>();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            List<TableName> tableNames = new ArrayList<>(txn.keySet(compactorMetadataTables.getCheckpointingStatusTable()));
            for (TableName table : tableNames) {
                CheckpointingStatus tableStatus = (CheckpointingStatus) txn.getRecord(
                        CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME, table).getPayload();
                if (tableStatus != null && tableStatus.getStatus() == StatusType.IDLE) {
                    stalledTables.add(table);
                }
            }
            txn.commit();
        } catch (Exception e) {
            log.warn("Unable to acquire checkpoint status while checking for stalled checkpoints, ", e);
            return;
        }

        long cycle = managerStatus.get().getCycleCount();
        if (cycle != observedCycle || checkpointsActive || stalledTables.size() < waitingTables) {
            observedCycle = cycle;
            lastCheckpointProgressMs = currentTimeMillis;
        }
        waitingTables = stalledTables.size();

        if (stalledTables.isEmpty() || currentTimeMillis - lastCheckpointProgressMs < STALLED_CHECKPOINT_THRESHOLD.toMillis()) {
            resolve(CHECKPOINT_STALLED, NO_STALLED_CHECKPOINTS_DESC);
            return;
        }

        String tableList = stalledTables.stream()
                .limit(MAX_STALLED_TABLES_IN_ISSUE_DESCRIPTION)
                .map(t -> t.getNamespace() + "$" + t.getTableName())
                .collect(Collectors.joining(", "));
        report(CHECKPOINT_STALLED, String.format(
                "%d table(s) requested for checkpointing but not started, and no checkpoint has made progress for "
                        + "%d minutes in the current compaction cycle (cycle started at epoch millis %d): %s%s",
                stalledTables.size(), STALLED_CHECKPOINT_THRESHOLD.toMinutes(), managerStatus.get().getTimeTaken(),
                tableList, stalledTables.size() > MAX_STALLED_TABLES_IN_ISSUE_DESCRIPTION ? ", ..." : ""));
    }

    /**
     * Reports, on every orchestrator pass of the leader, what the snapshot lease and the freeze token
     * say about checkpointing, whether or not a cycle is active: a freeze prevents a cycle from
     * starting in the first place.
     *
     * <p>The conditions of log replication itself are reported here too. The health monitor only
     * exists in the Corfu server process, so the log replication process, which detects them first,
     * can only log them; the lease is durable and shared, and this leader reads it anyway.
     */
    public void checkForProlongedFreeze(long currentTimeMillis) {
        SnapshotSyncLeaseRecord lease;
        RpcCommon.TokenMsg freezeToken;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            lease = SnapshotSyncLeaseStore.readForRetention(txn);
            freezeToken = (RpcCommon.TokenMsg) txn.getRecord(
                    CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.FREEZE_TOKEN).getPayload();
            txn.commit();
        } catch (Exception e) {
            log.warn("Unable to evaluate how long checkpointing has been frozen, ", e);
            return;
        }

        boolean protectionActive = SnapshotSyncLeaseStore.protectionActive(lease, currentTimeMillis);
        long pastDeadlineMs = lease.getProtectionHeld() && lease.getDeadlineMs() > 0
                ? currentTimeMillis - lease.getDeadlineMs() : -1;
        if (protectionActive && pastDeadlineMs >= OVERDUE_PROTECTION_THRESHOLD.toMillis()) {
            report(CHECKPOINT_FROZEN, String.format("Snapshot sync protection is still held %d minutes past its deadline "
                            + "(generation=%d, phase=%s, failure=%s). Checkpointing stays frozen until the log "
                            + "replication sink releases it or the grace ends.",
                    Duration.ofMillis(pastDeadlineMs).toMinutes(), lease.getGeneration(), lease.getPhase(), lease.getFailure()));
        } else if (freezeToken != null
                && currentTimeMillis - freezeToken.getSequence() >= FROZEN_TOKEN_THRESHOLD.toMillis()) {
            report(CHECKPOINT_FROZEN, String.format("Checkpointing has been frozen by the freeze token for %d minutes",
                    Duration.ofMillis(currentTimeMillis - freezeToken.getSequence()).toMinutes()));
        } else {
            resolve(CHECKPOINT_FROZEN, NOT_FROZEN_DESC);
        }

        String blocked = describeBlockedSnapshotSync(lease, protectionActive, pastDeadlineMs, currentTimeMillis);
        if (blocked != null) {
            report(SNAPSHOT_RECOVERY_BLOCKED, blocked);
        } else {
            resolve(SNAPSHOT_RECOVERY_BLOCKED, "Snapshot sync admission is not blocked");
        }

        if (lease.getConsecutiveAborts() >= FAILING_SNAPSHOT_ATTEMPTS) {
            report(SNAPSHOT_SYNC_FAILING, String.format("%d log replication snapshot sync attempts to this cluster were "
                            + "abandoned in a row (generation=%d, phase=%s, last failure=%s)",
                    lease.getConsecutiveAborts(), lease.getGeneration(), lease.getPhase(), lease.getFailure()));
        } else {
            resolve(SNAPSHOT_SYNC_FAILING, "Snapshot sync is not failing repeatedly");
        }
    }

    /** What keeps the next snapshot sync from being admitted for longer than it should, if anything. */
    private static String describeBlockedSnapshotSync(SnapshotSyncLeaseRecord lease, boolean protectionActive,
                                                      long pastDeadlineMs, long now) {
        String state = String.format("generation=%d, phase=%s, recoveryCut=%d, failure=%s",
                lease.getGeneration(), lease.getPhase(), lease.getRecoveryCut(), lease.getFailure());
        long blockedMs = BLOCKED_SNAPSHOT_THRESHOLD.toMillis();
        if (lease.getProtectionHeld() && !protectionActive) {
            return String.format("Log replication left snapshot sync protection held %d minutes past its deadline (%s). "
                            + "Checkpointing ignores it. The sink has no log replication leader, or its leader is hung.",
                    Duration.ofMillis(pastDeadlineMs).toMinutes(), state);
        }
        switch (lease.getPhase()) {
            case FAULTED:
                return "The log replication sink cannot clean up after a snapshot sync attempt (" + state + ")";
            case ABORTING:
                return lease.getAbortedAtMs() > 0 && now - lease.getAbortedAtMs() >= blockedMs
                        ? "The log replication sink has not cleaned up an abandoned snapshot sync attempt (" + state + ")" : null;
            case RELEASING:
                return lease.getCleanupStartedAtMs() > 0 && now - lease.getCleanupStartedAtMs() >= blockedMs
                        ? "The log replication sink has not released a finished snapshot sync attempt (" + state + ")" : null;
            case RECOVERING:
                return lease.getReleasedAtMs() > 0 && now - lease.getReleasedAtMs() >= blockedMs
                        ? "No snapshot sync is admitted until a compaction cycle completes and the log is trimmed past "
                        + "the recovery cut, which has not happened for " + Duration.ofMillis(now - lease.getReleasedAtMs()).toMinutes()
                        + " minutes (" + state + ")" : null;
            default:
                return null;
        }
    }

    /**
     * The health status keeps the description an issue was first reported with, so an issue whose
     * description changed is resolved and reported again. Logged when it appears, not on every pass.
     */
    private void report(Issue.IssueId id, String description) {
        String previous = reported.put(id, description);
        if (previous == null) {
            log.warn(description);
        } else if (!previous.equals(description)) {
            HealthMonitor.resolveIssue(Issue.createIssue(COMPACTOR, id, previous));
        }
        HealthMonitor.reportIssue(Issue.createIssue(COMPACTOR, id, description));
    }

    private void resolve(Issue.IssueId id, String description) {
        if (reported.remove(id) != null) {
            log.info("{} resolved: {}", id, description);
        }
        HealthMonitor.resolveIssue(Issue.createIssue(COMPACTOR, id, description));
    }

    /**
     * What this node reported as the leader stops being its to say once it no longer leads: the new
     * leader evaluates the same durable state.
     */
    public void resolveLeaderIssues() {
        if (reported.isEmpty()) {
            return;
        }
        for (Issue.IssueId id : new ArrayList<>(reported.keySet())) {
            resolve(id, "This node no longer leads compaction");
        }
    }

    private boolean checkFailureAndFinishCompactionCycle(TableName table) {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus tableStatus = (CheckpointingStatus) txn.getRecord(
                    CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME, table).getPayload();

            if (tableStatus.getStatus() != StatusType.COMPLETED && tableStatus.getStatus() != StatusType.FAILED) {
                txn.putRecord(compactorMetadataTables.getCheckpointingStatusTable(), table,
                        buildCheckpointStatus(StatusType.FAILED, tableStatus.getClientName(), tableStatus.getCycleCount()), null);
                txn.delete(CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME, table);
                txn.commit();
                log.warn("Marked table {}${} FAILED due to no checkpoint activity",
                        table.getNamespace(), table.getTableName());
                return true;
            } else {
                txn.delete(CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME, table);
                txn.commit();
            }
        } catch (TransactionAbortedException ex) {
            if (ex.getAbortCause() == AbortCause.CONFLICT) {
                log.warn("Another node tried to commit");
            }
        } catch (RuntimeException re) {
            log.warn("Unable to complete required operation due to {}. StackTrace: {}", re, re.getStackTrace());
        }
        return false;
    }

    private List<TableName> getAllActiveCheckpointsTables() {
        List<TableName> activeCheckpointTables = new ArrayList<>();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            activeCheckpointTables = new ArrayList<>(txn.keySet(compactorMetadataTables.getActiveCheckpointsTable()));
            txn.commit();
        } catch (Exception e) {
            log.warn("Unable to acquire activeCheckpointTables");
        }
        return activeCheckpointTables;
    }

    /**
     * Finish compaction cycle by the leader
     */
    public void finishCompactionCycle() {
        StatusType finalStatus = StatusType.UNRECOGNIZED;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();

            if (managerStatus == null || managerStatus.getStatus() != StatusType.STARTED) {
                log.warn("Cannot perform finishCompactionCycle due to managerStatus {}", managerStatus == null ?
                        "null" : managerStatus.getStatus());
                txn.commit();
                return;
            }

            List<TableName> tableNames = new ArrayList<>(txn.keySet(compactorMetadataTables.getCheckpointingStatusTable()));
            finalStatus = StatusType.COMPLETED;
            for (TableName table : tableNames) {
                CheckpointingStatus tableStatus = (CheckpointingStatus) txn.getRecord(
                        CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME, table).getPayload();
                StringBuilder str = new StringBuilder();
                str.append(printCheckpointStatus(table, tableStatus));
                log.info("{}", str);
                if (tableStatus.getStatus() != StatusType.COMPLETED) {
                    finalStatus = StatusType.FAILED;
                    break;
                }
            }
            long totalTimeElapsed = System.currentTimeMillis() - managerStatus.getTimeTaken();
            txn.putRecord(compactorMetadataTables.getCompactionManagerTable(), CompactorMetadataTables.COMPACTION_MANAGER_KEY,
                    buildCheckpointStatus(finalStatus, tableNames.size(), totalTimeElapsed, managerStatus.getCycleCount()),
                    null);
            txn.commit();
            log.info("Total time taken for the compaction cycle: {}ms for {} tables with status {}", totalTimeElapsed,
                    tableNames.size(), finalStatus);
            MicroMeterUtils.time(Duration.ofMillis(totalTimeElapsed), "compaction.total.timer",
                    "nodeEndpoint", nodeEndpoint);
            livenessValidator.clearLivenessMap();
            livenessValidator.clearLivenessValidator();
        } catch (RuntimeException re) {
            //Do not retry here, the compactor service will trigger this method again
            // The txn should succeed otherwise the status is FAILED
            finalStatus = StatusType.FAILED;
            log.warn("Exception in finishCompactionCycle: {}. StackTrace={}", re, re.getStackTrace());
        }
        finally {
            Issue compactionCycleIssue =
                    Issue.createIssue(COMPACTOR, COMPACTION_CYCLE_FAILED, "Last compaction cycle failed");
            if (finalStatus == StatusType.COMPLETED) {
                HealthMonitor.resolveIssue(compactionCycleIssue);
            } else {
                HealthMonitor.reportIssue(compactionCycleIssue);
            }
            deleteInstantKeyIfPresent();
        }
    }

    private void deleteInstantKeyIfPresent() {
        for (int i = 0; i < MAX_RETRIES; i++) {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                RpcCommon.TokenMsg instantTrimToken = (RpcCommon.TokenMsg) txn.getRecord(
                        CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM).getPayload();
                if (instantTrimToken != null) {
                    txn.delete(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.INSTANT_TIGGER_WITH_TRIM);
                    txn.commit();
                    log.info("Invoking trimlog() due to InstantTrigger with trim found");
                    trimLog.invokePrefixTrim(corfuRuntime, corfuStore);
                    return;
                }

                RpcCommon.TokenMsg instantToken = (RpcCommon.TokenMsg) txn.getRecord(
                        CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.INSTANT_TIGGER).getPayload();
                if (instantToken != null) {
                    txn.delete(CompactorMetadataTables.COMPACTION_CONTROLS_TABLE, CompactorMetadataTables.INSTANT_TIGGER);
                }
                txn.commit();
                return;
            } catch (RuntimeException re) {
                if (DistributedCheckpointer.isCriticalRuntimeException(re, i, MAX_RETRIES)) {
                    return;
                }
            }
        }
    }

    private CheckpointingStatus buildCheckpointStatus(CheckpointingStatus.StatusType statusType, String clientName, long compactorCycleCount) {
        return CheckpointingStatus.newBuilder()
                .setStatus(statusType)
                .setClientName(clientName)
                .setCycleCount(compactorCycleCount)
                .build();
    }

    private CheckpointingStatus buildCheckpointStatus(CheckpointingStatus.StatusType statusType,
                                                      long count, long time, long compactorCycleCount) {
        return CheckpointingStatus.newBuilder()
                .setStatus(statusType)
                .setTableSize(count)
                .setTimeTaken(time)
                .setClientName(nodeEndpoint)
                .setCycleCount(compactorCycleCount)
                .build();
    }

    private String printCheckpointStatus(TableName tableName, CheckpointingStatus status) {
        StringBuilder str = new StringBuilder();
        str.append(status.getClientName()).append(": ");
        if (status.getStatus() != StatusType.COMPLETED) {
            str.append("FAILED ");
        } else {
            str.append("SUCCESS ");
        }
        str.append(tableName.getNamespace()).append("$").append(tableName.getTableName());
        str.append(" in ")
                .append(status.getTimeTaken())
                .append("ms");
        return str.toString();
    }
}
