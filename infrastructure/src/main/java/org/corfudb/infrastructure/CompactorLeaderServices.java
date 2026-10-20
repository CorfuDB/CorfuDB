package org.corfudb.infrastructure;

import lombok.Getter;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.infrastructure.health.HealthMonitor;
import org.corfudb.infrastructure.health.Issue;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuCompactorManagement.CompactionCycleHistory;
import org.corfudb.runtime.CorfuCompactorManagement.CompactionCycleKey;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.DistributedCheckpointer;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.proto.RpcCommon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.corfudb.infrastructure.health.Component.COMPACTOR;
import static org.corfudb.infrastructure.health.Issue.IssueId.COMPACTION_CYCLE_FAILED;
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
    
    // Number of cycle histories to retain (covers ~25 hours at 15-min intervals)
    private static final int HISTORY_RETENTION_COUNT = 100;
    
    // Track cycle start time for history recording
    private volatile long cycleStartTime = 0;

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
        
        // Capture start time for history recording
        cycleStartTime = System.currentTimeMillis();

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();

            if (managerStatus != null && managerStatus.getStatus() == StatusType.STARTED) {
                txn.commit();
                log.warn("Compaction cycle already started");
                return LeaderInitStatus.FAIL;
            }

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
                }
            }
            
            long currentTime = System.currentTimeMillis();
            long totalTimeElapsed = currentTime - managerStatus.getTimeTaken();
            
            // Record lightweight cycle history - only essential fields
            CompactionCycleHistory history = CompactionCycleHistory.newBuilder()
                    .setCycleCount(managerStatus.getCycleCount())
                    .setStatus(finalStatus)
                    .setStartTimeMs(cycleStartTime)
                    .setEndTimeMs(currentTime)
                    .build();
            
            CompactionCycleKey historyKey = CompactionCycleKey.newBuilder()
                    .setCycleCount(managerStatus.getCycleCount())
                    .build();
            
            txn.putRecord(compactorMetadataTables.getCompactionCycleHistoryTable(), historyKey, history, null);
            
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
            
            // Automatically prune old cycle histories to prevent unbounded growth
            pruneOldCycleHistories();
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

    /**
     * Asynchronously prune old cycle histories to keep only the most recent 100 cycles.
     * This is called automatically after each cycle completes to prevent unbounded growth.
     * Errors are logged but don't affect cycle completion.
     */
    private void pruneOldCycleHistories() {
        try {
            int deleted = pruneOldCycleHistories(HISTORY_RETENTION_COUNT);
            if (deleted > 0) {
                log.info("Auto-pruned {} old cycle history records", deleted);
            }
        } catch (Exception e) {
            // Log but don't fail the cycle - pruning is best effort
            log.warn("Failed to auto-prune old cycle histories: {}", e.getMessage());
        }
    }

    /**
     * Delete old cycle history records, keeping only the most recent N cycles.
     *
     * @param keepCount number of recent cycles to keep
     * @return number of history records deleted
     */
    private int pruneOldCycleHistories(int keepCount) {
        int deletedCount = 0;
        for (int retry = 0; retry < MAX_RETRIES; retry++) {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                // Get current cycle count
                CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                        CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                        CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();
                
                if (managerStatus == null) {
                    txn.commit();
                    return 0;
                }
                
                long currentCycle = managerStatus.getCycleCount();
                long oldestToKeep = currentCycle - keepCount;
                
                // Get all keys and delete old ones
                List<CompactionCycleKey> keys = new ArrayList<>(txn.keySet(compactorMetadataTables.getCompactionCycleHistoryTable()));
                
                for (CompactionCycleKey key : keys) {
                    if (key.getCycleCount() < oldestToKeep) {
                        txn.delete(CompactorMetadataTables.COMPACTION_CYCLE_HISTORY_TABLE_NAME, key);
                        deletedCount++;
                    }
                }
                
                txn.commit();
                log.info("Pruned {} old cycle history records, keeping last {} cycles", deletedCount, keepCount);
                return deletedCount;
            } catch (TransactionAbortedException e) {
                if (e.getAbortCause() == AbortCause.CONFLICT) {
                    log.warn("Conflict while pruning cycle histories, retry {}/{}", retry + 1, MAX_RETRIES);
                } else {
                    log.warn("Transaction aborted while pruning cycle histories: {}", e.getMessage());
                    break;
                }
            } catch (Exception e) {
                log.warn("Unable to prune old cycle histories: {}", e.getMessage());
                break;
            }
        }
        return deletedCount;
    }
}
