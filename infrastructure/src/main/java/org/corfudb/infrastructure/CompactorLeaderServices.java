package org.corfudb.infrastructure;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CompactorMetadataTables;
import org.corfudb.runtime.CorfuCompactorManagement.ActiveCPStreamMsg;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.DistributedCheckpointerHelper;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * This class does all services that the coordinator has to perform. The actions performed by the coordinator are -
 * 1. Call trimLog() to trim everything till the token saved in the Checkpoint table
 * 2. Set CompactionManager's status as STARTED, marking the start of a compaction cycle.
 * 3. Validate liveness of tables in the ActiveCheckpoints table in order to detect slow or dead clients.
 * 4. Set CompactoinManager's status as COMPLETED or FAILED based on the checkpointing status of all the tables. This
 * marks the end of the compaction cycle.
 */
@Slf4j
public class CompactorLeaderServices {
    private final Map<TableName, LivenessMetadata> validateLivenessMap = new HashMap<>();
    private final CorfuRuntime corfuRuntime;
    private final CorfuStore corfuStore;
    private final String nodeEndpoint;
    private final LivenessValidatorHelper livenessValidatorHelper;
    private final TrimLog trimLog;
    private final Logger syslog;

    private CompactorMetadataTables compactorMetadataTables;

    private static final long LIVENESS_INIT_VALUE = -1;

    @AllArgsConstructor
    @Getter
    private class LivenessMetadata {
        private long heartbeat;
        private long streamTail;
        private long time;
    }

    @Getter
    private class LivenessValidatorHelper {
        private long prevIdleCount = LIVENESS_INIT_VALUE;
        private long prevActiveTime = LIVENESS_INIT_VALUE;

        public void updateValues(long idleCount, long activeTime) {
            prevIdleCount = idleCount;
            prevActiveTime = activeTime;
        }

        public void clear() {
            prevIdleCount = LIVENESS_INIT_VALUE;
            prevActiveTime = LIVENESS_INIT_VALUE;
        }
    }

    /**
     * This enum contains the status of the leader services of the current node
     * If the status is SUCCESS, the leader services will continue
     * If the status is FAIL, the leader services will return
     */
    //TODO: Do something else
    public static enum LeaderServicesStatus {
        SUCCESS,
        FAIL
    }

    public CompactorLeaderServices(CorfuRuntime corfuRuntime, String nodeEndpoint) {
        this.corfuRuntime = corfuRuntime;
        this.corfuStore = new CorfuStore(corfuRuntime);
        this.nodeEndpoint = nodeEndpoint;
        this.livenessValidatorHelper = new LivenessValidatorHelper();
        try {
            this.compactorMetadataTables = new CompactorMetadataTables(corfuStore);
        } catch (Exception e) {
            //TODO: Retry?
        }
        this.trimLog = new TrimLog(corfuRuntime, corfuStore);
        syslog = LoggerFactory.getLogger("syslog");
    }

    /**
     * The leader initiates the Distributed checkpointer
     * Trims the log till 'trimtoken' saved in the checkpoint table
     * Mark the start of the compaction cycle and populate CheckpointStatusTable with all the
     * tables in the registry.
     *
     * @return
     */
    public LeaderServicesStatus initDistributedCompaction() {
        syslog.info("=============Initiating Distributed Compaction============");

        if (DistributedCheckpointerHelper.isCheckpointFrozen(corfuStore)) {
            syslog.warn("Will not start compaction since it has been frozen");
            return LeaderServicesStatus.FAIL;
        }

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();

            if (managerStatus != null && (managerStatus.getStatus() == StatusType.STARTED ||
                    managerStatus.getStatus() == StatusType.STARTED_ALL)) {
                txn.commit();
                syslog.warn("Compaction cycle already started");
                return LeaderServicesStatus.FAIL;
            }

            List<TableName> tableNames = new ArrayList<>(corfuStore.listTables(null));
            CheckpointingStatus idleStatus = buildCheckpointStatus(StatusType.IDLE);

            txn.clear(CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME);
            txn.clear(CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME);
            //Populate CheckpointingStatusTable
            for (TableName table : tableNames) {
                txn.putRecord(compactorMetadataTables.getCheckpointingStatusTable(), table, idleStatus, null);
            }

            // Also record the minToken as the earliest token BEFORE checkpointing is initiated
            // This is the safest point to trim at since all data up to this point will surely
            // be included in the upcoming checkpoint cycle
            long minAddressBeforeCycleStarts = corfuRuntime.getAddressSpaceView().getLogTail();
            txn.putRecord(compactorMetadataTables.getCheckpointTable(), CompactorMetadataTables.CHECKPOINT_KEY,
                    RpcCommon.TokenMsg.newBuilder()
                            .setSequence(minAddressBeforeCycleStarts)
                            .build(),
                    null);

            CheckpointingStatus newManagerStatus = buildCheckpointStatus(StatusType.STARTED, tableNames.size(),
                    System.currentTimeMillis()); // put the current time when cycle starts
            txn.putRecord(compactorMetadataTables.getCompactionManagerTable(), CompactorMetadataTables.COMPACTION_MANAGER_KEY,
                    newManagerStatus, null);

            txn.commit();
        } catch (Exception e) {
            syslog.error("initDistributedCompaction hit an exception {}. Stack Trace {}", e, e.getStackTrace());
            return LeaderServicesStatus.FAIL;
        }
        return LeaderServicesStatus.SUCCESS;
    }

    /**
     * Validates the liveness of any on-going checkpoint of a table
     * ActiveCheckpointTable contains the list of tables for which checkpointing has started. This method is scheduled
     * to execute continuously by the leader, which monitors the checkpoint activity of the tables present in
     * ActiveCheckpointTable.
     * if there are no tables present,
     * ... check for idle tables in CheckpointStatusTable (To track progress when tables are checkpointed quickly)
     * ... if there's no progress for timeout ms, call finishCompactionCycle() to mark the end of the cycle
     * if there are any slow checkpointers,
     * ... monitor checkpointing of the table by observing if the checkpointStream's tail moves forward
     * ... if it does not move forward for timeout ms, then mark it as failed
     * Also, when checkpoint of a table is found to be failed, the cycle is immediately marks as failed.
     *
     * @param timeout in ms
     */
    public void validateLiveness(long timeout) {
        List<TableName> activeCheckpointTables;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            activeCheckpointTables = new ArrayList<>(txn.keySet(compactorMetadataTables.getActiveCheckpointsTable()));
            txn.commit();
        } catch (Exception e) {
            syslog.warn("Unable to acquire activeCheckpointTables");
            return;
        }

        long currentTime = System.currentTimeMillis();
        if (activeCheckpointTables.isEmpty()) {
            handleNoActiveCheckpointers(timeout, currentTime);
        }
        for (TableName table : activeCheckpointTables) {
            livenessValidatorHelper.clear();
            String streamName = TableRegistry.getFullyQualifiedTableName(table.getNamespace(), table.getTableName());
            UUID streamId = CorfuRuntime.getCheckpointStreamIdFromName(streamName);
            long currentStreamTail = corfuRuntime.getSequencerView()
                    .getStreamAddressSpace(new StreamAddressRange(streamId, Address.MAX, Address.NON_ADDRESS)).getTail();

            long syncHeartBeat = LIVENESS_INIT_VALUE;
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                ActiveCPStreamMsg activeCPStreamMsg = (ActiveCPStreamMsg) txn.getRecord(
                        CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME, table).getPayload();
                if (activeCPStreamMsg == null) {
                    continue;
                }
                syncHeartBeat = activeCPStreamMsg.getSyncHeartbeat();
                txn.commit();
            } catch (Exception e) {
                syslog.warn("Unable to acquire table status for {}", table, e);
            }
            if (validateLivenessMap.containsKey(table)) {
                LivenessMetadata previousStatus = validateLivenessMap.get(table);
                if (previousStatus.getStreamTail() < currentStreamTail) {
                    validateLivenessMap.put(table, new LivenessMetadata(previousStatus.getHeartbeat(),
                            currentStreamTail, currentTime));
                } else if (previousStatus.getHeartbeat() < syncHeartBeat) {
                    validateLivenessMap.put(table, new LivenessMetadata(syncHeartBeat,
                            previousStatus.getStreamTail(), currentTime));
                } else if (currentTime - previousStatus.getTime() > timeout &&
                        handleSlowCheckpointers(table) == LeaderServicesStatus.FAIL) {
                    validateLivenessMap.clear();
                    return;
                }
            } else {
                validateLivenessMap.put(table, new LivenessMetadata(syncHeartBeat, currentStreamTail, currentTime));
            }
        }
    }

    private int getIdleCount() {
        int idleCount = 0;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            idleCount = txn.executeQuery(CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME, entry -> (
                    (CheckpointingStatus) entry.getPayload()).getStatus() == StatusType.IDLE).size();
            syslog.trace("Number of idle tables: {}", idleCount);
            txn.commit();
        }
        return idleCount;
    }

    private void handleNoActiveCheckpointers(long timeout, long currentTime) {
        //Find the number of tables with IDLE status
        long idleCount = getIdleCount();
        if (livenessValidatorHelper.getPrevActiveTime() == LIVENESS_INIT_VALUE ||
                idleCount < livenessValidatorHelper.getPrevIdleCount()) {
            syslog.trace("Checkpointing in progress...");
            livenessValidatorHelper.updateValues(idleCount, currentTime);
            return;
        }
        CheckpointingStatus managerStatus = null;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            managerStatus = (CheckpointingStatus) txn.getRecord(
                    CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                    CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();
            txn.commit();
        } catch (Exception e) {
            syslog.warn("Unable to acquire Manager Status");
        }
        if (idleCount == 0 || currentTime - livenessValidatorHelper.getPrevActiveTime() > timeout &&
                managerStatus != null && managerStatus.getStatus() == StatusType.STARTED_ALL) {
            validateLivenessMap.clear();
            livenessValidatorHelper.clear();
            finishCompactionCycle();
        } else if (currentTime - livenessValidatorHelper.getPrevActiveTime() > timeout &&
                managerStatus != null && managerStatus.getStatus() == StatusType.STARTED) {
            syslog.info("No active client checkpointers available...");
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                managerStatus = (CheckpointingStatus) txn.getRecord(
                        CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME,
                        CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();

                txn.putRecord(compactorMetadataTables.getCompactionManagerTable(),
                        CompactorMetadataTables.COMPACTION_MANAGER_KEY,
                        buildCheckpointStatus(StatusType.STARTED_ALL,
                                managerStatus.getTableSize(),
                                managerStatus.getTimeTaken()),
                        null);
                txn.commit();
                livenessValidatorHelper.clear();
            } catch (Exception e) {
                syslog.warn("Exception in handleNoActiveCheckpointers, e: {}. StackTrace: {}", e, e.getStackTrace());
            }
        }
    }

    private LeaderServicesStatus handleSlowCheckpointers(TableName table) {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus tableStatus = (CheckpointingStatus) txn.getRecord(
                    CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME, table).getPayload();
            if (tableStatus.getStatus() != StatusType.COMPLETED && tableStatus.getStatus() != StatusType.FAILED) {
                txn.putRecord(compactorMetadataTables.getCheckpointingStatusTable(),
                        table,
                        buildCheckpointStatus(StatusType.FAILED),
                        null);
                txn.delete(CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME, table);
                //Mark cycle failed and return on first failure
                txn.putRecord(compactorMetadataTables.getCompactionManagerTable(),
                        CompactorMetadataTables.COMPACTION_MANAGER_KEY,
                        buildCheckpointStatus(StatusType.FAILED),
                        null);
                txn.commit();
                syslog.warn("Finished compaction cycle. FAILED due to table: {}${}", table.getNamespace(),
                        table.getTableName());
                return LeaderServicesStatus.FAIL;
            } else {
                txn.delete(CompactorMetadataTables.ACTIVE_CHECKPOINTS_TABLE_NAME, table);
                txn.commit();
            }
        } catch (TransactionAbortedException ex) {
            if (ex.getAbortCause() == AbortCause.CONFLICT) {
                syslog.warn("Another node tried to commit");
                validateLivenessMap.clear();
                return LeaderServicesStatus.SUCCESS;
            }
        }
        return LeaderServicesStatus.SUCCESS;
    }

    /**
     * Finish compaction cycle by the leader
     */
    public void finishCompactionCycle() {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    CompactorMetadataTables.COMPACTION_MANAGER_TABLE_NAME, CompactorMetadataTables.COMPACTION_MANAGER_KEY).getPayload();

            if (managerStatus == null || managerStatus.getStatus() != StatusType.STARTED &&
                    managerStatus.getStatus() != StatusType.STARTED_ALL) {
                syslog.warn("Cannot perform finishCompactionCycle due to managerStatus {}", managerStatus.getStatus());
                txn.commit();
                return;
            }

            List<TableName> tableNames = new ArrayList<>(txn.keySet(compactorMetadataTables.getCheckpointingStatusTable()));
            StatusType finalStatus = StatusType.COMPLETED;
            for (TableName table : tableNames) {
                StringBuilder str = new StringBuilder();
                CheckpointingStatus tableStatus = (CheckpointingStatus) txn.getRecord(
                        CompactorMetadataTables.CHECKPOINT_STATUS_TABLE_NAME, table).getPayload();
                str.append(printCheckpointStatus(table, tableStatus));
                if (tableStatus.getStatus() != StatusType.COMPLETED) {
                    finalStatus = StatusType.FAILED;
                    break;
                }
                syslog.info("{}", str);
            }
            long totalTimeElapsed = System.currentTimeMillis() - managerStatus.getTimeTaken();
            syslog.info("Total time taken for the compaction cycle: {}ms for {} tables with status {}", totalTimeElapsed,
                    tableNames.size(), finalStatus);
            MicroMeterUtils.time(Duration.ofMillis(totalTimeElapsed), "compaction.total.timer",
                    "nodeEndpoint", nodeEndpoint);
            txn.putRecord(compactorMetadataTables.getCompactionManagerTable(), CompactorMetadataTables.COMPACTION_MANAGER_KEY,
                    buildCheckpointStatus(finalStatus, tableNames.size(), totalTimeElapsed),
                    null);
            txn.commit();
        } catch (Exception e) {
            syslog.warn("Exception in finishCompactionCycle: {}. StackTrace={}", e, e.getStackTrace());
        }
        trimLogIfRequired();
    }

    private void trimLogIfRequired() {
        Optional<RpcCommon.TokenMsg> upgradeToken = Optional.empty();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            upgradeToken = Optional.ofNullable((RpcCommon.TokenMsg) txn.getRecord(CompactorMetadataTables.CHECKPOINT,
                    CompactorMetadataTables.UPGRADE_KEY).getPayload());
            txn.delete(CompactorMetadataTables.CHECKPOINT, CompactorMetadataTables.UPGRADE_KEY);
            txn.commit();
        } catch (Exception e) {
            syslog.error("Exception during transaction in checkpoint table", e);
        }

        if (upgradeToken.isPresent()) {
            syslog.info("Upgrade Key found: Hence invoking trimlog()");
            trimLog.invokePrefixTrim();
        }
    }

    private CheckpointingStatus buildCheckpointStatus(CheckpointingStatus.StatusType statusType) {
        return CheckpointingStatus.newBuilder()
                .setStatus(statusType)
                .setClientName(nodeEndpoint)
                .build();
    }

    private CheckpointingStatus buildCheckpointStatus(CheckpointingStatus.StatusType statusType,
                                                      long count,
                                                      long time) {
        return CheckpointingStatus.newBuilder()
                .setStatus(statusType)
                .setTableSize(count)
                .setTimeTaken(time)
                .setClientName(nodeEndpoint)
                .build();
    }

    public String printCheckpointStatus(TableName tableName, CheckpointingStatus status) {
        StringBuilder str = new StringBuilder("\n");
        str.append(status.getClientName()).append(":");
        if (status.getStatus() != StatusType.COMPLETED) {
            str.append("FAILED ");
        } else {
            str.append("SUCCESS ");
        }
        str.append(tableName.getNamespace()).append("$").append(tableName.getTableName());
        str.append(" size(")
                .append(status.getTableSize())
                .append(") in ")
                .append(status.getTimeTaken())
                .append("ms");
        return str.toString();
    }
}
