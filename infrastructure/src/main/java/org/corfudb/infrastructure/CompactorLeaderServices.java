package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import lombok.Getter;
import lombok.Setter;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.common.util.Tuple;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.protocols.wireprotocol.Token;
import org.corfudb.runtime.CorfuCompactorManagement.ActiveCPStreamMsg;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.DistributedCompactor;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.object.transactions.TransactionalContext;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.proto.service.CorfuMessage;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

/**
 * This class does all services that the coordinator has to perform. The actions performed by the coordinator are -
 * 1. Call trimLog() to trim everything till the token saved in the Checkpoint table
 * 2. Set CompactionManager's status as STARTED, marking the start of a compaction cycle.
 * 3. Validate liveness of tables in the ActiveCheckpoints table in order to detect slow or dead clients.
 * 4. Set CompactoinManager's status as COMPLETED or FAILED based on the checkpointing status of all the tables. This
 *    marks the end of the compactoin cycle.
 */

public class CompactorLeaderServices {
    private Table<StringKey, CheckpointingStatus, Message> compactionManagerTable;
    private Table<TableName, CheckpointingStatus, Message> checkpointingStatusTable;
    private Table<TableName, ActiveCPStreamMsg, Message> activeCheckpointsTable;
    private Table<StringKey, RpcCommon.TokenMsg, Message> checkpointTable;

    private Map<TableName, Tuple<Tuple<Long, Long>, Long>> readCache = new HashMap<>();

    private final CorfuRuntime corfuRuntime;
    private final CorfuStore corfuStore;
    private final String nodeEndpoint;

    @Setter
    private boolean isLeader;

    @Setter
    private long epoch;
    private Logger syslog;

    private static class LivenessValidatorHelper {
        @Getter
        @Setter
        private static long prevIdleCount = -1;
        @Getter
        @Setter
        private static long prevActiveTime = -1;

        public static void clear() {
            prevIdleCount = -1;
            prevActiveTime = -1;
        }
    }

    public CompactorLeaderServices(CorfuRuntime corfuRuntime, String nodeEndpoint) {
        this.corfuRuntime = corfuRuntime;
        this.corfuStore = new CorfuStore(corfuRuntime);
        this.nodeEndpoint = nodeEndpoint;
        syslog = LoggerFactory.getLogger("SYSLOG");

        try {
            this.compactionManagerTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME,
                    StringKey.class,
                    CheckpointingStatus.class,
                    null,
                    TableOptions.fromProtoSchema(CheckpointingStatus.class));

            this.checkpointingStatusTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.CHECKPOINT_STATUS_TABLE_NAME,
                    TableName.class,
                    CheckpointingStatus.class,
                    null,
                    TableOptions.fromProtoSchema(CheckpointingStatus.class));

            this.activeCheckpointsTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.ACTIVE_CHECKPOINTS_TABLE_NAME,
                    TableName.class,
                    ActiveCPStreamMsg.class,
                    null,
                    TableOptions.fromProtoSchema(ActiveCPStreamMsg.class));

            this.checkpointTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    DistributedCompactor.CHECKPOINT,
                    StringKey.class,
                    RpcCommon.TokenMsg.class,
                    null,
                    TableOptions.fromProtoSchema(RpcCommon.TokenMsg.class));

        } catch (Exception e) {
            syslog.error("Caught an exception while opening Compaction management tables ", e);
        }
    }

    /**
     * The leader initiates the Distributed checkpointer
     * Trims the log till 'trimtoken' saved in the checkpoint table
     * Mark the start of the compaction cycle and populate CheckpointStatusTable with all the
     * tables in the registry.
     * @return
     */
    @VisibleForTesting
    public boolean trimAndTriggerDistributedCheckpointing() {
        syslog.info("=============Initiating Distributed Checkpointing============");
        if (!isLeader) {
            return false;
        }
        trimLog();
        if (DistributedCompactor.isCheckpointFrozen(corfuStore, checkpointTable)) {
            syslog.warn("Will not start checkpointing since checkpointing has been frozen");
            return false;
        }

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CorfuStoreEntry compactionManagerRecord = txn.getRecord(
                    DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME, DistributedCompactor.COMPACTION_MANAGER_KEY);
            CheckpointingStatus managerStatus = (CheckpointingStatus) compactionManagerRecord.getPayload();

            if (managerStatus != null && (managerStatus.getStatus() == StatusType.STARTED ||
                    managerStatus.getStatus() == StatusType.STARTED_ALL)) {
                return false;
            }

            List<TableName> tableNames = new ArrayList<>(corfuStore.listTables(null));
            CheckpointingStatus idleStatus = buildCheckpointStatus(StatusType.IDLE);

            TransactionalContext.getCurrentContext().setPriorityLevel(CorfuMessage.PriorityLevel.HIGH);
            txn.clear(checkpointingStatusTable);
            txn.clear(activeCheckpointsTable);
            //Populate CheckpointingStatusTable
            for (TableName table : tableNames) {
                txn.putRecord(checkpointingStatusTable, table, idleStatus, null);
            }

            // Also record the minToken as the earliest token BEFORE checkpointing is initiated
            // This is the safest point to trim at since all data up to this point will surely
            // be included in the upcoming checkpoint cycle
            long minAddressBeforeCycleStarts = corfuRuntime.getAddressSpaceView().getLogTail();
            txn.putRecord(checkpointTable, DistributedCompactor.CHECKPOINT_KEY,
                    RpcCommon.TokenMsg.newBuilder()
                            .setEpoch(this.epoch)
                            .setSequence(minAddressBeforeCycleStarts)
                            .build(),
                    null);

            CheckpointingStatus newManagerStatus = buildCheckpointStatus(StatusType.STARTED, tableNames.size(),
                    System.currentTimeMillis()); // put the current time when cycle starts
            txn.putRecord(compactionManagerTable, DistributedCompactor.COMPACTION_MANAGER_KEY, newManagerStatus, null);

            txn.commit();
        } catch (Exception e) {
            syslog.error("triggerDistributedCheckpoint hit an exception {}. Stack Trace {}", e, e.getStackTrace());
            return false;
        }
        return true;
    }

    /**
     * Validates the liveness of any on-going checkpoint of a table
     * ActiveCheckpointTable contains the list of tables for which checkpointing has started. This method is scheduled
     * to execute continuously by the leader, which monitors the checkpoint activity of the tables present in
     * ActiveCheckpointTable.
     * if there are no tables present,
     *      ... check the CheckpointStatusTable for any progress made (this step is done to include the
     *              tables which are checkpointed really fast)
     *      ... if there's no progress for timeout ms, call finishCompactionCycle() to mark the end of the cycle
     * if there are any slow checkpointers,
     *      ... monitor checkpointing of the table by observing if the checkpointStream's tail moves forward
     *      ... if it does not move forward for timeout ms, then mark it as failed
     * Also, when checkpoint of a table is found to be failed, the cycle is immediately marks as failed.
     * @param timeout in ms
     */
    public void validateLiveness(long timeout) {
        List<TableName> tableNames;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            tableNames = new ArrayList<>(txn.keySet(activeCheckpointsTable)
                    .stream().collect(Collectors.toList()));
            txn.commit();
        } catch (Exception e) {
            syslog.warn("Unable to acquire tableNames");
            return;
        }

        long currentTime = System.currentTimeMillis();
        if (tableNames.size() == 0) {
            handleNoActiveCheckpointers(timeout, currentTime);
        }
        for (TableName table : tableNames) {
            LivenessValidatorHelper.clear();
            String streamName = TableRegistry.getFullyQualifiedTableName(table.getNamespace(), table.getTableName());
            UUID streamId = CorfuRuntime.getCheckpointStreamIdFromName(streamName);
            long currentStreamTail = corfuRuntime.getSequencerView()
                    .getStreamAddressSpace(new StreamAddressRange(streamId, Address.MAX, Address.NON_ADDRESS)).getTail();

            long syncHeartBeat = -1;
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                syncHeartBeat = txn.getRecord(activeCheckpointsTable, table).getPayload().getSyncHeartbeat();
                txn.commit();
            } catch (Exception e) {
                syslog.warn("Unable to acquire table status");
            }
            if (readCache.containsKey(table)) {
                //previousStatus.first is syncHeartbeat
                //previousStatus.second is StreamTail
                Tuple<Long, Long> previousStatus = readCache.get(table).first;
                if (previousStatus.second < currentStreamTail) {
                    readCache.put(table, Tuple.of(Tuple.of(previousStatus.first, currentStreamTail), currentTime));
                } else if (previousStatus.first < syncHeartBeat) {
                    readCache.put(table, Tuple.of(Tuple.of(syncHeartBeat, previousStatus.second), currentTime));
                } else if (previousStatus.second == currentStreamTail || previousStatus.first == syncHeartBeat) {
                    if ((currentTime - readCache.get(table).second > timeout) && !handleSlowCheckpointers(table)) {
                        readCache.clear();
                        return;
                    }
                }
            } else {
                readCache.put(table, Tuple.of(Tuple.of(syncHeartBeat, currentStreamTail), currentTime));
            }
        }
    }

    private int findCheckpointProgress() {
        int idleCount = 0;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            List<TableName> tableNames = new ArrayList<TableName>(txn.keySet(checkpointingStatusTable)
                    .stream().collect(Collectors.toList()));
            idleCount = tableNames.size();
            for (TableName table : tableNames) {
                CheckpointingStatus tableStatus = txn.getRecord(checkpointingStatusTable, table).getPayload();
                if (tableStatus.getStatus() != StatusType.IDLE) {
                    idleCount--;
                }
            }
            syslog.trace("Number of idle tables: {} out of {}", idleCount, tableNames.size());
            txn.commit();
        }
        return idleCount;
    }

    private void handleNoActiveCheckpointers(long timeout, long currentTime) {
        CheckpointingStatus managerStatus = null;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            managerStatus = (CheckpointingStatus) txn.getRecord(
                    DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME,
                    DistributedCompactor.COMPACTION_MANAGER_KEY).getPayload();
            txn.commit();
        } catch (Exception e) {
            syslog.warn("Unable to acquire Manager Status");
        }

        //Find the number of tables with IDLE status
        long idleCount = findCheckpointProgress();
        if (LivenessValidatorHelper.getPrevActiveTime() < 0 || idleCount < LivenessValidatorHelper.getPrevIdleCount()) {
            syslog.trace("Checkpointing in progress...");
            LivenessValidatorHelper.setPrevIdleCount(idleCount);
            LivenessValidatorHelper.setPrevActiveTime(currentTime);
        } else if (idleCount == 0 || (currentTime - LivenessValidatorHelper.getPrevActiveTime() > timeout &&
                managerStatus != null && managerStatus.getStatus() == StatusType.STARTED_ALL)) {
            readCache.clear();
            LivenessValidatorHelper.clear();
            finishCompactionCycle();
        } else if (currentTime - LivenessValidatorHelper.getPrevActiveTime() > timeout &&
                managerStatus != null && managerStatus.getStatus() == StatusType.STARTED) {
            syslog.info("No active client checkpointers available...");
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                managerStatus = (CheckpointingStatus) txn.getRecord(
                        DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME,
                        DistributedCompactor.COMPACTION_MANAGER_KEY).getPayload();

                TransactionalContext.getCurrentContext().setPriorityLevel(CorfuMessage.PriorityLevel.HIGH);
                txn.putRecord(compactionManagerTable,
                        DistributedCompactor.COMPACTION_MANAGER_KEY,
                        buildCheckpointStatus(StatusType.STARTED_ALL,
                                managerStatus.getTableSize(),
                                managerStatus.getTimeTaken()),
                        null);
                txn.commit();
                LivenessValidatorHelper.clear();
            } catch (Exception e) {
                syslog.warn("Exception in handleNoActiveCheckpointers, e: {}. StackTrace: {}", e, e.getStackTrace());
            }
        }
    }

    private boolean handleSlowCheckpointers(TableName table) {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus tableStatus = txn.getRecord(
                    checkpointingStatusTable, table).getPayload();
            if (tableStatus.getStatus() != StatusType.COMPLETED && tableStatus.getStatus() != StatusType.FAILED) {
                TransactionalContext.getCurrentContext().setPriorityLevel(CorfuMessage.PriorityLevel.HIGH);
                txn.putRecord(checkpointingStatusTable,
                        table,
                        buildCheckpointStatus(StatusType.FAILED),
                        null);
                txn.delete(activeCheckpointsTable, table);
                //Mark cycle failed and return on first failure
                txn.putRecord(compactionManagerTable,
                        DistributedCompactor.COMPACTION_MANAGER_KEY,
                        buildCheckpointStatus(StatusType.FAILED),
                        null);
                txn.commit();
                syslog.warn("Finished compaction cycle. FAILED due to table: {}${}", table.getNamespace(),
                        table.getTableName());
                return false;
            } else {
                txn.commit();
            }
        } catch (TransactionAbortedException ex) {
            if (ex.getAbortCause() == AbortCause.CONFLICT) {
                syslog.warn("Another node tried to commit");
                readCache.clear();
                return true;
            }
        }
        return true;
    }

    /**
     * Finish compaction cycle by the leader
     */
    public void finishCompactionCycle() {
        if (!isLeader) {
            syslog.warn("finishCompactionCycle Lost leadership, giving up");
            return;
        }

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME, DistributedCompactor.COMPACTION_MANAGER_KEY).getPayload();

            if (managerStatus == null || (managerStatus.getStatus() != StatusType.STARTED &&
                    managerStatus.getStatus() != StatusType.STARTED_ALL)) {
                syslog.warn("Cannot perform finishCompactionCycle");
                return;
            }

            List<TableName> tableNames = new ArrayList<TableName>(txn.keySet(checkpointingStatusTable)
                    .stream().collect(Collectors.toList()));

            boolean cpFailed = false;

            for (TableName table : tableNames) {
                StringBuilder str = new StringBuilder();
                CheckpointingStatus tableStatus = (CheckpointingStatus) txn.getRecord(
                        DistributedCompactor.CHECKPOINT_STATUS_TABLE_NAME, table).getPayload();
                str.append(printCheckpointStatus(table, tableStatus));
                if (tableStatus.getStatus() != StatusType.COMPLETED) {
                    cpFailed = true;
                }
                syslog.info("{}", str);
            }
            long totalTimeElapsed = System.currentTimeMillis() - managerStatus.getTimeTaken();
            syslog.info("Total time taken for the compaction cycle: {}ms for {} tables", totalTimeElapsed,
                    tableNames.size());
            MicroMeterUtils.time(Duration.ofMillis(totalTimeElapsed), "compaction.total.timer",
                    "nodeEndpoint", nodeEndpoint);
            TransactionalContext.getCurrentContext().setPriorityLevel(CorfuMessage.PriorityLevel.HIGH);
            txn.putRecord(compactionManagerTable, DistributedCompactor.COMPACTION_MANAGER_KEY,
                    buildCheckpointStatus(cpFailed ? StatusType.FAILED : StatusType.COMPLETED,
                            tableNames.size(), totalTimeElapsed),
                    null);
            txn.commit();
        } catch (Exception e) {
            syslog.warn("Exception in finishCompactionCycle: {}. StackTrace={}", e, e.getStackTrace());
        }
        syslog.info("Finished the compaction cycle");
    }

    /**
     * Perform log-trimming on CorfuDB by selecting the smallest value from checkpoint map.
     */
    @VisibleForTesting
    public void trimLog() {
        RpcCommon.TokenMsg thisTrimToken = null;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME,
                    DistributedCompactor.COMPACTION_MANAGER_KEY).getPayload();
            if (managerStatus.getStatus() == StatusType.COMPLETED) {
                thisTrimToken = txn.getRecord(checkpointTable, DistributedCompactor.CHECKPOINT_KEY).getPayload();
            } else {
                syslog.warn("Skip trimming since last checkpointing cycle did not complete successfully");
                return;
            }
            txn.commit();
        } catch (Exception e) {
            syslog.warn("Unable to acquire the trim token");
            return;
        }

        if (thisTrimToken == null) {
            syslog.warn("Trim token is not present... skipping.");
            return;
        }

        // Measure time spent on trimming.
        final long startTime = System.nanoTime();
        corfuRuntime.getAddressSpaceView().prefixTrim(
                new Token(thisTrimToken.getEpoch(), thisTrimToken.getSequence()));
        corfuRuntime.getAddressSpaceView().gc();
        final long endTime = System.nanoTime();

        syslog.info("Trim completed, elapsed({}s), log address up to {}.",
                TimeUnit.NANOSECONDS.toSeconds(endTime - startTime), thisTrimToken.getSequence());
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

    public static String printCheckpointStatus(TableName tableName, CheckpointingStatus status) {
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
