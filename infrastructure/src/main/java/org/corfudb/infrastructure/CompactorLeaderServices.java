package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
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
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.view.Address;
import org.corfudb.runtime.view.TableRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
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

@Slf4j
public class CompactorLeaderServices {
    private Table<StringKey, CheckpointingStatus, Message> compactionManagerTable;
    private Table<TableName, CheckpointingStatus, Message> checkpointingStatusTable;
    private Table<TableName, ActiveCPStreamMsg, Message> activeCheckpointsTable;
    private Table<StringKey, RpcCommon.TokenMsg, Message> checkpointTable;

    private Map<TableName, Tuple<Long, Long>> readCache = new HashMap<>();

    private final CorfuRuntime corfuRuntime;
    private final CorfuStore corfuStore;
    private final String clientName;

    @Setter
    private boolean isLeader;

    @Setter
    private long epoch;
    private Logger syslog;

    private static class LivenessValidator {
        @Getter
        @Setter
        private static long prevIdleCount = -1;
        @Getter
        @Setter
        private static long prevIdleTime = -1;
        @Getter
        @Setter
        private static long prevActiveTime = -1;

        public static void clear() {
            prevIdleCount = -1;
            prevIdleTime = -1;
            prevActiveTime = -1;
        }
    }

    public CompactorLeaderServices(CorfuRuntime corfuRuntime) {
        this.corfuRuntime = corfuRuntime;
        this.corfuStore = new CorfuStore(corfuRuntime);
        this.clientName = corfuRuntime.getParameters().getClientName();
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

    @VisibleForTesting
    public boolean trimAndTriggerDistributedCheckpointing() {
        syslog.info("=============Initiating Distributed Checkpointing============");
        if (!isLeader) {
            return false;
        }
        trimLog();
        if (DistributedCompactor.isCheckpointFrozen(corfuStore, checkpointTable)) {
            syslog.warn("Will not trigger checkpointing since checkpointing has been frozen");
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

            txn.clear(checkpointingStatusTable);
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
            syslog.error("triggerCheckpoint hit an exception {}. Stack Trace {}", e, e.getStackTrace());
            return false;
        }
        return true;
    }

    /**
     *
     * @param timeout
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
            LivenessValidator.clear();
            String streamName = TableRegistry.getFullyQualifiedTableName(table.getNamespace(), table.getTableName());
            UUID streamId = CorfuRuntime.getCheckpointStreamIdFromName(streamName);
            long currentStreamTail = corfuRuntime.getSequencerView()
                    .getStreamAddressSpace(new StreamAddressRange(streamId, Address.MAX, Address.NON_ADDRESS)).getTail();
            if (readCache.containsKey(table)) {
                if (readCache.get(table).first >= currentStreamTail &&
                        (currentTime - readCache.get(table).second) > timeout) {
                    if (!handleSlowCheckpointers(table)) {
                        readCache.clear();
                        return;
                    }
                }
                readCache.put(table, Tuple.of(currentStreamTail, readCache.get(table).second));
            } else {
                readCache.put(table, Tuple.of(currentStreamTail, currentTime));
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

        if (LivenessValidator.getPrevIdleTime() < 0) {
            LivenessValidator.setPrevIdleTime(currentTime);
            return;
        }
        //TODO: have a separate timeout?
        if ((currentTime - LivenessValidator.getPrevIdleTime()) > timeout) {
            long idleCount = findCheckpointProgress();
            if (LivenessValidator.getPrevActiveTime() < 0 || idleCount < LivenessValidator.getPrevIdleCount()) {
                syslog.trace("Checkpointing in progress...");
                LivenessValidator.setPrevIdleCount(idleCount);
                LivenessValidator.setPrevActiveTime(currentTime);
            } else if (currentTime - LivenessValidator.getPrevActiveTime() > timeout) {
                if (idleCount == 0 || (managerStatus != null && managerStatus.getStatus() == StatusType.STARTED_ALL)) {
                    readCache.clear();
                    LivenessValidator.clear();
                    finishCompactionCycle();
                } else if (managerStatus != null && managerStatus.getStatus() == StatusType.STARTED) {
                    syslog.info("No active client checkpointers available...");
                    try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                        managerStatus = (CheckpointingStatus) txn.getRecord(
                                DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME,
                                DistributedCompactor.COMPACTION_MANAGER_KEY).getPayload();

                        txn.putRecord(compactionManagerTable,
                                DistributedCompactor.COMPACTION_MANAGER_KEY,
                                buildCheckpointStatus(StatusType.STARTED_ALL,
                                        managerStatus.getTableSize(),
                                        managerStatus.getTimeTaken()),
                                null);
                        txn.commit();
                        LivenessValidator.clear();
                    } catch (Exception e) {
                        syslog.warn("Exception in handleNoActiveCheckpointers, e: {}. StackTrace: {}", e, e.getStackTrace());
                    }
                }
            }
        }
    }

    private boolean handleSlowCheckpointers(TableName table) {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus tableStatus = txn.getRecord(
                    checkpointingStatusTable, table).getPayload();
            if (tableStatus.getStatus() != StatusType.COMPLETED && tableStatus.getStatus() != StatusType.FAILED) {
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
                return false;
            }
            txn.commit();
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

            RpcCommon.TokenMsg minToken = (RpcCommon.TokenMsg) txn.getRecord(DistributedCompactor.CHECKPOINT,
                    DistributedCompactor.CHECKPOINT_KEY).getPayload();
            boolean cpFailed = false;
            StringBuilder str = new StringBuilder("finishCycle: ChkptStatusTableSize=");
            str.append(tableNames.size());

            for (TableName table : tableNames) {
                CheckpointingStatus tableStatus = (CheckpointingStatus) txn.getRecord(
                        DistributedCompactor.CHECKPOINT_STATUS_TABLE_NAME, table).getPayload();
                str.append(printCheckpointStatus(table, tableStatus));
                if (tableStatus.getStatus() != StatusType.COMPLETED) {
                    cpFailed = true;
                }
            }
            long totalTimeElapsed = System.currentTimeMillis() - managerStatus.getTimeTaken();
            str.append("\n").append(" totalTimeTaken=").append(totalTimeElapsed).append("ms");
            if (cpFailed) {
                syslog.warn("{}", str);
            } else {
                syslog.info("{}", str);
            }
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
                .setClientName(clientName)
                .build();
    }

    private CheckpointingStatus buildCheckpointStatus(CheckpointingStatus.StatusType statusType,
                                                      long count,
                                                      long time) {
        return CheckpointingStatus.newBuilder()
                .setStatus(statusType)
                .setTableSize(count)
                .setTimeTaken(time)
                .setClientName(clientName)
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
