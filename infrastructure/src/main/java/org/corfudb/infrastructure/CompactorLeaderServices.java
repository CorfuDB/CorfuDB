package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.RemovalNotification;
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

import javax.annotation.Nullable;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class CompactorLeaderServices {
    @Getter
    public static final String CHECKPOINT = "checkpoint";
    private final StringKey previousTokenKey;

    private Table<StringKey, CheckpointingStatus, Message> compactionManagerTable;
    private Table<TableName, CheckpointingStatus, Message> checkpointingStatusTable;
    private Table<TableName, ActiveCPStreamMsg, Message> activeCheckpointsTable;
    private Table<StringKey, RpcCommon.TokenMsg, Message> checkpointTable;

    private Map<TableName, Tuple<Long, Long>> readCache = new HashMap<>();

    private final CorfuRuntime corfuRuntime;
    private final CorfuStore corfuStore;
    private final UUID nodeId;

    @Setter
    private boolean isLeader;

    @Setter
    private long epoch;

    public CompactorLeaderServices(CorfuRuntime corfuRuntime, UUID nodeID) {
        this.corfuRuntime = corfuRuntime;
        this.corfuStore = new CorfuStore(corfuRuntime);
        this.nodeId = nodeID;
        this.previousTokenKey = StringKey.newBuilder().setKey("previousTokenKey").build();

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
                    CHECKPOINT,
                    StringKey.class,
                    RpcCommon.TokenMsg.class,
                    null,
                    TableOptions.fromProtoSchema(RpcCommon.TokenMsg.class));

        } catch (Exception e) {
            log.error("Caught an exception while opening Compaction management tables ", e);
        }
    }

    private void handleEviction(RemovalNotification<TableName, Tuple<Long, Long>> notification) {
        if (log.isTraceEnabled()) {
            log.trace("handleEviction: evicting {} cause {}", notification.getKey(), notification.getCause());
        }
    }

    @VisibleForTesting
    public boolean triggerCheckpointing() {
        log.info("=============Initiating Distributed Checkpointing============");
        if (!isLeader) {
            return false;
        }
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CorfuStoreEntry compactionManagerRecord = txn.getRecord(
                    DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME, DistributedCompactor.COMPACTION_MANAGER_KEY);
            CheckpointingStatus currentStatus = (CheckpointingStatus) compactionManagerRecord.getPayload();

            List<TableName> tableNames = new ArrayList<>(corfuStore.listTables(null));
            CheckpointingStatus idleStatus = getCheckpointingStatus(StatusType.IDLE, null, null);

            txn.clear(checkpointingStatusTable);
            for (TableName table : tableNames) {
                txn.putRecord(checkpointingStatusTable, table, idleStatus, null);
            }

            final CorfuStoreEntry<StringKey, RpcCommon.TokenMsg, Message> prevMinToken =
                    txn.getRecord(checkpointTable, DistributedCompactor.CHECKPOINT_KEY);

            // Also record the minToken as the earliest token BEFORE checkpointing is initiated
            // This is the safest point to trim at since all data up to this point will surely
            // be included in the upcoming checkpoint cycle
            long minTokenBeforeCycleStarts = corfuRuntime.getAddressSpaceView().getLogTail();
            txn.putRecord(checkpointTable, DistributedCompactor.CHECKPOINT_KEY,
                    RpcCommon.TokenMsg.newBuilder()
                            .setEpoch(this.epoch)
                            .setSequence(minTokenBeforeCycleStarts)
                            .build(),
                    null);

            if (prevMinToken.getPayload() != null) {
                txn.putRecord(checkpointTable, previousTokenKey, prevMinToken.getPayload(), null);
            }

            CheckpointingStatus managerStatus = getCheckpointingStatus(StatusType.STARTED,null, null);
            txn.putRecord(compactionManagerTable, DistributedCompactor.COMPACTION_MANAGER_KEY, managerStatus, null);

            txn.commit();
        } catch (Exception e) {
            log.error("triggerCheckpoint hit an exception {}. Stack Trace {}", e, e.getStackTrace());
            return false;
        }
        return true;
    }

    private TableName emptyTable = TableName.newBuilder().setTableName("Empty").build();
    private TableName active = TableName.newBuilder().setTableName("Active").build();

    public void validateLiveness(long timeout) {
        List<TableName> tableNames;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            tableNames = new ArrayList<>(txn.keySet(activeCheckpointsTable)
                    .stream().collect(Collectors.toList()));
            txn.commit();
        }
        if (tableNames.size() == 0) {
            handleNoActiveCheckpointers(timeout);
        }
        for (TableName table : tableNames) {
            readCache.remove(emptyTable);
            readCache.remove(active);
            String streamName = TableRegistry.getFullyQualifiedTableName(table.getNamespace(), table.getTableName());
            UUID streamId = CorfuRuntime.getCheckpointStreamIdFromName(streamName);
            long currentStreamTail = corfuRuntime.getSequencerView()
                    .getStreamAddressSpace(new StreamAddressRange(streamId, Address.MAX, Address.NON_ADDRESS)).getTail();
            if (readCache.containsKey(table)) {
                if (readCache.get(table).first >= currentStreamTail &&
                        (System.currentTimeMillis() - readCache.get(table).second) > timeout) {
                    if (!handleSlowCheckpointers(table)) {
                        readCache.clear();
                        return;
                    }
                }
                readCache.put(table, Tuple.of(currentStreamTail, readCache.get(table).second));
            } else {
                readCache.put(table, Tuple.of(currentStreamTail, System.currentTimeMillis()));
            }
        }
    }

    private int findCheckpointProgress() {
        int count = 0;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            List<TableName> tableNames = new ArrayList<TableName>(txn.keySet(checkpointingStatusTable)
                    .stream().collect(Collectors.toList()));
            for (TableName table : tableNames) {
                CheckpointingStatus tableStatus = txn.getRecord(checkpointingStatusTable, table).getPayload();
                if (tableStatus.getStatus() != StatusType.IDLE) {
                    count++;
                }
            }
            txn.commit();
        }
        return count;
    }

    private void handleNoActiveCheckpointers(long timeout) {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME,
                    DistributedCompactor.COMPACTION_MANAGER_KEY).getPayload();

            if (!readCache.containsKey(emptyTable)) {
                readCache.put(emptyTable, Tuple.of(null, System.currentTimeMillis()));
                txn.commit();
                return;
            }
            //TODO: have a separate timeout?
            if ((System.currentTimeMillis() - readCache.get(emptyTable).second) > timeout) {
                if (managerStatus != null && managerStatus.getStatus() == StatusType.STARTED) {
                    txn.putRecord(compactionManagerTable,
                            DistributedCompactor.COMPACTION_MANAGER_KEY,
                            getCheckpointingStatus(StatusType.STARTED_ALL, null, null),
                            null);
                    readCache.remove(emptyTable);
                    txn.commit();
                    return;
                }
                txn.commit();
                if (managerStatus != null && managerStatus.getStatus() == StatusType.STARTED_ALL) {
                    long count = findCheckpointProgress();
                    if (!readCache.containsKey(active) || count > readCache.get(active).first) {
                        readCache.put(active, Tuple.of(count, System.currentTimeMillis()));
                    } else if (System.currentTimeMillis() - readCache.get(active).second > timeout) {
                        readCache.clear();
                        finishCompactionCycle();
                        return;
                    } else {
                        readCache.put(active, Tuple.of(count, readCache.get(active).second));
                    }
                }
            } else {
                txn.commit();
            }
            readCache.put(emptyTable, readCache.get(emptyTable));
        } catch (Exception e) {
            log.warn("Exception in handleNoActiveCheckpointers. StackTrace: {}", e.getStackTrace());
        }
    }

    private boolean handleSlowCheckpointers(TableName table) {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus tableStatus = txn.getRecord(
                    checkpointingStatusTable, table).getPayload();
            if (tableStatus.getStatus() == StatusType.STARTED || tableStatus.getStatus() == StatusType.STARTED_ALL) {
                txn.putRecord(checkpointingStatusTable,
                        table,
                        getCheckpointingStatus(StatusType.FAILED, null, null),
                        null);
                txn.delete(activeCheckpointsTable, table);
                //Mark cycle failed and return on first failure
                txn.putRecord(compactionManagerTable,
                        DistributedCompactor.COMPACTION_MANAGER_KEY,
                        getCheckpointingStatus(StatusType.FAILED, null, null),
                        null);
                txn.commit();
                return false;
            }
            txn.commit();
        } catch (TransactionAbortedException ex) {
            if (ex.getAbortCause() == AbortCause.CONFLICT) {
                log.warn("Another node tried to commit");
                readCache.clear();
                return false;
            }
        }
        return true;
    }

    /**
     * Finish compaction cycle by the leader
     */
    public void finishCompactionCycle() {
        log.info("inside finishCompactionCycle");
        if (!isLeader) {
            log.warn("finishCompactionCycle Lost leadership, giving up");
            return;
        }

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    DistributedCompactor.COMPACTION_MANAGER_TABLE_NAME, DistributedCompactor.COMPACTION_MANAGER_KEY).getPayload();

            if (managerStatus.getStatus() != StatusType.STARTED && managerStatus.getStatus() != StatusType.STARTED_ALL) {
                return;
            }

            List<TableName> tableNames = new ArrayList<TableName>(txn.keySet(checkpointingStatusTable)
                    .stream().collect(Collectors.toList()));

            RpcCommon.TokenMsg minToken = (RpcCommon.TokenMsg) txn.getRecord(CHECKPOINT,
                    DistributedCompactor.CHECKPOINT_KEY).getPayload();
            boolean cpFailed = false;

            for (TableName table : tableNames) {
                CheckpointingStatus tableStatus = (CheckpointingStatus) txn.getRecord(
                        DistributedCompactor.CHECKPOINT_STATUS_TABLE_NAME, table).getPayload();
                if (tableStatus.getStatus() != StatusType.COMPLETED) {
                    log.warn("Checkpointing failed on {}${}. clientName={}", table.getNamespace(),
                            table.getTableName(),
                            table.getTableName());
                    cpFailed = true;
                } else {
                    log.info("Checkpoint of {}${} done by clientName={}", table.getNamespace(),
                            table.getTableName(), tableStatus.getClientName());
                }
            }
            txn.putRecord(compactionManagerTable, DistributedCompactor.COMPACTION_MANAGER_KEY, getCheckpointingStatus(
                    cpFailed ? StatusType.FAILED : StatusType.COMPLETED, null, null), null);
            txn.commit();
        } catch (Exception e) {
            log.warn("Exception caught: {}", e.getStackTrace());
        }
        log.info("finishCheckpoint: ");
    }

    /**
     * Perform log-trimming on CorfuDB by selecting the smallest value from checkpoint map.
     */
    public void trimLog(long timeOfLastCheckpointStart) {
        log.info("Starting CorfuStore trimming task");

        final RpcCommon.TokenMsg thisTrimToken;
        if (timeOfLastCheckpointStart == 0) {
            log.warn("No prior timestamp of a checkpoint cycle, so trimming token 2 cycles ago");
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                thisTrimToken = txn.getRecord(checkpointTable, previousTokenKey).getPayload();
                txn.commit();
            }
        } else {
            log.warn("Since safe trim time has elapsed, trimming token from start of last cycle");
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                thisTrimToken = txn.getRecord(checkpointTable, DistributedCompactor.CHECKPOINT_KEY).getPayload();
                txn.commit();
            }
        }

        if (thisTrimToken == null) {
            log.warn("Trim token is not present... skipping.");
            return;
        }

        log.info("Previously computed trim token: {}", thisTrimToken);

        // Measure time spent on trimming.
        final long startTime = System.nanoTime();
        corfuRuntime.getAddressSpaceView().prefixTrim(
                new Token(thisTrimToken.getEpoch(), thisTrimToken.getSequence()));
        corfuRuntime.getAddressSpaceView().gc();
        final long endTime = System.nanoTime();

        log.info("Trim completed, elapsed({}s), log address up to {} (exclusive).",
                TimeUnit.NANOSECONDS.toSeconds(endTime - startTime), thisTrimToken.getSequence());
    }

    private CheckpointingStatus getCheckpointingStatus(CheckpointingStatus.StatusType statusType,
                                                                                @Nullable RpcCommon.TokenMsg startToken,
                                                                                @Nullable RpcCommon.TokenMsg endToken) {
        return CheckpointingStatus.newBuilder()
                .setStatus(statusType)
                .setClientName(nodeId.toString())
                .setStartToken(startToken == null ? RpcCommon.TokenMsg.getDefaultInstance() : startToken)
                .setEndToken(endToken==null? RpcCommon.TokenMsg.getDefaultInstance():endToken)
                .build();
    }
}
