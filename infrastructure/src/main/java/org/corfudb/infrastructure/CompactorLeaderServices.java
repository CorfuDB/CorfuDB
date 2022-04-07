package org.corfudb.infrastructure;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.protobuf.Message;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.util.Tuple;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuCompactorManagement.ActiveCPStreamMsg;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.view.Address;
import org.immutables.value.Value;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;

@Slf4j
public class CompactorLeaderServices {
    @Getter
    private static final String COMPACTION_MANAGER_TABLE_NAME = "CompactionManager";
    private static final String CHECKPOINT_STATUS_TABLE_NAME = "CheckpointStatusTable";
    private static final String ACTIVE_CHECKPOINT_TABLE = "ActiveCheckpointTable";
    public static final String CHECKPOINT = "checkpoint";

    private Table<StringKey, CheckpointingStatus, Message> compactionManagerTable;
    private Table<TableName, CheckpointingStatus, Message> checkpointingStatusTable;
    private Table<TableName, ActiveCPStreamMsg, Message> activeCheckpointTable;
    private Table<StringKey, RpcCommon.TokenMsg, Message> checkpointTable;

    private Cache<TableName, Tuple<Long, Long>> readCache;

    @Getter
    private static final StringKey COMPACTION_MANAGER_KEY = StringKey.newBuilder().setKey("CompactionManagerKey").build();
    private static final StringKey CHECKPOINT_KEY = StringKey.newBuilder().setKey("minCheckpointToken").build();


    private final CorfuRuntime corfuRuntime;
    private final CorfuStore corfuStore;
    private final UUID nodeId;

    @Setter
    private boolean isLeader;

    public CompactorLeaderServices(CorfuRuntime corfuRuntime, UUID nodeID) {
        this.corfuRuntime = corfuRuntime;
        this.corfuStore = new CorfuStore(corfuRuntime);
        this.nodeId = nodeID;

        readCache = CacheBuilder.newBuilder()
                .expireAfterAccess(this.corfuRuntime.getParameters().getCacheExpiryTime(), TimeUnit.SECONDS)
                .removalListener(this::handleEviction)
                .recordStats()
                .build();

        try {
            this.compactionManagerTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    COMPACTION_MANAGER_TABLE_NAME,
                    StringKey.class,
                    CheckpointingStatus.class,
                    null,
                    TableOptions.fromProtoSchema(CheckpointingStatus.class));

            this.checkpointingStatusTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    CHECKPOINT_STATUS_TABLE_NAME,
                    TableName.class,
                    CheckpointingStatus.class,
                    null,
                    TableOptions.fromProtoSchema(CheckpointingStatus.class));

            this.activeCheckpointTable = this.corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                    ACTIVE_CHECKPOINT_TABLE,
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

    public boolean init() {
        log.info("in init()");
        if (!isLeader) {
            return false;
        }
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CorfuStoreEntry compactionManagerRecord = txn.getRecord(COMPACTION_MANAGER_TABLE_NAME, COMPACTION_MANAGER_KEY);
            CheckpointingStatus currentStatus = (CheckpointingStatus) compactionManagerRecord.getPayload();
            if (currentStatus != null && (currentStatus.getStatus() == StatusType.INITIALIZING ||
                    currentStatus.getStatus() == StatusType.STARTED)) {
                txn.commit();
                return false;
            }
            CheckpointingStatus newStatus = getCheckpointingStatus(StatusType.INITIALIZING, null, null);
            txn.putRecord(compactionManagerTable, COMPACTION_MANAGER_KEY, newStatus, null);
            txn.commit();
        } catch (TransactionAbortedException e) {
            if (e.getAbortCause() == AbortCause.CONFLICT) {
                log.warn("Looks like another compactor started first. Message: {}", e.getCause());
            } else {
                log.warn("Exception caught: {}", e.getStackTrace());
            }
            return false;
        }
        return startPopulating();
    }

    @VisibleForTesting
    public boolean startPopulating() {
        log.info("startPopulating");
        if (!isLeader) {
            return false;
        }
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CorfuStoreEntry compactionManagerRecord = txn.getRecord(COMPACTION_MANAGER_TABLE_NAME, COMPACTION_MANAGER_KEY);
            CheckpointingStatus currentStatus = (CheckpointingStatus) compactionManagerRecord.getPayload();

            if (currentStatus == null || currentStatus.getStatus() != StatusType.INITIALIZING) {
                txn.commit();
                return false;
            }

            List<TableName> tableNames = new ArrayList<>(corfuStore.listTables(null));
            CheckpointingStatus idleStatus = getCheckpointingStatus(StatusType.IDLE, null, null);

            txn.clear(checkpointingStatusTable);
            for (TableName table : tableNames) {
                txn.putRecord(checkpointingStatusTable, table, idleStatus, null);
            }
            txn.commit();
        }

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            //TODO: add startToken?
            CheckpointingStatus managerStatus = getCheckpointingStatus(StatusType.STARTED,null, null);
            txn.putRecord(compactionManagerTable, COMPACTION_MANAGER_KEY, managerStatus, null);
            txn.commit();
        }
        return true;
    }

    private TableName emptyTable = TableName.newBuilder().setTableName("Empty").build();
    public void validateLiveness(long timeout) {
        List<TableName> tableNames;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            tableNames = new ArrayList<>(txn.keySet(activeCheckpointTable)
                    .stream().collect(Collectors.toList()));
            txn.commit();
        }
        if (tableNames.size() == 0) {
            if (readCache.asMap().containsKey(emptyTable)) {
                if (System.currentTimeMillis() - readCache.asMap().get(emptyTable).second > timeout) {
                    finishCompactionCycle();
                    return;
                }
                readCache.asMap().put(emptyTable, readCache.asMap().get(emptyTable));
            } else {
                readCache.asMap().put(emptyTable, Tuple.of(null, System.currentTimeMillis()));
            }
        }
        for (TableName table : tableNames) {
            if (!isLeader) {
                return;
            }
            UUID streamId = CorfuRuntime.getCheckpointStreamIdFromName(table.getTableName());
            long currentStreamTail = corfuRuntime.getSequencerView()
                    .getStreamAddressSpace(new StreamAddressRange(streamId, Address.MAX, Address.NON_ADDRESS)).getTail();
            if (readCache.asMap().containsKey(table)) {
                if (readCache.asMap().get(table).first >= currentStreamTail &&
                        (System.currentTimeMillis() - readCache.asMap().get(table).second) > timeout) {
                    try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                        CheckpointingStatus tableStatus = txn.getRecord(
                                checkpointingStatusTable, table).getPayload();
                        if (tableStatus.getStatus() == StatusType.STARTED) {
                            txn.putRecord(checkpointingStatusTable,
                                    table,
                                    getCheckpointingStatus(StatusType.FAILED, null, null),
                                    null);
                            txn.delete(activeCheckpointTable, table);
                            log.info("Came here as well");
                        }
                        log.info("need to fail for table: {}", table);
                        txn.commit();
                    } catch (TransactionAbortedException ex) {
                        if (ex.getAbortCause() == AbortCause.CONFLICT) {
                            log.warn("Another node tried to commit");
                        }
                    }
                }
                readCache.asMap().put(table, Tuple.of(currentStreamTail, readCache.asMap().get(table).second));
            } else {
                readCache.asMap().put(table, Tuple.of(currentStreamTail, System.currentTimeMillis()));
            }
        }
    }

    public void finishCompactionCycle() {
        log.info("inside finishCompactionCycle");
        if (!isLeader) {
            return;
        }
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            CheckpointingStatus managerStatus = (CheckpointingStatus) txn.getRecord(
                    COMPACTION_MANAGER_TABLE_NAME, COMPACTION_MANAGER_KEY).getPayload();

            if (managerStatus.getStatus() != StatusType.STARTED) {
                //The leader seems to have changed but the previous leader failed after making it FINALIZING
                // and it's probably still running?
                return;
            }

            txn.putRecord(compactionManagerTable, COMPACTION_MANAGER_KEY,
                    getCheckpointingStatus(StatusType.FINALIZING, null, null),
                    null);
            txn.commit();
        } catch (TransactionAbortedException e) {
            if (e.getAbortCause() == AbortCause.CONFLICT) {
                log.warn("Another compactor tried to modify this table", e);
            } else {
                log.error("TransactionAbortedException: {}", e.getStackTrace());
            }
            return;
        }

        List<TableName> tableNames;
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            tableNames = new ArrayList<TableName>(txn.keySet(checkpointingStatusTable)
                    .stream().collect(Collectors.toList()));

            RpcCommon.TokenMsg minToken = (RpcCommon.TokenMsg) txn.getRecord(CHECKPOINT, CHECKPOINT_KEY).getPayload();
            RpcCommon.TokenMsg newToken = null;
            boolean cpFailed = false;

            for (TableName table : tableNames) {
                if (!isLeader) {
                    return;
                }
                CheckpointingStatus tableStatus = (CheckpointingStatus) txn.getRecord(
                        CHECKPOINT_STATUS_TABLE_NAME, table).getPayload();
                if (tableStatus.getStatus() == StatusType.COMPLETED) {
                    newToken = (newToken == null) ? tableStatus.getEndToken() :
                            minTokenMsg(newToken, tableStatus.getEndToken());
                } else {
                    cpFailed = true;
                }
            }
            if (!isLeader) {
                return;
            }
            if (newToken != null && (minToken == null || minToken.getEpoch() <= newToken.getEpoch() &&
                    minToken.getSequence() <= newToken.getSequence())) {
                txn.putRecord(checkpointTable, CHECKPOINT_KEY, newToken, null);
            }
            txn.putRecord(compactionManagerTable, COMPACTION_MANAGER_KEY, getCheckpointingStatus(
                    cpFailed ? StatusType.FAILED : StatusType.COMPLETED, null, null), null);
            txn.commit();
        } catch (Exception e) {
            log.warn("Exception caught: {}", e.getStackTrace());
        }
        log.info("Done with finishCompactionCycle");
    }

    private RpcCommon.TokenMsg minTokenMsg(RpcCommon.TokenMsg a, RpcCommon.TokenMsg b) {
        int epochCmp = Long.compare(a.getEpoch(), b.getEpoch());
        if (epochCmp == 0) {
            return a.getSequence() <= b.getSequence() ? a : b;
        }
        return epochCmp < 0 ? a : b;
    }

    private CheckpointingStatus getCheckpointingStatus(CheckpointingStatus.StatusType statusType,
                                                                                @Nullable RpcCommon.TokenMsg startToken,
                                                                                @Nullable RpcCommon.TokenMsg endToken) {
        return CheckpointingStatus.newBuilder()
                .setStatus(statusType)
                .setClientId(RpcCommon.UuidMsg.newBuilder()
                        .setMsb(nodeId.getMostSignificantBits())
                        .setLsb(nodeId.getLeastSignificantBits()))
                .setStartToken(startToken == null ? RpcCommon.TokenMsg.getDefaultInstance() : startToken)
                .setEndToken(endToken==null? RpcCommon.TokenMsg.getDefaultInstance():endToken)
                .build();
    }
}
