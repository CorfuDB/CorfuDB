package org.corfudb.runtime;

import com.google.common.reflect.TypeToken;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.micrometer.MicroMeterUtils;
import org.corfudb.runtime.CorfuStoreMetadata.TableName;
import org.corfudb.runtime.collections.CorfuDynamicKey;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStoreEntry;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.PersistedStreamingMap;
import org.corfudb.runtime.collections.OpaqueCorfuDynamicRecord;
import org.corfudb.runtime.collections.StreamingMap;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxnContext;
import org.corfudb.runtime.exceptions.AbortCause;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.corfudb.runtime.object.ICorfuVersionPolicy;
import org.corfudb.runtime.CorfuCompactorManagement.ActiveCPStreamMsg;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus;
import org.corfudb.runtime.CorfuCompactorManagement.CheckpointingStatus.StatusType;
import org.corfudb.runtime.CorfuCompactorManagement.StringKey;
import org.corfudb.runtime.proto.RpcCommon;
import org.corfudb.runtime.proto.RpcCommon.TokenMsg;
import org.corfudb.runtime.view.ObjectOpenOption;
import org.corfudb.runtime.view.SMRObject;
import org.corfudb.util.serializer.KeyDynamicProtobufSerializer;
import org.corfudb.util.serializer.ISerializer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.corfudb.runtime.view.TableRegistry.CORFU_SYSTEM_NAMESPACE;
import static org.corfudb.runtime.view.TableRegistry.getFullyQualifiedTableName;

/**
 * DistributedCompactor class performs checkpointing of tables in multiple clients in parallel.
 * If the DistributedCompactor is triggered by the server, all opened and unopened tables are checkpointed. Otherwise,
 * only opened tables are checkpointed.
 * To checkpoint a table -
 * 1. Check if the table is locked for checkpointing by any other client ie. check if the Status of the table is IDLE
 * 2. Acquire lock to checkpoint the table, ie. mark table's status as STARTED if no other client already did
 * 3. Perform checkpointing for the table's stream
 * 4. Release lock to checkpoint the table ie. mark table's status as FAILED/COMPLETED
 */

@Slf4j
public class DistributedCompactor {
    private final CorfuRuntime runtime;
    private final CorfuRuntime cpRuntime;
    private final Optional<String> persistedCacheRoot;
    private final boolean isClient;

    private final String clientName;

    public static final long CONN_RETRY_DELAY_MILLISEC = 500;
    public static final String EMPTY_STRING = "";
    public static final String COMPACTION_MANAGER_TABLE_NAME = "CompactionManager";
    public static final String CHECKPOINT_STATUS_TABLE_NAME = "CheckpointStatusTable";
    public static final String ACTIVE_CHECKPOINTS_TABLE_NAME = "ActiveCheckpoints";

    public static final String CHECKPOINT = "checkpoint";

    public static final StringKey COMPACTION_MANAGER_KEY = StringKey.newBuilder().setKey("CompactionManagerKey").build();
    public static final StringKey CHECKPOINT_KEY = StringKey.newBuilder().setKey("minCheckpointToken").build();
    public static final StringKey UPGRADE_KEY = StringKey.newBuilder().setKey("UpgradeKey").build();

    private CorfuStore corfuStore = null;
    private Table<StringKey, CheckpointingStatus, Message> compactionManagerTable = null;
    private Table<TableName, CheckpointingStatus, Message> checkpointingStatusTable = null;
    private Table<TableName, ActiveCPStreamMsg, Message> activeCheckpointsTable = null;
    private Table<StringKey, TokenMsg, Message> checkpointTable = null;

    private CheckpointLivenessUpdater livenessUpdater;

    public static class CorfuTableNamePair {
        public TableName tableName;
        public CorfuTable corfuTable;

        public CorfuTableNamePair(TableName tableName, CorfuTable corfuTable) {
            this.tableName = tableName;
            this.corfuTable = corfuTable;
        }
    }

    public DistributedCompactor(CorfuRuntime corfuRuntime, CorfuRuntime cpRuntime, Optional<String> persistedCacheRoot) {
        this.runtime = corfuRuntime;
        this.cpRuntime = cpRuntime;
        this.persistedCacheRoot = persistedCacheRoot;
        this.isClient = false;
        this.clientName = corfuRuntime.getParameters().getClientName();
    }

    public DistributedCompactor(CorfuRuntime corfuRuntime) {
        this.runtime = corfuRuntime;
        this.cpRuntime = null;
        this.persistedCacheRoot = Optional.empty();
        this.isClient = true;
        this.clientName = corfuRuntime.getParameters().getClientName();
    }

    private void openCheckpointingMetadataTables() throws Exception {
        if (this.corfuStore != null) { // only run this method once
            return;
        }
        this.corfuStore = new CorfuStore(this.runtime);
        livenessUpdater = new CheckpointLivenessUpdater(corfuStore);
        log.debug("Opening all the checkpoint metadata tables");
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

        checkpointTable = corfuStore.openTable(CORFU_SYSTEM_NAMESPACE,
                DistributedCompactor.CHECKPOINT,
                StringKey.class,
                RpcCommon.TokenMsg.class,
                null,
                TableOptions.fromProtoSchema(RpcCommon.TokenMsg.class));
    }

    private boolean checkCompactionManagerStartTrigger() {
        //This is necessary here to stop checkpointing after it has started
        if (isCheckpointFrozen(corfuStore, this.checkpointTable) ||
                isClient && isUpgrade(corfuStore, this.checkpointTable)) {
            return false;
        }

        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            final CheckpointingStatus managerStatus = txn.getRecord(
                    compactionManagerTable, DistributedCompactor.COMPACTION_MANAGER_KEY).getPayload();
            txn.commit();
            if (managerStatus == null ||
                    managerStatus.getStatus() != StatusType.STARTED &&
                            managerStatus.getStatus() != StatusType.STARTED_ALL) {
                return false;
            }
        } catch (Exception e) {
            log.error("Unable to acquire CompactionManager status, {}, {}", e, e.getStackTrace());
            return false;
        }
        return true;
    }

    /**
     * An entry for all the clients participating in checkpointing
     * Tries to checkpoint tables that are marked as IDLE
     *
     * @return count - the number of tables checkpointed by the client
     */
    public int startCheckpointing() {
        long startCp = System.currentTimeMillis();
        log.trace("Starting Checkpointing in-memory tables..");
        try {
            openCheckpointingMetadataTables();
        } catch (Exception ex) {
            log.error("Caught exception while opening checkpoint management tables ", ex);
            corfuStore = null;
            return 0;
        }

        if (!checkCompactionManagerStartTrigger()) {
            log.trace("Checkpoint hasn't started");
            return 0; // Orchestrator says checkpointing is either not needed or done.
        }

        int count = checkpointOpenedTables();

        if (count <= 0) {
            log.trace("Opened tables not checkpointed by client: {}", clientName);
            if (count < 0) {
                return count;
            }
        }

        if (!isClient) {
            log.info("{} in-memory tables checkpointed using protobuf serializer. Checkpointing remaining.", count);
            count += checkpointUnopenedTables();
        }

        if (count > 0) {
            long cpDuration = System.currentTimeMillis() - startCp;
            log.info("Client {} took {} ms to checkpoint {} tables", clientName, cpDuration, count);

            MicroMeterUtils.time(Duration.ofMillis(cpDuration), "checkpoint.total.client_timer",
                    "clientName", clientName);
            MicroMeterUtils.measure(count, "checkpoint.num_tables." + clientName);
        }

        return count;
    }

    private int checkpointUnopenedTables() {
        int count = 0;

        KeyDynamicProtobufSerializer keyDynamicProtobufSerializer = new KeyDynamicProtobufSerializer(cpRuntime);
        cpRuntime.getSerializers().registerSerializer(keyDynamicProtobufSerializer);

        List<TableName> tableNames = getAllTablesToCheckpoint();
        for (TableName tableName : tableNames) {
            final CorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord> corfuTable =
                    openTable(tableName, keyDynamicProtobufSerializer, cpRuntime);
            final int successfulCheckpoints = tryCheckpointTable(tableName, corfuTable);
            if (successfulCheckpoints < 0) {
                // finishCompactionCycle will mark it as failed
                MicroMeterUtils.measure(count, "compaction.num_tables." + clientName);
                return count;
            }
            count += successfulCheckpoints; // 0 means lock failed so don't tick up count
        }

        return count;
    }

    private List<TableName> getAllTablesToCheckpoint() {
        List<TableName> tablesToCheckpoint = Collections.emptyList();
        final int maxRetry = 5;
        for (int retry = 0; retry < maxRetry; retry++) {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                tablesToCheckpoint = new ArrayList<>(txn.keySet(checkpointingStatusTable)
                        .stream().collect(Collectors.toList()));
                txn.commit();
            } catch (RuntimeException re) {
                if (!isCriticalRuntimeException(re, retry, maxRetry)) {
                    return tablesToCheckpoint;
                }
            } catch (Throwable t) {
                log.error("getAllTablesToCheckpoint: encountered unexpected exception", t);
            }
        }
        return tablesToCheckpoint;
    }

    public <K, V> CheckpointingStatus appendCheckpoint(CorfuTable<K, V> corfuTable, TableName tableName, CorfuRuntime rt) {
        MultiCheckpointWriter<CorfuTable> mcw = new MultiCheckpointWriter<>();
        mcw.addMap(corfuTable);
        long tableCkptStartTime = System.currentTimeMillis();
        log.info("{} Starting checkpoint: {}${}", clientName,
                tableName.getNamespace(), tableName.getTableName());

        livenessUpdater.updateLiveness(tableName);
        CheckpointingStatus returnStatus;
        try {
            mcw.appendCheckpoints(rt, "checkpointer", Optional.of(livenessUpdater));
            returnStatus = CheckpointingStatus.newBuilder()
                    .setStatus(CheckpointingStatus.StatusType.COMPLETED)
                    .setClientName(this.clientName)
                    .setTableSize(corfuTable.size())
                    .setTimeTaken(System.currentTimeMillis() - tableCkptStartTime)
                    .build();
        } catch (Exception e) {
            log.error("Unable to checkpoint table: {}, e: {}", tableName, e);
            returnStatus = CheckpointingStatus.newBuilder()
                    .setStatus(CheckpointingStatus.StatusType.FAILED)
                    .setClientName(this.clientName)
                    .setTableSize(corfuTable.size())
                    .setTimeTaken(System.currentTimeMillis() - tableCkptStartTime)
                    .build();
        }
        livenessUpdater.notifyOnSyncComplete();
        return returnStatus;
    }

    private CorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord> openTable(TableName tableName,
                                                                            ISerializer serializer,
                                                                            CorfuRuntime rt) {
        log.info("Opening table {} in namespace {}", tableName.getTableName(), tableName.getNamespace());
        SMRObject.Builder<CorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord>> corfuTableBuilder =
                rt.getObjectsView().build()
                        .setTypeToken(new TypeToken<CorfuTable<CorfuDynamicKey, OpaqueCorfuDynamicRecord>>() {
                        })
                        .setStreamName(getFullyQualifiedTableName(tableName.getNamespace(), tableName.getTableName()))
                        .setSerializer(serializer)
                        .addOpenOption(ObjectOpenOption.NO_CACHE);
        if (persistedCacheRoot.isPresent()) {
            String persistentCacheDirName = String.format("compactor_%s_%s",
                    tableName.getNamespace(), tableName.getTableName());
            Path persistedCacheLocation = Paths.get(persistedCacheRoot.get()).resolve(persistentCacheDirName);
            Supplier<StreamingMap<CorfuDynamicKey, OpaqueCorfuDynamicRecord>> mapSupplier =
                    () -> new PersistedStreamingMap<>(
                            persistedCacheLocation, PersistedStreamingMap.getPersistedStreamingMapOptions(),
                            serializer, rt);
            corfuTableBuilder.setArguments(mapSupplier, ICorfuVersionPolicy.MONOTONIC);
        }
        return corfuTableBuilder.open();
    }

    /**
     * Checkpoint all tables already opened by the current JVM to save on reads & memory.
     *
     * @return positive count on success and negative value on failure
     */
    public int checkpointOpenedTables() {
        log.trace("Checkpointing opened tables");
        int count = 0;
        for (CorfuTableNamePair openedTable :
                this.runtime.getTableRegistry().getAllOpenTablesForCheckpointing()) {
            final int isSuccess = tryCheckpointTable(openedTable.tableName, openedTable.corfuTable);
            if (isSuccess < 0) {
                log.warn("Stopping checkpointing after failure in {}${}",
                        openedTable.tableName.getNamespace(), openedTable.tableName.getTableName());
                return -count; // Stop checkpointing other tables on first failure
            }
            count += isSuccess; // 0 means lock failed so don't uptick count of checkpointed tables
        }
        return count; // All open tables have been successfully checkpointed
    }

    /**
     * Distributed Checkpointing involves 3 steps:
     * 1. Acquire distributed lock on the table to be checkpointed using transactions.
     * 2. Attempt to checkpoint the table, retry on retryable errors like WrongEpochException.
     * 3. If successful, unlock the table. if unsuccessful mark the checkpoint as failed.
     *
     * @param corfuTable - the locally opened Table instance to be checkpointed
     * @return 1 on success, 0 if lock failed and negative value on error
     */
    private int tryCheckpointTable(TableName tableName, CorfuTable corfuTable) {
        if (!tryLockMyTableToCheckpoint(tableName)) {
            return 0; // Failure to get a lock is treated as success
        }

        CheckpointingStatus checkpointStatus = tryAppendCheckpoint(tableName, corfuTable);

        return unlockMyCheckpointTable(tableName, checkpointStatus);
    }

    /**
     * @param tableName - protobuf name of the table used as key for the granular lock
     * @return true if the table can be checkpointed by me
     * false if lock acquisition fails due to race or a different error
     */
    private boolean tryLockMyTableToCheckpoint(TableName tableName) {
        final int maxRetries = 5;
        for (int retry = 0; retry < maxRetries; retry++) {
            try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                final CorfuStoreEntry<TableName, CheckpointingStatus, Message> tableToChkpt =
                        txn.getRecord(checkpointingStatusTable, tableName);
                if (tableToChkpt.getPayload().getStatus() == StatusType.IDLE) {
                    txn.putRecord(checkpointingStatusTable,
                            tableName,
                            CheckpointingStatus.newBuilder()
                                    .setStatus(CheckpointingStatus.StatusType.STARTED)
                                    .setClientName(clientName)
                                    .build(),
                            null);
                    txn.putRecord(activeCheckpointsTable, tableName,
                            ActiveCPStreamMsg.newBuilder()
                                    .setIsClientTriggered(isClient) // This will stall server side compaction!
                                    .build(),
                            null);
                    txn.commit();
                    return true; // Lock successfully acquired!
                }
                txn.commit();
                return false; // This table is already being checkpointed by someone else
            } catch (TransactionAbortedException e) {
                if (e.getAbortCause() == AbortCause.CONFLICT) {
                    log.info("My opened table {}${} is being checkpointed by someone else",
                            tableName.getNamespace(), tableName.getTableName());
                    return false;
                }
            } catch (RuntimeException re) { // TODO: return a different error code maybe?
                if (isCriticalRuntimeException(re, retry, maxRetries)) {
                    return false; // stop on non-retryable exceptions
                }
            } catch (Throwable t) {
                log.error("clientChpt-tryLock: encountered unexpected exception, {}", t.getMessage());
                log.error("StackTrace: {}, Cause: {}", t.getStackTrace(), t.getCause());
            }
        }
        return false;
    }

    /**
     * This is the routine where the actual checkpointing is invoked
     *
     * @param tableName  - protobuf name of the table to be checkpointed
     * @param corfuTable - the table to be checkpointed
     * @return the token returned from the checkpointing, null if failure happens
     */
    private CheckpointingStatus tryAppendCheckpoint(TableName tableName, CorfuTable corfuTable) {
        final int maxRetries = 5;
        CheckpointingStatus failedStatus = CheckpointingStatus.newBuilder()
                .setStatus(CheckpointingStatus.StatusType.FAILED)
                .setClientName(clientName)
                .setTableSize(corfuTable.size())
                .build();
        for (int retry = 0; retry < maxRetries; retry++) {
            try {
                return appendCheckpoint(corfuTable, tableName, runtime);
            } catch (RuntimeException re) {
                if (isCriticalRuntimeException(re, retry, maxRetries)) {
                    return failedStatus; // stop on non-retryable exceptions
                }
            } catch (Throwable t) {
                log.error("Unexpected exception encountered while trying to checkpoint, ", t);
                log.error("StackTrace: {}", t.getStackTrace());
            }
        }
        return failedStatus;
    }

    /**
     * Mark the checkpointed table as either done or failed based on endToken
     *
     * @param tableName        - protobuf name of the table just checkpointed
     * @param checkpointStatus - status of checkpoint with all the info
     * @return - 1 on success, 0 or negative value on failure
     */
    private int unlockMyCheckpointTable(TableName tableName, CheckpointingStatus checkpointStatus) {
        final int checkpointFailedError = -1;
        final int checkpointSuccess = 1;
        int isSuccess = checkpointFailedError;
        final int maxRetries = 5;
        for (int retry = 0; retry < maxRetries; retry++) {
            try (TxnContext endTxn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
                endTxn.putRecord(checkpointingStatusTable, tableName, checkpointStatus, null);
                if (checkpointStatus.getStatus() != CheckpointingStatus.StatusType.COMPLETED) {
                    log.error("clientCheckpointer: Marking checkpointing as failed on table {}", tableName);
                    isSuccess = checkpointFailedError; // this will stop checkpointing on first failure
                } else {
                    isSuccess = checkpointSuccess;
                }
                endTxn.delete(activeCheckpointsTable, tableName);
                endTxn.commit();
                break;
            } catch (RuntimeException re) {
                if (isCriticalRuntimeException(re, retry, maxRetries)) {
                    return checkpointFailedError; // stop on non-retryable exceptions
                }
            } catch (Throwable t) {
                log.error("Unexpected exception encountered while unlocking my checkpoint table, ", t);
                log.error("StackTrace : {}", t.getStackTrace());
                isSuccess = checkpointFailedError;
                break;
            }
        }
        return isSuccess;
    }

    /**
     * @param re         - the exception this method is called on
     * @param retry      - the number of times retries have been done
     * @param maxRetries - max number of times retries need to happen
     * @return - True if we should stop & return. False if we can retry!
     */
    private boolean isCriticalRuntimeException(RuntimeException re, int retry, int maxRetries) {
        log.trace("checkpointer: encountered an exception on attempt {}/{}.",
                retry, maxRetries, re);

        if (retry == maxRetries - 1) {
            log.error("checkpointer: retry exhausted.", re);
            return true;
        }

        if (re instanceof NetworkException || re.getCause() instanceof TimeoutException) {
            try {
                TimeUnit.MILLISECONDS.sleep(DistributedCompactor.CONN_RETRY_DELAY_MILLISEC);
            } catch (InterruptedException e) {
                log.error("Interrupted in network retry sleep");
                return true;
            }
        }
        if (re instanceof WrongClusterException) {
            log.error("Wrong cluster exception hit! stopping right away!");
            return true;
        }
        return false; // it is ok to retry a few times on network timeouts
    }

    public static TableName getTableName(Table<Message, Message, Message> table) {
        String fullName = table.getFullyQualifiedTableName();
        return TableName.newBuilder()
                .setNamespace(table.getNamespace())
                .setTableName(fullName.substring(fullName.indexOf("$") + 1))
                .build();
    }

    /**
     * In the global checkpoint map we examine if there is a special "freeze token"
     * The sequence part of this token is overloaded with the timestamp
     * when the freeze was requested.
     * Now checkpointer being a busybody has limited patience (2 hours)
     * If the freeze request is within 2 hours it will honor it and step aside.
     * Otherwise it will angrily remove the freezeToken and continue about
     * its business.
     *
     * @return - true if checkpointing should be skipped, false if not.
     */
    public static boolean isCheckpointFrozen(CorfuStore corfuStore, final Table<StringKey, TokenMsg, Message> chkptMap) {
        final StringKey freezeCheckpointNS = StringKey.newBuilder().setKey("freezeCheckpointNS").build();
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            TokenMsg freezeToken = txn.getRecord(chkptMap, freezeCheckpointNS).getPayload();

            final long patience = 2 * 60 * 60 * 1000;
            if (freezeToken != null) {
                long now = System.currentTimeMillis();
                long frozeAt = freezeToken.getSequence();
                Date frozeAtDate = new Date(frozeAt);
                if (now - frozeAt > patience) {
                    txn.delete(chkptMap, freezeCheckpointNS);
                    log.warn("Checkpointer asked to freeze at {} but run out of patience",
                            frozeAtDate);
                } else {
                    log.warn("Checkpointer asked to freeze at {}", frozeAtDate);
                    txn.commit();
                    return true;
                }
            }
            txn.commit();
        }
        return false;
    }

    public static boolean isUpgrade(CorfuStore corfuStore, final Table<StringKey, TokenMsg, Message> chkptMap) {
        try (TxnContext txn = corfuStore.txn(CORFU_SYSTEM_NAMESPACE)) {
            TokenMsg upgradeToken = txn.getRecord(chkptMap, UPGRADE_KEY).getPayload();
            txn.commit();
            if (upgradeToken != null) {
                log.warn("Client Checkpointer asked to freeze due to upgrade");
                return true;
            }
        }
        return false;
    }
}

